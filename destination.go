// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate mockgen -destination=mock_destination_test.go -self_package=github.com/conduitio/conduit-connector-sdk -package=sdk -write_package_comment=false . Destination

package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"go.uber.org/multierr"
)

// Destination receives records from Conduit and writes them to 3rd party
// resources.
// All implementations must embed UnimplementedDestination for forward
// compatibility.
type Destination interface {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination.
	Parameters() map[string]Parameter

	// Configure is the first function to be called in a connector. It provides the
	// connector with the configuration that needs to be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	Configure(context.Context, map[string]string) error

	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
	Open(context.Context) error

	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).
	Write(ctx context.Context, r []Record) (n int, err error)

	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	Teardown(context.Context) error

	mustEmbedUnimplementedDestination()
}

// NewDestinationPlugin takes a Destination and wraps it into an adapter that
// converts it into a cpluginv1.DestinationPlugin. If the parameter is nil it
// will wrap UnimplementedDestination instead.
func NewDestinationPlugin(impl Destination) cpluginv1.DestinationPlugin {
	if impl == nil {
		// prevent nil pointers
		impl = UnimplementedDestination{}
	}
	return &destinationPluginAdapter{impl: impl}
}

type destinationPluginAdapter struct {
	impl Destination

	lastPosition *internal.AtomicValueWatcher[Position]
	openCancel   context.CancelFunc

	// write is the chosen write strategy, either single records or batches
	writeStrategy writeStrategy
}

func (a *destinationPluginAdapter) Configure(ctx context.Context, req cpluginv1.DestinationConfigureRequest) (cpluginv1.DestinationConfigureResponse, error) {
	ctx = DestinationWithBatch{}.setBatchEnabled(ctx, false)

	var multiErr error
	// run builtin validations
	multiErr = multierr.Append(multiErr, validator(a.impl.Parameters()).Validate(req.Config))
	// run custom validations written by developer
	multiErr = multierr.Append(multiErr, a.impl.Configure(ctx, req.Config))
	multiErr = multierr.Append(multiErr, a.configureWriteStrategy(ctx, req.Config))

	return cpluginv1.DestinationConfigureResponse{}, multiErr
}

func (a *destinationPluginAdapter) configureWriteStrategy(ctx context.Context, config map[string]string) error {
	a.writeStrategy = &writeStrategySingle{impl: a.impl} // by default we write single records

	batchEnabled := DestinationWithBatch{}.getBatchEnabled(ctx)
	if !batchEnabled {
		// batching disabled, just write single records
		return nil
	}

	var batchSize int
	var batchDelay time.Duration

	batchSizeRaw := config[configDestinationBatchSize]
	if batchSizeRaw != "" {
		batchSizeInt, err := strconv.Atoi(batchSizeRaw)
		if err != nil {
			return fmt.Errorf("invalid %q: %w", configDestinationBatchSize, err)
		}
		batchSize = batchSizeInt
	}

	delayRaw := config[configDestinationBatchDelay]
	if delayRaw != "" {
		delayDur, err := time.ParseDuration(delayRaw)
		if err != nil {
			return fmt.Errorf("invalid %q: %w", configDestinationBatchDelay, err)
		}
		batchDelay = delayDur
	}

	if batchSize < 0 {
		return fmt.Errorf("invalid %q: must not be negative", configDestinationBatchSize)
	}
	if batchDelay < 0 {
		return fmt.Errorf("invalid %q: must not be negative", configDestinationBatchDelay)
	}

	if batchSize > 0 || batchDelay > 0 {
		a.writeStrategy = &writeStrategyBatch{
			impl:       a.impl,
			batchSize:  batchSize,
			batchDelay: batchDelay,
		}
		// TODO remove this once batching is implemented
		return errors.New("batching not implemented")
	}

	return nil
}

func (a *destinationPluginAdapter) Start(ctx context.Context, req cpluginv1.DestinationStartRequest) (cpluginv1.DestinationStartResponse, error) {
	a.lastPosition = new(internal.AtomicValueWatcher[Position])

	// detach context, so we can control when it's canceled
	ctxOpen := internal.DetachContext(ctx)
	ctxOpen, a.openCancel = context.WithCancel(ctxOpen)

	startDone := make(chan struct{})
	defer close(startDone)
	go func() {
		// for duration of the Start call we propagate the cancellation of ctx to
		// ctxOpen, after Start returns we decouple the context and let it live
		// until the plugin should stop running
		select {
		case <-ctx.Done():
			a.openCancel()
		case <-startDone:
			// start finished before ctx was canceled, leave context open
		}
	}()

	err := a.impl.Open(ctxOpen)
	return cpluginv1.DestinationStartResponse{}, err
}

func (a *destinationPluginAdapter) Run(ctx context.Context, stream cpluginv1.DestinationRunStream) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				// stream is closed
				return nil
			}
			return fmt.Errorf("write stream error: %w", err)
		}
		r := a.convertRecord(req.Record)

		err = a.writeStrategy.Write(ctx, r, func(err error) error {
			return a.ack(r, err, stream)
		})
		a.lastPosition.Store(r.Position)
		if err != nil {
			return err
		}
	}
}

// ack sends a message into the stream signaling that the record was processed.
func (a *destinationPluginAdapter) ack(r Record, writeErr error, stream cpluginv1.DestinationRunStream) error {
	var ackErrStr string
	if writeErr != nil {
		ackErrStr = writeErr.Error()
	}
	err := stream.Send(cpluginv1.DestinationRunResponse{
		AckPosition: r.Position,
		Error:       ackErrStr,
	})
	if err != nil {
		return fmt.Errorf("ack stream error: %w", err)
	}
	return nil
}

func (a *destinationPluginAdapter) Stop(ctx context.Context, req cpluginv1.DestinationStopRequest) (cpluginv1.DestinationStopResponse, error) {
	// last thing we do is cancel context in Open
	defer a.openCancel()

	// wait for at most 1 minute
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute) // TODO make the timeout configurable (https://github.com/ConduitIO/conduit/issues/183)
	defer cancel()

	// wait for last record to be received
	err := a.lastPosition.Await(waitCtx, func(val Position) bool {
		return bytes.Equal(val, req.LastPosition)
	})

	// flush cached records
	flushErr := a.writeStrategy.Flush(ctx)
	if flushErr != nil && err == nil {
		err = flushErr
	} else if flushErr != nil {
		Logger(ctx).Err(err).Msg("error flushing records")
	}

	return cpluginv1.DestinationStopResponse{}, err
}

func (a *destinationPluginAdapter) Teardown(ctx context.Context, req cpluginv1.DestinationTeardownRequest) (cpluginv1.DestinationTeardownResponse, error) {
	err := a.impl.Teardown(ctx)
	if err != nil {
		return cpluginv1.DestinationTeardownResponse{}, err
	}
	return cpluginv1.DestinationTeardownResponse{}, nil
}

func (a *destinationPluginAdapter) convertRecord(r cpluginv1.Record) Record {
	return Record{
		Position:  r.Position,
		Operation: Operation(r.Operation),
		Metadata:  r.Metadata,
		Key:       a.convertData(r.Key),
		Payload:   a.convertChange(r.Payload),
	}
}

func (a *destinationPluginAdapter) convertChange(c cpluginv1.Change) Change {
	return Change{
		Before: a.convertData(c.Before),
		After:  a.convertData(c.After),
	}
}

func (a *destinationPluginAdapter) convertData(d cpluginv1.Data) Data {
	if d == nil {
		return nil
	}

	switch v := d.(type) {
	case cpluginv1.RawData:
		return RawData(v)
	case cpluginv1.StructuredData:
		return StructuredData(v)
	default:
		panic("unknown data type")
	}
}

// writeStrategy is used to switch between writing single records and batching
// them.
type writeStrategy interface {
	Write(ctx context.Context, r Record, ack func(error) error) error
	Flush(ctx context.Context) error
}

// writeStrategySingle will write records synchronously one by one without
// caching them. Acknowledgments are sent back to Conduit right after they are
// written.
type writeStrategySingle struct {
	impl Destination
}

func (w *writeStrategySingle) Write(ctx context.Context, r Record, ack func(error) error) error {
	_, err := w.impl.Write(ctx, []Record{r})
	if err != nil {
		Logger(ctx).Err(err).Bytes("record_position", r.Position).Msg("error writing record")
	}
	return ack(err)
}

func (w *writeStrategySingle) Flush(ctx context.Context) error {
	return nil // nothing to flush
}

// writeStrategyBatch will cache records before writing them. Records are
// grouped into batches that get written when they reach the size batchSize or
// when the time since adding the first record to the current batch reaches
// batchDelay.
// TODO needs to be implemented
type writeStrategyBatch struct {
	impl Destination

	batchSize  int
	batchDelay time.Duration
}

func (w *writeStrategyBatch) Write(ctx context.Context, r Record, ack func(error) error) error {
	panic("batching not implemented yet")
}
func (w *writeStrategyBatch) Flush(ctx context.Context) error {
	panic("batching not implemented yet")
}

// DestinationUtil provides utility methods for implementing a destination.
type DestinationUtil struct{}

// Route makes it easier to implement a destination that mutates entities in
// place and thus handles different operations differently. It will inspect the
// operation on the record and based on that choose which handler to call.
//
// Example usage:
//
//	func (d *Destination) Write(ctx context.Context, r sdk.Record) error {
//	  return d.Util.Route(ctx, r,
//	    d.handleInsert,
//	    d.handleUpdate,
//	    d.handleDelete,
//	    d.handleSnapshot, // we could also reuse d.handleInsert
//	  )
//	}
//	func (d *Destination) handleInsert(ctx context.Context, r sdk.Record) error {
//	  ...
//	}
func (DestinationUtil) Route(
	ctx context.Context,
	rec Record,
	handleCreate func(context.Context, Record) error,
	handleUpdate func(context.Context, Record) error,
	handleDelete func(context.Context, Record) error,
	handleSnapshot func(context.Context, Record) error,
) error {
	switch rec.Operation {
	case OperationCreate:
		return handleCreate(ctx, rec)
	case OperationUpdate:
		return handleUpdate(ctx, rec)
	case OperationDelete:
		return handleDelete(ctx, rec)
	case OperationSnapshot:
		return handleSnapshot(ctx, rec)
	default:
		return fmt.Errorf("invalid operation %q", rec.Operation)
	}
}
