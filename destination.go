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
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/conduit-connector-sdk/internal/csync"
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

	// -- Lifecycle events -----------------------------------------------------

	// LifecycleOnCreated is called after Configure and before Open when the
	// connector is run for the first time. This call will be skipped if the
	// connector was already started before. This method can be used to do some
	// initialization that needs to happen only once in the lifetime of a
	// connector (e.g. create a bucket). Anything that the connector creates in
	// this method is considered to be owned by this connector and should be
	// cleaned up in LifecycleOnDeleted.
	LifecycleOnCreated(ctx context.Context, config map[string]string) error
	// LifecycleOnUpdated is called after Configure and before Open when the
	// connector configuration has changed since the last run. This call will be
	// skipped if the connector configuration did not change. It can be used to
	// update anything that was initialized in LifecycleOnCreated, in case the
	// configuration change affects it.
	LifecycleOnUpdated(ctx context.Context, configBefore, configAfter map[string]string) error
	// LifecycleOnDeleted is called when the connector was deleted. It will be
	// the only method that is called in that case. This method can be used to
	// clean up anything that was initialized in LifecycleOnCreated.
	LifecycleOnDeleted(ctx context.Context, config map[string]string) error

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

	lastPosition *csync.ValueWatcher[Position]
	openCancel   context.CancelFunc

	// write is the chosen write strategy, either single records or batches
	writeStrategy writeStrategy
}

func (a *destinationPluginAdapter) Configure(ctx context.Context, req cpluginv1.DestinationConfigureRequest) (cpluginv1.DestinationConfigureResponse, error) {
	ctx = DestinationWithBatch{}.setBatchEnabled(ctx, false)

	v := validator(a.impl.Parameters())
	// init config and apply default values
	updatedCfg, multiErr := v.InitConfig(req.Config)
	// run builtin validations
	multiErr = multierr.Append(multiErr, v.Validate(updatedCfg))
	// run custom validations written by developer
	multiErr = multierr.Append(multiErr, a.impl.Configure(ctx, updatedCfg))
	multiErr = multierr.Append(multiErr, a.configureWriteStrategy(ctx, updatedCfg))

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
		a.writeStrategy = newWriteStrategyBatch(a.impl, batchSize, batchDelay)
	}

	return nil
}

func (a *destinationPluginAdapter) Start(ctx context.Context, _ cpluginv1.DestinationStartRequest) (cpluginv1.DestinationStartResponse, error) {
	a.lastPosition = new(csync.ValueWatcher[Position])

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
		a.lastPosition.Set(r.Position)
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

	// wait for last record to be received
	_, err := a.lastPosition.Watch(ctx, func(val Position) bool {
		return bytes.Equal(val, req.LastPosition)
	}, csync.WithTimeout(time.Minute)) // TODO make the timeout configurable (https://github.com/ConduitIO/conduit/issues/183)

	// flush cached records, allow it to take at most 1 minute
	flushCtx, cancel := context.WithTimeout(ctx, time.Minute) // TODO make the timeout configurable
	defer cancel()

	flushErr := a.writeStrategy.Flush(flushCtx)
	if flushErr != nil && err == nil {
		err = flushErr
	} else if flushErr != nil {
		Logger(ctx).Err(err).Msg("error flushing records")
	}

	return cpluginv1.DestinationStopResponse{}, err
}

func (a *destinationPluginAdapter) Teardown(ctx context.Context, _ cpluginv1.DestinationTeardownRequest) (cpluginv1.DestinationTeardownResponse, error) {
	err := a.impl.Teardown(ctx)
	if err != nil {
		return cpluginv1.DestinationTeardownResponse{}, err
	}
	return cpluginv1.DestinationTeardownResponse{}, nil
}

func (a *destinationPluginAdapter) LifecycleOnCreated(ctx context.Context, req cpluginv1.DestinationLifecycleOnCreatedRequest) (cpluginv1.DestinationLifecycleOnCreatedResponse, error) {
	return cpluginv1.DestinationLifecycleOnCreatedResponse{}, a.impl.LifecycleOnCreated(ctx, req.Config)
}
func (a *destinationPluginAdapter) LifecycleOnUpdated(ctx context.Context, req cpluginv1.DestinationLifecycleOnUpdatedRequest) (cpluginv1.DestinationLifecycleOnUpdatedResponse, error) {
	return cpluginv1.DestinationLifecycleOnUpdatedResponse{}, a.impl.LifecycleOnUpdated(ctx, req.ConfigBefore, req.ConfigAfter)
}
func (a *destinationPluginAdapter) LifecycleOnDeleted(ctx context.Context, req cpluginv1.DestinationLifecycleOnDeletedRequest) (cpluginv1.DestinationLifecycleOnDeletedResponse, error) {
	return cpluginv1.DestinationLifecycleOnDeletedResponse{}, a.impl.LifecycleOnDeleted(ctx, req.Config)
}

func (a *destinationPluginAdapter) convertRecord(r cpluginv1.Record) Record {
	return Record{
		Position:  r.Position,
		Operation: Operation(r.Operation),
		Metadata:  a.convertMetadata(r.Metadata),
		Key:       a.convertData(r.Key),
		Payload:   a.convertChange(r.Payload),
	}
}

func (a *destinationPluginAdapter) convertMetadata(m map[string]string) Metadata {
	metadata := (Metadata)(m)
	if metadata == nil {
		metadata = make(map[string]string, 1)
	}
	metadata.SetOpenCDCVersion()
	return metadata
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

func (w *writeStrategySingle) Flush(context.Context) error {
	return nil // nothing to flush
}

// writeStrategyBatch will cache records before writing them. Records are
// grouped into batches that get written when they reach the size batchSize or
// when the time since adding the first record to the current batch reaches
// batchDelay.
type writeStrategyBatch struct {
	impl    Destination
	batcher *internal.Batcher[writeBatchItem]
}

type writeBatchItem struct {
	ctx    context.Context
	record Record
	ack    func(error) error
}

func newWriteStrategyBatch(impl Destination, batchSize int, batchDelay time.Duration) *writeStrategyBatch {
	strategy := &writeStrategyBatch{impl: impl}
	strategy.batcher = internal.NewBatcher(
		batchSize,
		batchDelay,
		strategy.writeBatch,
	)
	return strategy
}

func (w *writeStrategyBatch) writeBatch(batch []writeBatchItem) error {
	records := make([]Record, len(batch))
	for i, item := range batch {
		records[i] = item.record
	}
	// use the last record's context as the write context
	ctx := batch[len(batch)-1].ctx

	n, err := w.impl.Write(ctx, records)
	if n == len(batch) && err != nil {
		err = fmt.Errorf("connector reported a successful write of all records in the batch and simultaneously returned an error, this is probably a bug in the connector. Original error: %w", err)
		n = 0 // nack all messages in the batch
	} else if n < len(batch) && err == nil {
		err = fmt.Errorf("batch contained %d messages, connector has only written %d without reporting the error, this is probably a bug in the connector", len(batch), n)
	}

	var (
		firstErr error
		errOnce  bool
	)
	for i, item := range batch {
		if i < n {
			err := item.ack(err)
			if err != nil && !errOnce {
				firstErr = err
				errOnce = true
			}
		}
	}
	return firstErr
}

func (w *writeStrategyBatch) Write(ctx context.Context, r Record, ack func(error) error) error {
	select {
	case result := <-w.batcher.Results():
		Logger(ctx).Debug().
			Int("batchSize", result.Size).
			Time("at", result.At).Err(result.Err).
			Msg("last batch was flushed asynchronously")
		if result.Err != nil {
			return fmt.Errorf("last batch write failed: %w", result.Err)
		}
	default:
		// last batch was not flushed yet
	}

	status := w.batcher.Enqueue(writeBatchItem{
		ctx:    ctx,
		record: r,
		ack:    ack,
	})

	switch status {
	case internal.Scheduled:
		// This message was scheduled for the next batch.
		// We need to check the results channel of the previous batch, in case
		// the flush happened just before enqueuing this record.
		select {
		case result := <-w.batcher.Results():
			Logger(ctx).Debug().
				Int("batchSize", result.Size).
				Time("at", result.At).Err(result.Err).
				Msg("last batch was flushed asynchronously")
			if result.Err != nil {
				return fmt.Errorf("last batch write failed: %w", result.Err)
			}
		default:
			// last batch was not flushed yet
		}
		return nil
	case internal.Flushed:
		// This record caused a flush, get the result.
		result := <-w.batcher.Results()
		Logger(ctx).Debug().
			Int("batchSize", result.Size).
			Time("at", result.At).Err(result.Err).
			Msg("batch was flushed synchronously")
		return result.Err
	default:
		return fmt.Errorf("unknown batcher enqueue status: %v", status)
	}
}
func (w *writeStrategyBatch) Flush(ctx context.Context) error {
	w.batcher.Flush()
	select {
	case result := <-w.batcher.Results():
		Logger(ctx).Debug().
			Int("batchSize", result.Size).
			Time("at", result.At).Err(result.Err).
			Msg("batch was flushed synchronously")
		if result.Err != nil {
			return fmt.Errorf("last batch write failed: %w", result.Err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// DestinationUtil provides utility methods for implementing a destination. Use
// it by calling Util.Destination.*.
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
