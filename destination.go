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
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/conduitio/conduit-connector-sdk/internal"
)

// Destination receives records from Conduit and writes them to 3rd party
// resources.
// All implementations must embed UnimplementedDestination for forward
// compatibility.
type Destination interface {
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

	openCancel context.CancelFunc
}

func (a *destinationPluginAdapter) Configure(ctx context.Context, req cpluginv1.DestinationConfigureRequest) (cpluginv1.DestinationConfigureResponse, error) {
	err := a.impl.Configure(ctx, req.Config)
	return cpluginv1.DestinationConfigureResponse{}, err
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

		_, err = a.impl.Write(ctx, []Record{r}) // TODO implement batching
		if err != nil {
			Logger(ctx)
		}
		err = a.ack(r, err, stream)
		a.lastPosition.Store(r.Position) // store last processed position
		if err != nil {
			return err
		}
	}
}

// ackFunc creates an AckFunc that can be called to signal that the record was
// processed. The destination plugin adapter keeps track of how many AckFunc
// functions still need to be called, once an AckFunc returns it decrements the
// internal counter by one.
// It is allowed to call AckFunc only once, if it's called more than once it
// will panic.
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

func (a *destinationPluginAdapter) waitForClose(ctx context.Context, stop chan struct{}) error {
	select {
	case <-stop:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

// DestinationUtil provides utility methods for implementing a destination.
type DestinationUtil struct{}

// Route makes it easier to implement a destination that mutates entities in
// place and thus handles different operations differently. It will inspect the
// operation on the record and based on that choose which handler to call.
//
// Example usage:
//   func (d *Destination) Write(ctx context.Context, r sdk.Record) error {
//     return d.Util.Route(ctx, r,
//       d.handleInsert,
//       d.handleUpdate,
//       d.handleDelete,
//       d.handleSnapshot, // we could also reuse d.handleInsert
//     )
//   }
//   func (d *Destination) handleInsert(ctx context.Context, r sdk.Record) error {
//     ...
//   }
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
