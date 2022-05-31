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

package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/conduitio/conduit-connector-sdk/internal"
)

// Destination receives records from Conduit and writes them to 3rd party
// resources.
// All implementations must embed UnimplementedDestination for forward
// compatibility.
// When implementing Destination you can choose between implementing the method
// WriteAsync or Write. If both are implemented, then WriteAsync will be used.
// Use WriteAsync if the plugin will cache records and write them to the 3rd
// party resource in batches, otherwise use Write.
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

	// WriteAsync receives a Record and can cache it internally and write it at
	// a later time. Once the record is successfully written to the destination
	// the plugin must call the provided AckFunc function with a nil error. If
	// the plugin failed to write the record to the destination it must call the
	// supplied AckFunc with a non-nil error. If any AckFunc is left uncalled
	// the connector will not be able to exit gracefully. The WriteAsync
	// function should only return an error in case a critical error happened,
	// it is expected that all future calls to WriteAsync will fail with the
	// same error.
	// If the plugin caches records before writing them to the destination it
	// needs to store the AckFunc as well and call it once the record is
	// written.
	// AckFunc will panic if it's called more than once.
	// If WriteAsync returns ErrUnimplemented the SDK will fall back and call
	// Write instead. A connector must implement exactly one of these functions,
	// not both.
	WriteAsync(context.Context, Record, AckFunc) error

	// Write receives a Record and is supposed to write the record to the
	// destination right away. If the function returns nil the record is assumed
	// to have reached the destination.
	// WriteAsync takes precedence and will be tried first to write a record. If
	// WriteAsync is not implemented the SDK will fall back and use Write
	// instead. A connector must implement exactly one of these functions, not
	// both.
	Write(context.Context, Record) error

	// Flush signals the plugin it should flush any cached records and call all
	// outstanding AckFunc functions. This function needs to be implemented only
	// if the struct implements WriteAsync. No more calls to WriteAsync
	// will be issued after Flush is called.
	Flush(context.Context) error

	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	Teardown(context.Context) error

	mustEmbedUnimplementedDestination()
}

// AckFunc is a function that forwards the acknowledgment to Conduit. It returns
// an error in case the forwarding failed, otherwise it returns nil. If nil is
// passed to this function the record is assumed to be successfully processed,
// otherwise the parameter should contain an error describing why a record
// failed to be processed. If an error is returned from AckFunc it should be
// regarded as fatal and all processing should be stopped as soon as possible.
type AckFunc func(error) error

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
	impl       Destination
	wgAckFuncs sync.WaitGroup
	isAsync    bool

	lastPositionHolder *internal.Holder[Position]

	openCancel context.CancelFunc
}

func (a *destinationPluginAdapter) Configure(ctx context.Context, req cpluginv1.DestinationConfigureRequest) (cpluginv1.DestinationConfigureResponse, error) {
	err := a.impl.Configure(ctx, req.Config)
	return cpluginv1.DestinationConfigureResponse{}, err
}

func (a *destinationPluginAdapter) Start(ctx context.Context, req cpluginv1.DestinationStartRequest) (cpluginv1.DestinationStartResponse, error) {
	a.lastPositionHolder = new(internal.Holder[Position])

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
	var writeFunc func(context.Context, Record, cpluginv1.DestinationRunStream) error
	// writeFunc will overwrite itself the first time it is called, depending
	// on what the destination supports. If it supports async writes it will be
	// replaced with writeAsync, if not it will be replaced with write.
	writeFunc = func(ctx context.Context, r Record, stream cpluginv1.DestinationRunStream) error {
		Logger(ctx).Trace().Msg("determining write mode")
		err := a.writeAsync(ctx, r, stream)
		if errors.Is(err, ErrUnimplemented) {
			// WriteAsync is not implemented, fallback to Write and overwrite
			// writeFunc, so we don't try WriteAsync again.
			Logger(ctx).Trace().Msg("using sync write mode")
			writeFunc = a.write
			return a.write(ctx, r, stream)
		}
		// WriteAsync is implemented, overwrite writeFunc as we don't need the
		// fallback logic anymore
		Logger(ctx).Trace().Msg("using async write mode")
		a.isAsync = true
		writeFunc = a.writeAsync
		return err
	}

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

		a.wgAckFuncs.Add(1)
		err = writeFunc(ctx, r, stream)
		a.lastPositionHolder.Put(r.Position) // store last processed position
		if err != nil {
			return err
		}
	}
}

// write will call the Write function and send the acknowledgment back to
// Conduit when it returns.
func (a *destinationPluginAdapter) write(ctx context.Context, r Record, stream cpluginv1.DestinationRunStream) error {
	err := a.impl.Write(ctx, r)
	return a.ackFunc(r, stream)(err)
}

// writeAsync will call the WriteAsync function and provide the ack function to
// the implementation without calling it.
func (a *destinationPluginAdapter) writeAsync(ctx context.Context, r Record, stream cpluginv1.DestinationRunStream) error {
	return a.impl.WriteAsync(ctx, r, a.ackFunc(r, stream))
}

// ackFunc creates an AckFunc that can be called to signal that the record was
// processed. The destination plugin adapter keeps track of how many AckFunc
// functions still need to be called, once an AckFunc returns it decrements the
// internal counter by one.
// It is allowed to call AckFunc only once, if it's called more than once it
// will panic.
func (a *destinationPluginAdapter) ackFunc(r Record, stream cpluginv1.DestinationRunStream) AckFunc {
	var isCalled int32
	return func(ackErr error) error {
		if !atomic.CompareAndSwapInt32(&isCalled, 0, 1) {
			panic("same ack func must not be called twice")
		}

		defer a.wgAckFuncs.Done()
		var ackErrStr string
		if ackErr != nil {
			ackErrStr = ackErr.Error()
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
}

func (a *destinationPluginAdapter) waitForAcks(ctx context.Context) error {
	// wait for all acks to be sent back to Conduit
	ackFuncsDone := make(chan struct{})
	go func() {
		a.wgAckFuncs.Wait()
		close(ackFuncsDone)
	}()
	return a.waitForClose(ctx, ackFuncsDone)
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
	awaitErr := a.lastPositionHolder.Await(waitCtx, func(val Position) bool {
		return bytes.Equal(val, req.LastPosition)
	})
	if awaitErr != nil {
		// just log error and continue to flush at least the processed records
		Logger(ctx).Warn().Err(awaitErr).Msg("failed to wait for last record to be received")
	}

	// flush cached records
	err := a.impl.Flush(ctx)
	if err != nil &&
		// only propagate error if we are sure the plugin is writing records
		// asynchronously or if it's some other error than ErrUnimplemented
		(a.isAsync || !errors.Is(err, ErrUnimplemented)) {
		return cpluginv1.DestinationStopResponse{}, err
	}

	// wait for acks, use waitCtx so we still respect the 1 minute limit
	// if the connector implements Flush correctly no acks should be outstanding
	// now, we are just being extra careful
	err = a.waitForAcks(waitCtx)
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
		Metadata:  r.Metadata,
		Key:       a.convertData(r.Key),
		Payload:   a.convertData(r.Payload),
		CreatedAt: r.CreatedAt,
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
