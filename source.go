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

//go:generate mockgen -destination=mock_source_test.go -self_package=github.com/conduitio/conduit-connector-sdk -package=sdk -write_package_comment=false . Source

package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/jpillora/backoff"
	"gopkg.in/tomb.v2"
)

// Source fetches records from 3rd party resources and sends them to Conduit.
// All implementations must embed UnimplementedSource for forward compatibility.
type Source interface {
	// Configure is the first function to be called in a connector. It provides the
	// connector with the configuration that needs to be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	Configure(context.Context, map[string]string) error

	// Open is called after Configure to signal the plugin it can prepare to
	// start producing records. If needed, the plugin should open connections in
	// this function. The position parameter will contain the position of the
	// last record that was successfully processed, Source should therefore
	// start producing records after this position. The context passed to Open
	// will be cancelled once the plugin receives a stop signal from Conduit.
	Open(context.Context, Position) error

	// Read returns a new Record and is supposed to block until there is either
	// a new record or the context gets cancelled. It can also return the error
	// ErrBackoffRetry to signal to the SDK it should call Read again with a
	// backoff retry.
	// If Read receives a cancelled context or the context is cancelled while
	// Read is running it must stop retrieving new records from the source
	// system and start returning records that have already been buffered. If
	// there are no buffered records left Read must return the context error to
	// signal a graceful stop. If Read returns ErrBackoffRetry while the context
	// is cancelled it will also signal that there are no records left and Read
	// won't be called again.
	// After Read returns an error the function won't be called again (except if
	// the error is ErrBackoffRetry, as mentioned above).
	// Read can be called concurrently with Ack.
	Read(context.Context) (Record, error)
	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled, since there might be
	// outstanding acks that need to be delivered. When Teardown is called it is
	// guaranteed there won't be any more calls to Ack.
	// Ack can be called concurrently with Read.
	Ack(context.Context, Position) error

	// Teardown signals to the plugin that there will be no more calls to any
	// other function. After Teardown returns, the plugin should be ready for a
	// graceful shutdown.
	Teardown(context.Context) error

	mustEmbedUnimplementedSource()
}

// NewSourcePlugin takes a Source and wraps it into an adapter that converts it
// into a cpluginv1.SourcePlugin. If the parameter is nil it will wrap
// UnimplementedSource instead.
func NewSourcePlugin(impl Source) cpluginv1.SourcePlugin {
	if impl == nil {
		// prevent nil pointers
		impl = UnimplementedSource{}
	}
	return &sourcePluginAdapter{impl: impl}
}

type sourcePluginAdapter struct {
	impl Source

	// readDone will be closed after runRead stops running.
	readDone chan struct{}

	// lastPosition stores the position of the last record sent to Conduit.
	lastPosition Position

	openCancel context.CancelFunc
	readCancel context.CancelFunc
	t          *tomb.Tomb
}

func (a *sourcePluginAdapter) Configure(ctx context.Context, req cpluginv1.SourceConfigureRequest) (cpluginv1.SourceConfigureResponse, error) {
	err := a.impl.Configure(ctx, req.Config)
	return cpluginv1.SourceConfigureResponse{}, err
}

func (a *sourcePluginAdapter) Start(ctx context.Context, req cpluginv1.SourceStartRequest) (cpluginv1.SourceStartResponse, error) {
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

	err := a.impl.Open(ctxOpen, req.Position)
	return cpluginv1.SourceStartResponse{}, err
}

func (a *sourcePluginAdapter) Run(ctx context.Context, stream cpluginv1.SourceRunStream) error {
	t, ctx := tomb.WithContext(ctx)
	readCtx, readCancel := context.WithCancel(ctx)

	a.t = t
	a.readCancel = readCancel
	a.readDone = make(chan struct{})

	t.Go(func() error {
		defer close(a.readDone)
		return a.runRead(readCtx, stream)
	})
	t.Go(func() error {
		return a.runAck(ctx, stream)
	})

	<-t.Dying() // stop as soon as it's dying
	return t.Err()
}

func (a *sourcePluginAdapter) runRead(ctx context.Context, stream cpluginv1.SourceRunStream) error {
	// TODO make backoff params configurable (https://github.com/ConduitIO/conduit/issues/184)
	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second * 5,
	}

	for {
		r, err := a.impl.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil // not an actual error
			}
			if errors.Is(err, ErrBackoffRetry) {
				// the plugin wants us to retry reading later
				select {
				case <-ctx.Done():
					// the plugin is using the SDK for long polling and relying
					// on the SDK to check for a cancelled context
					return nil
				case <-time.After(b.Duration()):
					continue
				}
			}
			return fmt.Errorf("read plugin error: %w", err)
		}

		err = stream.Send(cpluginv1.SourceRunResponse{Record: a.convertRecord(r)})
		if err != nil {
			return fmt.Errorf("read stream error: %w", err)
		}
		a.lastPosition = r.Position // store last sent position

		// reset backoff retry
		b.Reset()
	}
}

func (a *sourcePluginAdapter) runAck(ctx context.Context, stream cpluginv1.SourceRunStream) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil // stream is closed, not an error
			}
			return fmt.Errorf("ack stream error: %w", err)
		}
		err = a.impl.Ack(ctx, req.AckPosition)
		// implementing Ack is optional
		if err != nil && !errors.Is(err, ErrUnimplemented) {
			return fmt.Errorf("ack plugin error: %w", err)
		}
	}
}

func (a *sourcePluginAdapter) Stop(ctx context.Context, req cpluginv1.SourceStopRequest) (cpluginv1.SourceStopResponse, error) {
	// stop reading new messages
	a.openCancel()
	a.readCancel()

	// TODO timeout for badly written connectors
	<-a.readDone // wait for read to actually stop running

	return cpluginv1.SourceStopResponse{
		LastPosition: a.lastPosition,
	}, nil
}

func (a *sourcePluginAdapter) Teardown(ctx context.Context, req cpluginv1.SourceTeardownRequest) (cpluginv1.SourceTeardownResponse, error) {
	var waitErr error
	if a.t != nil {
		// wait for at most 1 minute
		waitCtx, cancel := context.WithTimeout(ctx, time.Minute) // TODO make the timeout configurable (https://github.com/ConduitIO/conduit/issues/183)
		defer cancel()

		waitErr = a.waitForRun(waitCtx) // wait for Run to stop running
		if waitErr != nil {
			// just log error and continue to call Teardown to keep guarantee
			Logger(ctx).Warn().Err(waitErr).Msg("failed to wait for Run to stop running")
		}
	}

	err := a.impl.Teardown(ctx)
	if err != nil {
		return cpluginv1.SourceTeardownResponse{}, err
	}

	return cpluginv1.SourceTeardownResponse{}, waitErr
}

// waitForRun returns once the Run function returns or the context gets
// cancelled, whichever happens first. If the context gets cancelled the context
// error will be returned.
func (a *sourcePluginAdapter) waitForRun(ctx context.Context) error {
	// wait for all acks to be sent back to Conduit
	ackFuncsDone := make(chan struct{})
	go func() {
		_ = a.t.Wait() // ignore tomb error, it will be returned in Run anyway
		close(ackFuncsDone)
	}()
	return a.waitForClose(ctx, ackFuncsDone)
}

func (a *sourcePluginAdapter) waitForClose(ctx context.Context, stop chan struct{}) error {
	select {
	case <-stop:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *sourcePluginAdapter) convertRecord(r Record) cpluginv1.Record {
	return cpluginv1.Record{
		Position:  r.Position,
		Metadata:  r.Metadata,
		Key:       a.convertData(r.Key),
		Payload:   a.convertData(r.Payload),
		CreatedAt: r.CreatedAt,
	}
}

func (a *sourcePluginAdapter) convertData(d Data) cpluginv1.Data {
	if d == nil {
		return nil
	}

	switch v := d.(type) {
	case RawData:
		return cpluginv1.RawData(v)
	case StructuredData:
		return cpluginv1.StructuredData(v)
	default:
		panic("unknown data type")
	}
}
