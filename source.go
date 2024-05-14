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

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/cpluginv2"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/conduit-connector-sdk/internal/cchan"
	"github.com/conduitio/conduit-connector-sdk/internal/csync"
	"github.com/jpillora/backoff"
	"gopkg.in/tomb.v2"
)

var (
	// TODO make the timeout configurable (https://github.com/ConduitIO/conduit/issues/183)
	stopTimeout     = time.Minute
	teardownTimeout = time.Minute
)

// Source fetches records from 3rd party resources and sends them to Conduit.
// All implementations must embed UnimplementedSource for forward compatibility.
type Source interface {
	// Parameters is a map of named Parameters that describe how to configure
	// the Source.
	Parameters() config.Parameters

	// Configure is the first function to be called in a connector. It provides the
	// connector with the configuration that needs to be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The connector SDK will sanitize, apply defaults and validate the
	// configuration before calling this function. This means that the
	// configuration will always contain all keys defined in Parameters
	// (unprovided keys will have their default values) and all non-empty
	// values will be of the correct type.
	Configure(context.Context, map[string]string) error

	// Open is called after Configure to signal the plugin it can prepare to
	// start producing records. If needed, the plugin should open connections in
	// this function. The position parameter will contain the position of the
	// last record that was successfully processed, Source should therefore
	// start producing records after this position. The context passed to Open
	// will be cancelled once the plugin receives a stop signal from Conduit.
	Open(context.Context, opencdc.Position) error

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
	Read(context.Context) (opencdc.Record, error)
	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled, since there might be
	// outstanding acks that need to be delivered. When Teardown is called it is
	// guaranteed there won't be any more calls to Ack.
	// Ack can be called concurrently with Read.
	Ack(context.Context, opencdc.Position) error

	// Teardown signals to the plugin that there will be no more calls to any
	// other function. After Teardown returns, the plugin should be ready for a
	// graceful shutdown.
	Teardown(context.Context) error

	// -- Lifecycle events -----------------------------------------------------

	// LifecycleOnCreated is called after Configure and before Open when the
	// connector is run for the first time. This call will be skipped if the
	// connector was already started before. This method can be used to do some
	// initialization that needs to happen only once in the lifetime of a
	// connector (e.g. create a logical replication slot). Anything that the
	// connector creates in this method is considered to be owned by this
	// connector and should be cleaned up in LifecycleOnDeleted.
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

	mustEmbedUnimplementedSource()
}

// NewSourcePlugin takes a Source and wraps it into an adapter that converts it
// into a cpluginv2.SourcePlugin. If the parameter is nil it will wrap
// UnimplementedSource instead.
func NewSourcePlugin(impl Source) cpluginv2.SourcePlugin {
	if impl == nil {
		// prevent nil pointers
		impl = UnimplementedSource{}
	}
	return &sourcePluginAdapter{impl: impl}
}

type sourcePluginAdapter struct {
	impl Source

	state internal.ConnectorStateWatcher

	// readDone will be closed after runRead stops running.
	readDone chan struct{}

	// lastPosition stores the position of the last record sent to Conduit.
	lastPosition opencdc.Position

	openCancel context.CancelFunc
	readCancel context.CancelFunc
	t          *tomb.Tomb
}

func (a *sourcePluginAdapter) Configure(ctx context.Context, req cpluginv2.SourceConfigureRequest) (cpluginv2.SourceConfigureResponse, error) {
	err := a.state.DoWithLock(ctx, internal.DoWithLockOptions{
		ExpectedStates:       []internal.ConnectorState{internal.StateInitial},
		StateBefore:          internal.StateConfiguring,
		StateAfter:           internal.StateConfigured,
		WaitForExpectedState: false,
	}, func(_ internal.ConnectorState) error {
		params := a.impl.Parameters()

		// TODO should we stop doing this here? The Processor SDK does NOT do this.
		// sanitize config and apply default values
		cfg := config.Config(req.Config).
			Sanitize().
			ApplyDefaults(params)

		// run builtin validations
		err1 := cfg.Validate(params)
		// run custom validations written by developer
		err2 := a.impl.Configure(ctx, cfg)

		return errors.Join(err1, err2)
	})

	return cpluginv2.SourceConfigureResponse{}, err
}

func (a *sourcePluginAdapter) Start(ctx context.Context, req cpluginv2.SourceStartRequest) (cpluginv2.SourceStartResponse, error) {
	err := a.state.DoWithLock(ctx, internal.DoWithLockOptions{
		ExpectedStates:       []internal.ConnectorState{internal.StateConfigured},
		StateBefore:          internal.StateStarting,
		StateAfter:           internal.StateStarted,
		WaitForExpectedState: false,
	}, func(_ internal.ConnectorState) error {
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

		return a.impl.Open(ctxOpen, req.Position)
	})

	return cpluginv2.SourceStartResponse{}, err
}

func (a *sourcePluginAdapter) Run(ctx context.Context, stream cpluginv2.SourceRunStream) (err error) {
	err = a.state.DoWithLock(ctx, internal.DoWithLockOptions{
		ExpectedStates:       []internal.ConnectorState{internal.StateStarted},
		StateBefore:          internal.StateInitiatingRun,
		StateAfter:           internal.StateRunning,
		WaitForExpectedState: false,
	}, func(_ internal.ConnectorState) error {
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
		return nil
	})
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			a.state.Set(internal.StateErrored)
		} else {
			a.state.Set(internal.StateStopped)
		}
	}()

	<-a.t.Dying() // stop as soon as it's dying
	return a.t.Err()
}

func (a *sourcePluginAdapter) runRead(ctx context.Context, stream cpluginv2.SourceRunStream) error {
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
				_, _, err := cchan.ChanOut[time.Time](time.After(b.Duration())).Recv(ctx)
				if err != nil {
					// the plugin is using the SDK for long polling and relying
					// on the SDK to check for a cancelled context
					return nil
				}
				continue
			}
			return fmt.Errorf("read plugin error: %w", err)
		}

		err = stream.Send(cpluginv2.SourceRunResponse{Record: r})
		if err != nil {
			return fmt.Errorf("read stream error: %w", err)
		}
		a.lastPosition = r.Position // store last sent position

		// reset backoff retry
		b.Reset()
	}
}

func (a *sourcePluginAdapter) runAck(ctx context.Context, stream cpluginv2.SourceRunStream) error {
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

func (a *sourcePluginAdapter) Stop(ctx context.Context, _ cpluginv2.SourceStopRequest) (cpluginv2.SourceStopResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, stopTimeout)
	defer cancel()

	err := a.state.DoWithLock(ctx, internal.DoWithLockOptions{
		ExpectedStates: []internal.ConnectorState{
			internal.StateRunning, internal.StateStopping, internal.StateTornDown, internal.StateErrored,
		},
		StateBefore:          internal.StateInitiatingStop,
		StateAfter:           internal.StateStopping,
		WaitForExpectedState: true, // wait for one of the expected states
	}, func(state internal.ConnectorState) error {
		if state != internal.StateRunning {
			// stop already executed or we errored out, in any case we don't do anything
			Logger(ctx).Warn().Str("state", state.String()).Msg("connector state is not \"Running\", skipping stop")
			return nil
		}

		// stop reading new messages
		a.openCancel()
		a.readCancel()
		return nil
	})
	if err != nil {
		return cpluginv2.SourceStopResponse{}, fmt.Errorf("failed to stop connector: %w", err)
	}

	// wait for read to actually stop running with a timeout, in case the
	// connector gets stuck
	_, _, err = cchan.ChanOut[struct{}](a.readDone).Recv(ctx)
	if err != nil {
		Logger(ctx).Warn().Err(err).Msg("failed to wait for Read to stop running")
		return cpluginv2.SourceStopResponse{}, fmt.Errorf("failed to stop connector: %w", err)
	}

	return cpluginv2.SourceStopResponse{
		LastPosition: a.lastPosition,
	}, nil
}

func (a *sourcePluginAdapter) Teardown(ctx context.Context, _ cpluginv2.SourceTeardownRequest) (cpluginv2.SourceTeardownResponse, error) {
	err := a.state.DoWithLock(ctx, internal.DoWithLockOptions{
		ExpectedStates: nil, // Teardown can be called from any state
		StateBefore:    internal.StateTearingDown,
		StateAfter:     internal.StateTornDown,
	}, func(state internal.ConnectorState) error {
		// cancel open and read context, in case Stop was not called (can happen in
		// case the stop was triggered by an error)
		// teardown can be called without "open" or "read" being called previously
		// e.g. when Conduit is validating a connector configuration,
		// it will call "configure" and then "teardown".
		if a.openCancel != nil {
			a.openCancel()
		}
		if a.readCancel != nil {
			a.readCancel()
		}

		var waitErr error
		if a.t != nil {
			waitErr = a.waitForRun(ctx, teardownTimeout) // wait for Run to stop running
			if waitErr != nil {
				// just log error and continue to call Teardown to keep guarantee
				Logger(ctx).Warn().Err(waitErr).Msg("failed to wait for Run to stop running")
				// kill tomb to release Run
				a.t.Kill(errors.New("forceful teardown"))
			}
		}

		err := a.impl.Teardown(ctx)
		if err == nil {
			err = waitErr
		}
		return err
	})

	return cpluginv2.SourceTeardownResponse{}, err
}

func (a *sourcePluginAdapter) LifecycleOnCreated(ctx context.Context, req cpluginv2.SourceLifecycleOnCreatedRequest) (cpluginv2.SourceLifecycleOnCreatedResponse, error) {
	return cpluginv2.SourceLifecycleOnCreatedResponse{}, a.impl.LifecycleOnCreated(ctx, req.Config)
}
func (a *sourcePluginAdapter) LifecycleOnUpdated(ctx context.Context, req cpluginv2.SourceLifecycleOnUpdatedRequest) (cpluginv2.SourceLifecycleOnUpdatedResponse, error) {
	return cpluginv2.SourceLifecycleOnUpdatedResponse{}, a.impl.LifecycleOnUpdated(ctx, req.ConfigBefore, req.ConfigAfter)
}
func (a *sourcePluginAdapter) LifecycleOnDeleted(ctx context.Context, req cpluginv2.SourceLifecycleOnDeletedRequest) (cpluginv2.SourceLifecycleOnDeletedResponse, error) {
	return cpluginv2.SourceLifecycleOnDeletedResponse{}, a.impl.LifecycleOnDeleted(ctx, req.Config)
}

// waitForRun returns once the Run function returns or the context gets
// cancelled, whichever happens first. If the context gets cancelled the context
// error will be returned.
func (a *sourcePluginAdapter) waitForRun(ctx context.Context, timeout time.Duration) error {
	// wait for all acks to be sent back to Conduit, stop waiting if context
	// gets cancelled or timeout is reached
	return csync.Run(
		ctx,
		func() { _ = a.t.Wait() }, // ignore tomb error, it will be returned in Run anyway
		csync.WithTimeout(timeout),
	)
}

// SourceUtil provides utility methods for implementing a source. Use it by
// calling Util.Source.*.
type SourceUtil struct{}

// NewRecordCreate can be used to instantiate a record with OperationCreate.
func (SourceUtil) NewRecordCreate(
	position opencdc.Position,
	metadata opencdc.Metadata,
	key opencdc.Data,
	payload opencdc.Data,
) opencdc.Record {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata.SetReadAt(time.Now())
	return opencdc.Record{
		Position:  position,
		Operation: opencdc.OperationCreate,
		Metadata:  metadata,
		Key:       key,
		Payload: opencdc.Change{
			After: payload,
		},
	}
}

// NewRecordSnapshot can be used to instantiate a record with OperationSnapshot.
func (SourceUtil) NewRecordSnapshot(
	position opencdc.Position,
	metadata opencdc.Metadata,
	key opencdc.Data,
	payload opencdc.Data,
) opencdc.Record {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata.SetReadAt(time.Now())
	return opencdc.Record{
		Position:  position,
		Operation: opencdc.OperationSnapshot,
		Metadata:  metadata,
		Key:       key,
		Payload: opencdc.Change{
			After: payload,
		},
	}
}

// NewRecordUpdate can be used to instantiate a record with OperationUpdate.
func (SourceUtil) NewRecordUpdate(
	position opencdc.Position,
	metadata opencdc.Metadata,
	key opencdc.Data,
	payloadBefore opencdc.Data,
	payloadAfter opencdc.Data,
) opencdc.Record {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata.SetReadAt(time.Now())
	return opencdc.Record{
		Position:  position,
		Operation: opencdc.OperationUpdate,
		Metadata:  metadata,
		Key:       key,
		Payload: opencdc.Change{
			Before: payloadBefore,
			After:  payloadAfter,
		},
	}
}

// NewRecordDelete can be used to instantiate a record with OperationDelete.
func (SourceUtil) NewRecordDelete(
	position opencdc.Position,
	metadata opencdc.Metadata,
	key opencdc.Data,
) opencdc.Record {
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata.SetReadAt(time.Now())
	return opencdc.Record{
		Position:  position,
		Operation: opencdc.OperationDelete,
		Metadata:  metadata,
		Key:       key,
	}
}
