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

//go:generate mockgen -typed -destination=mock_destination_test.go -self_package=github.com/conduitio/conduit-connector-sdk -package=sdk -write_package_comment=false . Destination

package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/conduitio/conduit-commons/ccontext"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-sdk/internal"
)

// Destination receives records from Conduit and writes them to 3rd party
// resources.
// All implementations must embed UnimplementedDestination for forward
// compatibility.
type Destination interface {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination.
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
	Configure(context.Context, config.Config) error

	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
	Open(context.Context) error

	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).
	Write(ctx context.Context, r []opencdc.Record) (n int, err error)

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
	LifecycleOnCreated(ctx context.Context, config config.Config) error
	// LifecycleOnUpdated is called after Configure and before Open when the
	// connector configuration has changed since the last run. This call will be
	// skipped if the connector configuration did not change. It can be used to
	// update anything that was initialized in LifecycleOnCreated, in case the
	// configuration change affects it.
	LifecycleOnUpdated(ctx context.Context, configBefore, configAfter config.Config) error
	// LifecycleOnDeleted is called when the connector was deleted. It will be
	// the only method that is called in that case. This method can be used to
	// clean up anything that was initialized in LifecycleOnCreated.
	LifecycleOnDeleted(ctx context.Context, config config.Config) error

	mustEmbedUnimplementedDestination()
}

// NewDestinationPlugin takes a Destination and wraps it into an adapter that
// converts it into a pconnector.DestinationPlugin. If the parameter is nil it
// will wrap UnimplementedDestination instead.
func NewDestinationPlugin(impl Destination, cfg pconnector.PluginConfig) pconnector.DestinationPlugin {
	if impl == nil {
		// prevent nil pointers
		impl = UnimplementedDestination{}
	}
	return &destinationPluginAdapter{impl: impl, cfg: cfg}
}

type destinationPluginAdapter struct {
	impl Destination
	cfg  pconnector.PluginConfig

	lastPosition *csync.ValueWatcher[opencdc.Position]
	openCancel   context.CancelFunc

	// write is the chosen write strategy, either single records or batches
	writeStrategy writeStrategy
}

func (a *destinationPluginAdapter) Configure(ctx context.Context, req pconnector.DestinationConfigureRequest) (pconnector.DestinationConfigureResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)
	ctx = (&destinationWithBatch{}).setBatchConfig(ctx, DestinationWithBatchConfig{})

	err := a.impl.Configure(ctx, req.Config)
	if err != nil {
		return pconnector.DestinationConfigureResponse{}, err
	}

	a.configureWriteStrategy(ctx)
	return pconnector.DestinationConfigureResponse{}, nil
}

func (a *destinationPluginAdapter) configureWriteStrategy(ctx context.Context) {
	a.writeStrategy = &writeStrategySingle{impl: a.impl} // by default we write single records

	batchConfig := (&destinationWithBatch{}).getBatchConfig(ctx)
	if batchConfig.BatchSize > 1 || batchConfig.BatchDelay > 0 {
		a.writeStrategy = newWriteStrategyBatch(a.impl, batchConfig.BatchSize, batchConfig.BatchDelay)
	}
}

func (a *destinationPluginAdapter) Open(ctx context.Context, _ pconnector.DestinationOpenRequest) (pconnector.DestinationOpenResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)

	a.lastPosition = new(csync.ValueWatcher[opencdc.Position])

	// detach context, so we can control when it's canceled
	ctxOpen := ccontext.Detach(ctx)
	ctxOpen, a.openCancel = context.WithCancel(ctxOpen)

	openDone := make(chan struct{})
	defer close(openDone)
	go func() {
		// for duration of the Open call we propagate the cancellation of ctx to
		// ctxOpen, after Open returns we decouple the context and let it live
		// until the plugin should stop running
		select {
		case <-ctx.Done():
			a.openCancel()
		case <-openDone:
			// start finished before ctx was canceled, leave context open
		}
	}()

	err := a.impl.Open(ctxOpen)
	return pconnector.DestinationOpenResponse{}, err
}

func (a *destinationPluginAdapter) Run(ctx context.Context, stream pconnector.DestinationRunStream) error {
	ctx = internal.Enrich(ctx, a.cfg)

	for stream := stream.Server(); ; {
		batch, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// stream is closed
				return nil
			}
			return fmt.Errorf("write stream error: %w", err)
		}

		for _, rec := range batch.Records {
			err = a.writeStrategy.Write(ctx, rec, func(err error) error {
				return a.ack(rec, err, stream)
			})
			a.lastPosition.Set(rec.Position)
			if err != nil {
				return err
			}
		}
	}
}

// ack sends a message into the stream signaling that the record was processed.
func (a *destinationPluginAdapter) ack(r opencdc.Record, writeErr error, stream pconnector.DestinationRunStreamServer) error {
	var ackErrStr string
	if writeErr != nil {
		ackErrStr = writeErr.Error()
	}
	err := stream.Send(pconnector.DestinationRunResponse{
		Acks: []pconnector.DestinationRunResponseAck{{
			Position: r.Position,
			Error:    ackErrStr,
		}},
	})
	if err != nil {
		return fmt.Errorf("ack stream error: %w", err)
	}
	return nil
}

// Stop will initiate the stop of the destination connector. It will first wait
// that the last position processed by the connector matches the last position
// in the request and then trigger a flush, in case there are any cached records
// (relevant in case of batching).
// If the requested last position is not encountered in 1 minute it will proceed
// flushing records received so far and return an error. Flushing of records
// also has a timeout of 1 minute, after which the stop operation returns with
// an error. In the worst case this operation can thus take 2 minutes.
func (a *destinationPluginAdapter) Stop(ctx context.Context, req pconnector.DestinationStopRequest) (pconnector.DestinationStopResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)

	// last thing we do is cancel context in Open
	defer a.openCancel()

	// wait for last record to be received, if it doesn't arrive in time we try
	// to flush what we have so far
	watchCtx, watchCancel := context.WithTimeout(ctx, stopTimeout)
	defer watchCancel()

	actualLastPosition, err := a.lastPosition.Watch(
		watchCtx,
		func(val opencdc.Position) bool {
			return bytes.Equal(val, req.LastPosition)
		},
	)
	if err != nil {
		err = fmt.Errorf("did not encounter expected last position %q, actual last position %q: %w", req.LastPosition, actualLastPosition, err)
		Logger(ctx).Warn().Err(err).Msg("proceeding to flush records that were received so far (other records won't be acked)")
	}

	// flush cached records, allow it to take at most 1 minute
	flushCtx, flushCancel := context.WithTimeout(ctx, stopTimeout)
	defer flushCancel()

	flushErr := a.writeStrategy.Flush(flushCtx)
	if flushErr != nil && err == nil {
		err = flushErr
	} else if flushErr != nil {
		Logger(ctx).Err(err).Msg("error flushing records")
	}

	return pconnector.DestinationStopResponse{}, err
}

func (a *destinationPluginAdapter) Teardown(ctx context.Context, _ pconnector.DestinationTeardownRequest) (pconnector.DestinationTeardownResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)

	// cancel open context, in case Stop was not called (can happen in case the
	// stop was triggered by an error)
	// teardown can be called without "open" being called previously
	// e.g. when Conduit is validating a connector configuration,
	// it will call "configure" and then "teardown".
	if a.openCancel != nil {
		a.openCancel()
	}

	err := a.impl.Teardown(ctx)
	if err != nil {
		return pconnector.DestinationTeardownResponse{}, err
	}
	return pconnector.DestinationTeardownResponse{}, nil
}

func (a *destinationPluginAdapter) LifecycleOnCreated(ctx context.Context, req pconnector.DestinationLifecycleOnCreatedRequest) (pconnector.DestinationLifecycleOnCreatedResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)

	return pconnector.DestinationLifecycleOnCreatedResponse{}, a.impl.LifecycleOnCreated(ctx, req.Config)
}

func (a *destinationPluginAdapter) LifecycleOnUpdated(ctx context.Context, req pconnector.DestinationLifecycleOnUpdatedRequest) (pconnector.DestinationLifecycleOnUpdatedResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)

	return pconnector.DestinationLifecycleOnUpdatedResponse{}, a.impl.LifecycleOnUpdated(ctx, req.ConfigBefore, req.ConfigAfter)
}

func (a *destinationPluginAdapter) LifecycleOnDeleted(ctx context.Context, req pconnector.DestinationLifecycleOnDeletedRequest) (pconnector.DestinationLifecycleOnDeletedResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)

	return pconnector.DestinationLifecycleOnDeletedResponse{}, a.impl.LifecycleOnDeleted(ctx, req.Config)
}

// writeStrategy is used to switch between writing single records and batching
// them.
type writeStrategy interface {
	Write(ctx context.Context, r opencdc.Record, ack func(error) error) error
	Flush(ctx context.Context) error
}

// writeStrategySingle will write records synchronously one by one without
// caching them. Acknowledgments are sent back to Conduit right after they are
// written.
type writeStrategySingle struct {
	impl Destination
}

func (w *writeStrategySingle) Write(ctx context.Context, r opencdc.Record, ack func(error) error) error {
	_, err := w.impl.Write(ctx, []opencdc.Record{r})
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
	ctx    context.Context //nolint:containedctx // We need the context to pass it to Write.
	record opencdc.Record
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
	records := make([]opencdc.Record, len(batch))
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
		ackResponse error
		firstErr    error
		errOnce     bool
	)
	for i, item := range batch {
		if i == n {
			// records from this index on failed to be written, include the
			// error in the response
			ackResponse = err
		}
		err := item.ack(ackResponse)
		if err != nil && !errOnce {
			firstErr = err
			errOnce = true
		}
	}
	return firstErr
}

func (w *writeStrategyBatch) Write(ctx context.Context, r opencdc.Record, ack func(error) error) error {
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
//	func (d *Destination) Write(ctx context.Context, r opencdc.Record) error {
//	  return d.Util.Route(ctx, r,
//	    d.handleInsert,
//	    d.handleUpdate,
//	    d.handleDelete,
//	    d.handleSnapshot, // we could also reuse d.handleInsert
//	  )
//	}
//	func (d *Destination) handleInsert(ctx context.Context, r opencdc.Record) error {
//	  ...
//	}
func (DestinationUtil) Route(
	ctx context.Context,
	rec opencdc.Record,
	handleCreate func(context.Context, opencdc.Record) error,
	handleUpdate func(context.Context, opencdc.Record) error,
	handleDelete func(context.Context, opencdc.Record) error,
	handleSnapshot func(context.Context, opencdc.Record) error,
) error {
	switch rec.Operation {
	case opencdc.OperationCreate:
		return handleCreate(ctx, rec)
	case opencdc.OperationUpdate:
		return handleUpdate(ctx, rec)
	case opencdc.OperationDelete:
		return handleDelete(ctx, rec)
	case opencdc.OperationSnapshot:
		return handleSnapshot(ctx, rec)
	default:
		return fmt.Errorf("invalid operation %q", rec.Operation)
	}
}
