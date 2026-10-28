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
	"sync"
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
	// Config returns the configuration that the destination expects. It should
	// return a pointer to a struct that contains all the configuration keys that
	// the destination expects. The struct should be annotated with the necessary
	// validation tags. The value should be a pointer to allow the SDK to
	// populate it using the values from the configuration.
	//
	// The returned DestinationConfig should contain all the configuration keys
	// that the destination expects, including middleware fields (see
	// [DefaultDestinationMiddleware]).
	Config() DestinationConfig

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

// DestinationConfig represents the configuration containing all configuration
// keys that a destination expects. The type needs to implement [Validatable],
// which will be used to automatically validate the config when configuring the
// connector.
type DestinationConfig interface {
	Validatable

	mustEmbedUnimplementedDestinationConfig()
}

// NewDestinationPlugin takes a Destination and wraps it into an adapter that
// converts it into a pconnector.DestinationPlugin. If the parameter is nil it
// will wrap UnimplementedDestination instead.
func NewDestinationPlugin(impl Destination, cfg pconnector.PluginConfig, parameters config.Parameters) pconnector.DestinationPlugin {
	if impl == nil {
		// prevent nil pointers
		impl = UnimplementedDestination{}
	}
	return &destinationPluginAdapter{
		impl:       impl,
		cfg:        cfg,
		parameters: parameters,
	}
}

type destinationPluginAdapter struct {
	impl       Destination
	cfg        pconnector.PluginConfig
	parameters config.Parameters

	// lastPosition holds the position of the last record passed to the connector's
	// Write method. It is used to determine when the connector should stop.
	lastPosition *csync.ValueWatcher[opencdc.Position]
	openCancel   context.CancelFunc

	// write is the chosen write strategy, either single records or batches
	writeStrategy writeStrategy
}

func (a *destinationPluginAdapter) Configure(ctx context.Context, req pconnector.DestinationConfigureRequest) (pconnector.DestinationConfigureResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)

	cfg := a.impl.Config()
	if cfg == nil {
		// Connector without a config. Nothing to do.
		return pconnector.DestinationConfigureResponse{}, nil
	}

	err := Util.ParseConfig(ctx, req.Config, cfg, a.parameters)
	if err != nil {
		return pconnector.DestinationConfigureResponse{}, fmt.Errorf("failed to parse configuration: %w", err)
	}

	return pconnector.DestinationConfigureResponse{}, nil
}

func (a *destinationPluginAdapter) Open(ctx context.Context, _ pconnector.DestinationOpenRequest) (pconnector.DestinationOpenResponse, error) {
	ctx = internal.Enrich(ctx, a.cfg)
	ctx = (&destinationWithBatch{}).setBatchConfig(ctx, DestinationWithBatch{})

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
	a.configureWriteStrategy(ctxOpen)

	// Invariant 1 (ack correctness): declare the ack-order guarantee the engine
	// relies on to run several writes in flight. It holds because Run handles
	// requests sequentially and writeStrategySingle.Write acks a batch in
	// record order (acks for batch[:n], then the nack for batch[n:]) before
	// Run receives the next request; the batch strategy serializes its flushes
	// under the Batcher mutex in enqueue order, so batching keeps the order too.
	// See ConduitIO/conduit#3292 and the protocol field ordered_acks.
	return pconnector.DestinationOpenResponse{OrderedAcks: true}, err
}

// defaultMaxBatchSize bounds the destination write batch when only
// sdk.batch.delay is set (no explicit sdk.batch.size), so the delay timer
// coalesces records instead of flushing one per Write and a fast source cannot
// grow the pending buffer without limit. It mirrors the arch-v2 engine's
// defaultMaxBatchSize (pkg/lifecycle-poc/funnel/destination_batch.go).
const defaultMaxBatchSize = 1000

func (a *destinationPluginAdapter) configureWriteStrategy(ctx context.Context) {
	writeSingle := &writeStrategySingle{impl: a.impl, ackFn: a.ack}
	a.writeStrategy = writeSingle // by default we write single records

	batchConfig := (&destinationWithBatch{}).getBatchConfig(ctx)
	if batchConfig.BatchDelay > 0 && batchConfig.BatchSize <= 1 {
		// Delay-only batching (sdk.batch.delay set, sdk.batch.size unset or 1):
		// the batcher flushes when the size threshold is reached, and a
		// threshold <= 1 would flush every record immediately, ignoring the
		// delay entirely. Bound the buffer with a default max size so the delay
		// actually coalesces records.
		batchConfig.BatchSize = defaultMaxBatchSize
	}
	if batchConfig.BatchSize > 1 || batchConfig.BatchDelay > 0 {
		a.writeStrategy = newWriteStrategyBatch(writeSingle, batchConfig.BatchSize, batchConfig.BatchDelay)
	}
}

func (a *destinationPluginAdapter) Run(ctx context.Context, stream pconnector.DestinationRunStream) error {
	ctx = internal.Enrich(ctx, a.cfg)
	a.writeStrategy.SetStream(stream.Server())

	for stream := stream.Server(); ; {
		batch, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// stream is closed
				return nil
			}
			return fmt.Errorf("write stream error: %w", err)
		}

		if len(batch.Records) == 0 {
			// Nothing to write and nothing to ack: an empty request must not
			// reach the connector, nor index into Records below.
			continue
		}

		err = a.writeStrategy.Write(ctx, batch.Records)
		a.lastPosition.Set(batch.Records[len(batch.Records)-1].Position)
		if err != nil {
			return err
		}
	}
}

// ack sends a message into the stream signaling that the record was processed.
func (a *destinationPluginAdapter) ack(r []opencdc.Record, writeErr error, stream pconnector.DestinationRunStreamServer) error {
	if len(r) == 0 {
		return nil
	}

	var ackErrStr string
	if writeErr != nil {
		ackErrStr = writeErr.Error()
	}

	acks := make([]pconnector.DestinationRunResponseAck, len(r))
	for i, rec := range r {
		acks[i] = pconnector.DestinationRunResponseAck{
			Position: rec.Position,
			Error:    ackErrStr,
		}
	}

	err := stream.Send(pconnector.DestinationRunResponse{
		Acks: acks,
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

// Teardown waits for a running Write, refuses later ones and calls the
// connector's Teardown.
//
// Once Teardown has begun (or the Run stream has ended) the adapter sends no
// further acks, so the acks sent always form a prefix of the records received
// (see the ordered_acks guarantee in the protocol). Every record not acked by
// then stays unanswered, neither acked nor nacked: records that reach the
// adapter after Teardown started, records still buffered by the batch write
// strategy, and the records of a Write that Teardown gave up waiting for. They
// are not durable; Conduit redelivers them after a restart instead of routing
// them to a DLQ.
//
// The wait is bounded by min(ctx, destinationTeardownTimeout), 1 minute at
// most, so a Teardown ctx that is already canceled abandons immediately. If a Write is
// still running when it expires, Teardown gives up on it and goes on: the
// connector's Teardown is called while that Write is still running, which
// breaks the connector's "no other call after Teardown" guarantee, but keeping
// the guarantee that Teardown is called wins over waiting forever. The
// abandoned Write is not acked when it returns, whatever it returns, and
// Teardown returns an error that says so.
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

	// Teardown can arrive without a prior Stop, while Run is still inside the
	// connector's Write. The Destination contract promises the connector that
	// no other call happens once Teardown starts, so wait (bounded) for
	// running writes to return and refuse new ones. Like the source adapter,
	// log and carry on if a write does not return in time, to keep the
	// guarantee that Teardown is called.
	var waitErr error
	if a.writeStrategy != nil {
		if single := a.singleStrategy(); single != nil {
			if err := single.closeAndWait(ctx, destinationTeardownTimeout); err != nil {
				waitErr = fmt.Errorf("failed to wait for Write to return: %w", err)
				Logger(ctx).Warn().Err(err).Msg("failed to wait for Write to return, tearing down anyway")
			}
		}
	}

	err := a.impl.Teardown(ctx)
	if err == nil {
		err = waitErr
	}
	if err != nil {
		return pconnector.DestinationTeardownResponse{}, err
	}
	return pconnector.DestinationTeardownResponse{}, nil
}

// singleStrategy returns the strategy that calls the connector's Write, which
// is the write strategy itself or, with batching, the one it wraps.
func (a *destinationPluginAdapter) singleStrategy() *writeStrategySingle {
	switch ws := a.writeStrategy.(type) {
	case *writeStrategySingle:
		return ws
	case *writeStrategyBatch:
		return ws.writer
	}
	return nil
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
	Write(ctx context.Context, recs []opencdc.Record) error
	Flush(ctx context.Context) error
	SetStream(pconnector.DestinationRunStreamServer)
}

// writeStrategySingle will write batches synchronously without caching them.
// Acknowledgments are sent back to Conduit right after they are written.
type writeStrategySingle struct {
	impl Destination

	// inflight counts Write calls that are running inside the connector, so
	// Teardown can wait for them (see closeAndWait). closed is set once
	// Teardown starts; later writes are refused instead of reaching a
	// connector that was promised no more calls. abandoned is set when
	// Teardown stopped waiting for a Write that was still running; that Write
	// must not ack when it finally returns. idle is closed when inflight drops
	// to zero while closeAndWait is parked on it.
	mu        sync.Mutex
	inflight  int
	closed    bool
	abandoned bool
	idle      chan struct{}

	ackFn  func([]opencdc.Record, error, pconnector.DestinationRunStreamServer) error
	stream pconnector.DestinationRunStreamServer
}

// enter registers a Write call. It returns false if Teardown already started.
func (w *writeStrategySingle) enter() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	w.inflight++
	return true
}

// leave deregisters a Write call. It reports whether Teardown gave up waiting
// for it, in which case the Write's result must not be acked.
func (w *writeStrategySingle) leave() (abandoned bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight--
	if w.inflight == 0 && w.idle != nil {
		close(w.idle)
		w.idle = nil
	}
	return w.abandoned
}

// closeAndWait refuses new writes and waits, at most timeout, for the ones
// running inside the connector to return. It starts no goroutine, so giving up
// leaves nothing behind. On timeout the running writes are marked abandoned.
func (w *writeStrategySingle) closeAndWait(ctx context.Context, timeout time.Duration) error {
	w.mu.Lock()
	w.closed = true
	if w.inflight == 0 {
		w.mu.Unlock()
		return nil
	}
	if w.idle == nil {
		w.idle = make(chan struct{})
	}
	idle := w.idle
	w.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.inflight == 0 {
			return nil // finished while we were giving up
		}
		w.abandoned = true
		return ctx.Err()
	}
}

func (w *writeStrategySingle) Write(ctx context.Context, batch []opencdc.Record) error {
	if !w.enter() {
		// Teardown started. Leave the records unanswered: neither ack nor
		// nack. A nack can make Conduit route them to the DLQ (with a
		// non-zero threshold the DLQ handler acks them at the source), which
		// would create spurious DLQ entries for records that were never
		// attempted. Unanswered records are redelivered after a restart
		// (at-least-once). This does not block the stream: Run keeps receiving
		// until the stream is closed.
		Logger(ctx).Warn().
			Int("records", len(batch)).
			Msg("records received after Teardown started were not written and are left unacknowledged")
		return nil
	}
	n, err := w.impl.Write(ctx, batch)
	invalidN := n < 0 || n > len(batch)
	if w.leave() {
		// Teardown gave up waiting for this Write and already called the
		// connector's Teardown. Whatever the connector reports now, the
		// outcome is not acked: the records are redelivered after a restart.
		Logger(ctx).Warn().
			Int("records", len(batch)).
			Msg("Write returned after Teardown gave up waiting for it, not acknowledging its records")
		return nil
	}
	if invalidN {
		// Invariant 1: never ack more than the connector reported as written.
		// An out-of-range n would panic when slicing the batch and leave the
		// batch unanswered, so nack every record instead.
		invalid := fmt.Errorf("connector reported %d written records for a batch of %d, this is probably a bug in the connector", n, len(batch))
		if err != nil {
			invalid = fmt.Errorf("%w (original error: %w)", invalid, err)
		}
		err = invalid
		n = 0
	}
	if err != nil {
		var pos []byte
		if n < len(batch) {
			pos = batch[n].Position
		}
		Logger(ctx).Err(err).Bytes("record_position", pos).Msg("error writing record")
	}

	if n == len(batch) && err != nil {
		err = fmt.Errorf("connector reported a successful write of all records in the batch and simultaneously returned an error, this is probably a bug in the connector. Original error: %w", err)
		n = 0 // nack all messages in the batch
	} else if n < len(batch) && err == nil {
		err = fmt.Errorf("batch contained %d messages, connector has only written %d without reporting the error, this is probably a bug in the connector", len(batch), n)
	}

	ackErr := w.ackFn(batch[:n], nil, w.stream)
	if ackErr != nil {
		return fmt.Errorf("ack error: %w", ackErr)
	}
	ackErr = w.ackFn(batch[n:], err, w.stream)
	if ackErr != nil {
		return fmt.Errorf("ack error: %w", ackErr)
	}

	return nil
}

func (w *writeStrategySingle) Flush(context.Context) error {
	return nil // nothing to flush
}

func (w *writeStrategySingle) SetStream(stream pconnector.DestinationRunStreamServer) {
	w.stream = stream
}

// writeStrategyBatch will cache records before writing them. Records are
// grouped into batches that get written when they reach the size batchSize or
// when the time since adding the first record to the current batch reaches
// batchDelay.
type writeStrategyBatch struct {
	batcher *internal.Batcher[writeBatchItem]

	writer *writeStrategySingle
}

type writeBatchItem struct {
	ctx     context.Context //nolint:containedctx // We need the context to pass it to Write.
	records []opencdc.Record
}

func newWriteStrategyBatch(writer *writeStrategySingle, batchSize int, batchDelay time.Duration) *writeStrategyBatch {
	strategy := &writeStrategyBatch{
		writer: writer,
	}
	strategy.batcher = internal.NewBatcher(
		batchSize,
		batchDelay,
		strategy.flushBatch,
	)
	return strategy
}

func (w *writeStrategyBatch) flushBatch(batch []writeBatchItem, batchSize int) error {
	records := make([]opencdc.Record, 0, batchSize)
	for _, item := range batch {
		records = append(records, item.records...)
	}
	// use the last record's context as the write context
	ctx := batch[len(batch)-1].ctx

	return w.writer.Write(ctx, records)
}

func (w *writeStrategyBatch) Write(ctx context.Context, recs []opencdc.Record) error {
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
		ctx:     ctx,
		records: recs,
	}, len(recs))

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

// Flush writes the cached records, if any, and reports the result of the last
// batch written.
//
// When nothing is cached, no new result is coming, so Flush must not wait for
// one (ConduitIO/conduit#3092). That is the normal state after a drain: the
// last Write either filled a batch and consumed its result itself, or the
// delay timer flushed it and left the result buffered. A buffered result is
// still reported, without waiting.
func (w *writeStrategyBatch) Flush(ctx context.Context) error {
	if !w.batcher.Flush() {
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
			// The last result was already consumed by Write.
		}
		return nil
	}
	// Invariant 1: the cached records were just written; their acks were sent
	// by the write itself, so we only collect the result here.
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

func (w *writeStrategyBatch) SetStream(stream pconnector.DestinationRunStreamServer) {
	w.writer.SetStream(stream)
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
