// Copyright © 2026 Meroxa, Inc.
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
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/cchan"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jpillora/backoff"
)

const (
	batchSizeParam  = "sdk.batch.size"
	batchDelayParam = "sdk.batch.delay"
)

// batchMode is how a source batches, after the configuration is resolved.
type batchMode int

const (
	// batchModePassthrough: no batching, ReadN is forwarded to the connector
	// as it was before batching existed.
	batchModePassthrough batchMode = iota
	// batchModeAdaptive: block for the first record, then take what is already
	// queued up to the batch size, without a timer.
	batchModeAdaptive
	// batchModeWindow: collect until the batch is full or the delay, started
	// at the first record, expires.
	batchModeWindow
	// batchModeUntilFull: collect until the batch is full, with no deadline.
	// This is the pre-existing meaning of an explicit size with delay 0. It is
	// deprecated; see [CodeBatchDrainSemanticsPending].
	batchModeUntilFull
)

func (m batchMode) String() string {
	switch m {
	case batchModePassthrough:
		return "passthrough"
	case batchModeAdaptive:
		return "adaptive"
	case batchModeWindow:
		return "window"
	case batchModeUntilFull:
		return "until-full"
	default:
		return "invalid"
	}
}

// SourceWithBatch adds support for batching on the source.
//
// By default a source hands the engine up to 100 records per read
// (sdk.batch.size=100, sdk.batch.delay=0). With a delay of 0 the batch is
// adaptive: the SDK blocks for the first record, takes whatever the read-ahead
// has already queued (up to the size) and returns without waiting on a timer.
// A busy engine therefore gets full batches and a sparse source gets
// one-record batches with no added latency. A delay above 0 is a fixed window:
// the batch is returned when it is full or when the delay, started at the
// first record, expires.
//
// Setting both parameters to 0 turns batching off and restores one record per
// read. The SDK never returns more than sdk.batch.size records at a time, even
// if the connector's ReadN ignores its n argument; the excess is carried over
// to the next read. See [CodeBatchUnboundedWait], [CodeBatchSizeDefaulted] and
// [CodeBatchDrainSemanticsPending] for the configurations the SDK warns about.
//
// A connector that must keep different defaults (for example because it
// batches internally) pins them by setting non-nil values in its NewSource.
type SourceWithBatch struct {
	// Maximum size of batch before it gets read from the source.
	BatchSize *int `json:"sdk.batch.size" default:"100" validate:"gt=-1"`
	// Maximum delay before an incomplete batch is read from the source. 0
	// returns what is already queued without waiting.
	BatchDelay *time.Duration `json:"sdk.batch.delay" default:"0"`

	// resolved is set by Configure (parseConfig) or, for configs that did not
	// go through it, by Open.
	resolved *batchSettings
}

// batchSettings are the effective settings after normalization.
type batchSettings struct {
	size  int
	delay time.Duration
	mode  batchMode
}

// batchWarning is a configuration condition the SDK logs with a stable code.
type batchWarning struct {
	code string
	path string
	msg  string
	fix  string
}

// batchInputs records, before defaults are applied, which batch parameters the
// user actually set. A value equal to the default still counts as set:
// "sdk.batch.size: 100" is an explicit choice.
type batchInputs struct {
	sizeSet  bool
	delaySet bool
	// defaultDelay is the delay a pipeline that does not set one gets.
	defaultDelay time.Duration
}

// Wrap a Source into the middleware.
func (c *SourceWithBatch) Wrap(impl Source) Source {
	return &sourceWithBatch{
		Source: impl,
		config: c,
	}
}

// resolveBatch normalizes a batch configuration. size and delay are the values
// after defaults were applied. It is pure so every rule can be tested without
// a connector.
func resolveBatch(size int, delay time.Duration, in batchInputs) (batchSettings, []batchWarning) {
	size = max(size, 0)
	delay = max(delay, 0)
	windowDefault := in.defaultDelay > 0

	switch {
	case size == 0 && delay == 0:
		// Opt-out (or a connector that pins 0/0): today's passthrough.
		return batchSettings{mode: batchModePassthrough}, nil

	case size == 0:
		// Delay only. ReadN(ctx, 0) breaks connectors that validate n, and
		// connectors that accept it never batch, so bound it.
		return batchSettings{size: defaultMaxBatchSize, delay: delay, mode: batchModeWindow}, []batchWarning{{
			code: CodeBatchSizeDefaulted,
			path: batchSizeParam,
			msg: fmt.Sprintf("%s is set but %s is 0, using a batch size of %d",
				batchDelayParam, batchSizeParam, defaultMaxBatchSize),
			fix: "set " + batchSizeParam + " explicitly",
		}}

	case delay > 0:
		return batchSettings{size: size, delay: delay, mode: batchModeWindow}, nil

	case !in.sizeSet && windowDefault:
		// The default is a window, and the user turned the window off by
		// setting only delay 0. They would inherit a size with no deadline,
		// a stall the upgrade itself would create, so keep them on passthrough.
		return batchSettings{mode: batchModePassthrough}, []batchWarning{{
			code: CodeBatchUnboundedWait,
			path: batchDelayParam,
			msg: fmt.Sprintf("%s is 0 and %s is not set, which would wait for a full batch with no deadline; batching is off",
				batchDelayParam, batchSizeParam),
			fix: "set " + batchDelayParam + " above 0, or set " + batchSizeParam + " explicitly",
		}}

	case !in.sizeSet:
		// Defaults only (or a defaulted size with delay 0): adaptive drain.
		return batchSettings{size: size, mode: batchModeAdaptive}, nil

	case windowDefault:
		// Explicit size with an explicit delay of 0 while the default is a
		// window: wait-until-full with no deadline.
		return batchSettings{size: size, mode: batchModeUntilFull}, []batchWarning{{
			code: CodeBatchUnboundedWait,
			path: batchDelayParam,
			msg: fmt.Sprintf("%s is set and %s is 0, so a batch waits until it is full with no deadline",
				batchSizeParam, batchDelayParam),
			fix: "set " + batchDelayParam + " above 0",
		}}

	default:
		// An explicit size with delay 0 keeps its pre-existing meaning of
		// waiting for a full batch, and is announced as changing. Rollout:
		//   v0.18.0 (N): wait until full, warn (this case).
		//   N+1:         the same, plus a notice in the connector docs.
		//   N+2:         this case becomes the adaptive drain.
		return batchSettings{size: size, mode: batchModeUntilFull}, []batchWarning{{
			code: CodeBatchDrainSemanticsPending,
			path: batchDelayParam,
			msg: fmt.Sprintf("%s is set and %s is 0: a batch currently waits until it is full, and will return what is already queued in a later release",
				batchSizeParam, batchDelayParam),
			fix: "set " + batchDelayParam + " above 0 to keep full batches",
		}}
	}
}

func logBatchWarnings(ctx context.Context, ws []batchWarning) {
	for _, w := range ws {
		Logger(ctx).Warn().
			Str("code", w.code).
			Str("config_path", w.path).
			Str("fix", w.fix).
			Msg(w.msg)
	}
}

func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

func derefDuration(p *time.Duration) time.Duration {
	if p == nil {
		return 0
	}
	return *p
}

// captureBatchInputs must be called on the sanitized config before defaults
// are applied: afterwards a user-set value and a defaulted one look the same.
func captureBatchInputs(cfg config.Config, params config.Parameters) batchInputs {
	in := batchInputs{
		sizeSet:  strings.TrimSpace(cfg[batchSizeParam]) != "",
		delaySet: strings.TrimSpace(cfg[batchDelayParam]) != "",
	}

	// The default delay comes from the connector spec. Specs generated before
	// this version have no entry or default 0. A spec with a non-zero default is
	// the fixed-window variant.
	if d, err := time.ParseDuration(params[batchDelayParam].Default); err == nil {
		in.defaultDelay = d
	}

	return in
}

// resolve is called by Configure after the config was decoded into c.
func (c *SourceWithBatch) resolve(ctx context.Context, in batchInputs) {
	st, ws := resolveBatch(derefInt(c.BatchSize), derefDuration(c.BatchDelay), in)
	logBatchWarnings(ctx, ws)
	c.resolved = &st
}

// settings returns the resolved settings. A config that did not go through
// Configure (built by hand) has its values treated as explicit.
func (c *SourceWithBatch) settings(ctx context.Context) batchSettings {
	if c.resolved == nil {
		c.resolve(ctx, batchInputs{sizeSet: true, delaySet: true})
	}
	return *c.resolved
}

// findSourceWithBatch returns the batch middleware config embedded in a
// connector config, or nil.
func findSourceWithBatch(target any) *SourceWithBatch {
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct {
		return nil
	}
	for _, mw := range sourceMiddlewareFromConfigRecursive(v.Elem()) {
		if b, ok := mw.(*SourceWithBatch); ok {
			return b
		}
	}
	return nil
}

// -- middleware ---------------------------------------------------------------

type sourceWithBatch struct {
	Source
	config *SourceWithBatch

	readCh  chan readResponse
	readNCh chan readNResponse
	stop    chan struct{}
	// stopOnce closes stop exactly once, from Teardown.
	stopOnce sync.Once

	collectFn func(context.Context, int) ([]opencdc.Record, error)

	// The fields below are used only by the goroutine that calls ReadN
	// (the adapter's runRead), which calls it sequentially.
	size  int
	delay time.Duration
	mode  batchMode
	// useRead is set once the connector reported ErrUnimplemented for ReadN.
	useRead bool
	// carry holds records the connector returned beyond the batch size. It is
	// only non-empty right after a full batch was returned.
	carry []opencdc.Record
	// err is a connector error that was received after records were already
	// collected. It is returned once the carry is empty.
	err error
}

type readNResponse struct {
	Records []opencdc.Record
	Err     error
}

type readResponse struct {
	Record opencdc.Record
	Err    error
}

func (s *sourceWithBatch) Open(ctx context.Context, pos opencdc.Position) error {
	err := s.Source.Open(ctx, pos)
	if err != nil {
		return err
	}

	st := s.config.settings(ctx)
	s.size, s.delay, s.mode = st.size, st.delay, st.mode

	s.stop = make(chan struct{})
	s.readCh = make(chan readResponse, s.size)
	s.readNCh = make(chan readNResponse, 1)

	Logger(ctx).Info().
		Int("size", s.size).
		Dur("delay", s.delay).
		Str("mode", s.mode.String()).
		Str("read_path", "ReadN").
		Msg("source batching configured")

	if s.mode != batchModePassthrough {
		s.collectFn = s.collect
		go s.runReadN(ctx)
	} else {
		// Batching off, simply forward the call.
		s.collectFn = s.Source.ReadN
	}

	return nil
}

// Teardown stops the reader goroutines, then tears the connector down. Closing
// stop means they exit even if the Open context is still alive. It is safe to
// call more than once.
func (s *sourceWithBatch) Teardown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		if s.stop != nil {
			close(s.stop)
		}
	})
	return s.Source.Teardown(ctx)
}

func (s *sourceWithBatch) runReadN(ctx context.Context) {
	defer close(s.readNCh)

	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second * 5,
	}

	for {
		recs, err := s.Source.ReadN(ctx, s.size)
		if err != nil {
			switch {
			case errors.Is(err, ErrBackoffRetry):
				// the plugin wants us to retry reading later
				_, _, err := cchan.ChanOut[time.Time](time.After(b.Duration())).Recv(ctx)
				if err != nil {
					// The plugin is using the SDK for long-polling
					// and relying on the SDK to check for a cancelled context.
					return
				}
				continue

			case errors.Is(err, ErrUnimplemented):
				Logger(ctx).Info().
					Int("size", s.size).
					Dur("delay", s.delay).
					Str("mode", s.mode.String()).
					Str("read_path", "Read").
					Msg("source does not support batch reads, falling back to single reads")

				go s.runRead(ctx)
				s.sendReadN(ctx, readNResponse{Err: err})
				return

			default:
				s.sendReadN(ctx, readNResponse{Err: err})
				return
			}
		}

		// Ownership: the slice the connector returned is the connector's again
		// as soon as the next ReadN call starts, and this loop makes that call
		// right after handing the batch over, while the consumer is still
		// reading and sending it. A connector that reuses its returned slice
		// (buf = buf[:0]; append(...)) would overwrite the batch in flight and
		// records would be lost (invariant 3). Copy the slice before the next
		// call. This copies the Record values only, see [Source.ReadN] for what
		// the connector must not touch.
		recs = slices.Clone(recs)

		if !s.sendReadN(ctx, readNResponse{Records: recs}) {
			return
		}
	}
}

// sendReadN hands a response to the collector. It gives up when the source is
// stopping, so the goroutine cannot leak blocked on a channel nobody reads.
func (s *sourceWithBatch) sendReadN(ctx context.Context, resp readNResponse) bool {
	select {
	case s.readNCh <- resp:
		return true
	case <-ctx.Done():
		return false
	case <-s.stop:
		return false
	}
}

func (s *sourceWithBatch) runRead(ctx context.Context) {
	defer close(s.readCh)

	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second * 5,
	}

	for {
		rec, err := s.Source.Read(ctx)
		if err != nil {
			if errors.Is(err, ErrBackoffRetry) {
				// the plugin wants us to retry reading later
				_, _, err := cchan.ChanOut[time.Time](time.After(b.Duration())).Recv(ctx)
				if err != nil {
					// The plugin is using the SDK for long-polling
					// and relying on the SDK to check for a cancelled context.
					return
				}
				continue
			}

			// Not a bare send: with a full channel and no collector this would
			// block forever.
			select {
			case s.readCh <- readResponse{Err: err}:
			case <-ctx.Done():
			case <-s.stop:
			}
			return
		}

		select {
		case s.readCh <- readResponse{Record: rec}:
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		}
	}
}

// Read returns one record. The SDK itself only calls ReadN, but Read is part of
// the Source interface and is called directly by tests and acceptance
// harnesses. With batching on, a background goroutine owns the connector's
// read methods, so forwarding the call would run two readers against the
// connector at once and split the stream between them. Take the record from
// the collector instead and keep the rest of its batch for the next call.
func (s *sourceWithBatch) Read(ctx context.Context) (opencdc.Record, error) {
	if s.mode == batchModePassthrough || s.collectFn == nil {
		return s.Source.Read(ctx)
	}
	if len(s.carry) == 0 {
		batch, err := s.collect(ctx, 1)
		if err != nil {
			return opencdc.Record{}, err
		}
		s.carry = batch
	}
	rec := s.carry[0]
	s.carry = s.carry[1:]
	if len(s.carry) == 0 {
		s.carry = nil
	}
	return rec, nil
}

func (s *sourceWithBatch) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	return s.collectFn(ctx, n)
}

type feedKind int

const (
	feedRecords feedKind = iota // recs and/or err are set
	feedEmpty                   // non-blocking receive found nothing queued
	feedTimer                   // the window expired
	feedClosed                  // the reader goroutine exited
	feedCtx                     // the context was cancelled
	feedSwitch                  // ReadN is unimplemented, now using Read
)

type feedItem struct {
	recs []opencdc.Record
	err  error
	kind feedKind
}

// recv takes the next item from whichever reader goroutine is active. With
// block false it returns feedEmpty instead of waiting.
func (s *sourceWithBatch) recv(ctx context.Context, timer <-chan time.Time, block bool) feedItem {
	if s.useRead {
		if !block {
			select {
			case resp, ok := <-s.readCh:
				return fromRead(resp, ok)
			default:
				return feedItem{kind: feedEmpty}
			}
		}
		select {
		case resp, ok := <-s.readCh:
			return fromRead(resp, ok)
		case <-timer:
			return feedItem{kind: feedTimer}
		case <-ctx.Done():
			return feedItem{kind: feedCtx}
		}
	}

	if !block {
		select {
		case resp, ok := <-s.readNCh:
			return s.fromReadN(resp, ok)
		default:
			return feedItem{kind: feedEmpty}
		}
	}
	select {
	case resp, ok := <-s.readNCh:
		return s.fromReadN(resp, ok)
	case <-timer:
		return feedItem{kind: feedTimer}
	case <-ctx.Done():
		return feedItem{kind: feedCtx}
	}
}

func fromRead(resp readResponse, ok bool) feedItem {
	switch {
	case !ok:
		return feedItem{kind: feedClosed}
	case resp.Err != nil:
		return feedItem{err: resp.Err}
	default:
		return feedItem{recs: []opencdc.Record{resp.Record}}
	}
}

func (s *sourceWithBatch) fromReadN(resp readNResponse, ok bool) feedItem {
	switch {
	case !ok:
		return feedItem{kind: feedClosed}
	case errors.Is(resp.Err, ErrUnimplemented):
		// the plugin doesn't support batch reads, fall back to single reads
		s.useRead = true
		return feedItem{kind: feedSwitch}
	case resp.Err != nil:
		return feedItem{err: resp.Err}
	default:
		return feedItem{recs: resp.Records}
	}
}

// takeCarry moves up to s.size carried records into batch.
func (s *sourceWithBatch) takeCarry(batch []opencdc.Record) []opencdc.Record {
	if len(s.carry) == 0 {
		return batch
	}
	k := min(len(s.carry), s.size-len(batch))
	batch = append(batch, s.carry[:k]...)
	s.carry = s.carry[k:]
	if len(s.carry) == 0 {
		s.carry = nil // release the backing array
	}
	return batch
}

// collect returns the next batch: at least one record, at most s.size records,
// or an error. It never returns an empty batch together with a nil error,
// because the adapter treats that as "no records yet" and calls again, which
// against a closed channel would be a busy loop.
//
// Ordering when a connector error arrives (flush then fail): records already
// collected are returned first, then any carry, then the error. A record that
// the connector delivered before failing is therefore delivered, as it would
// be without batching.
//
// Cancellation is different: cancelling ctx (Stop, Teardown) drops the partial
// batch and the carry. They were never sent, so never acked, and replay from
// the last persisted position (invariants 1 and 3).
func (s *sourceWithBatch) collect(ctx context.Context, _ int) ([]opencdc.Record, error) {
	if len(s.carry) == 0 && s.err != nil {
		return nil, s.err
	}

	batch := make([]opencdc.Record, 0, s.size)
	batch = s.takeCarry(batch)
	if len(batch) >= s.size || (len(batch) > 0 && s.err != nil) {
		return batch, nil
	}

	var (
		timer  *time.Timer
		timerC <-chan time.Time
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	if len(batch) > 0 && s.mode == batchModeWindow {
		// Records carried over count as the first records of the window.
		timer = time.NewTimer(s.delay)
		timerC = timer.C
	}

	for {
		if err := ctx.Err(); err != nil {
			// Invariant 3: nothing collected here was sent, so dropping it
			// loses nothing; it replays.
			s.carry = nil
			return nil, err
		}

		// In adaptive mode only the first record is waited for.
		block := s.mode != batchModeAdaptive || len(batch) == 0
		it := s.recv(ctx, timerC, block)

		switch it.kind {
		case feedCtx:
			s.carry = nil
			return nil, ctx.Err()

		case feedTimer, feedEmpty:
			if len(batch) == 0 {
				continue // cannot happen, the timer starts with the first record
			}
			return batch, nil

		case feedClosed:
			// The reader goroutine exited without a connector error: its
			// context was cancelled. Never return an empty batch with a nil
			// error here.
			s.carry = nil
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("source reader stopped: %w", context.Canceled)

		case feedSwitch:
			continue

		case feedRecords:
			var done bool
			var err error
			batch, done, err = s.absorb(ctx, batch, it)
			if done || err != nil {
				return batch, err
			}
			if s.mode == batchModeWindow && timer == nil && len(batch) > 0 {
				// start the delay timer after we have received the first records
				timer = time.NewTimer(s.delay)
				timerC = timer.C
			}
		}
	}
}

// absorb adds one received item to batch. done reports that batch must be
// returned now (with err, if any).
func (s *sourceWithBatch) absorb(ctx context.Context, batch []opencdc.Record, it feedItem) (out []opencdc.Record, done bool, err error) {
	if it.err != nil {
		s.err = it.err
		if len(batch) == 0 || ctx.Err() != nil || errors.Is(it.err, context.Canceled) {
			// Nothing to flush, or a cancellation, which drops the batch.
			return nil, true, it.err
		}
		return batch, true, nil
	}

	room := s.size - len(batch)
	if len(it.recs) > room {
		// Hard cap: a connector that ignores n must not make the batch
		// larger than the configured size. Keep the excess, in order.
		batch = append(batch, it.recs[:room]...)
		s.carry = it.recs[room:]
		return batch, true, nil
	}
	batch = append(batch, it.recs...)
	// An empty ReadN result leaves the batch as it was; keep waiting.
	return batch, len(batch) >= s.size, nil
}
