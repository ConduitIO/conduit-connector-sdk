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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
)

// The v2 harness drives a connector through pconnector.SourcePlugin and
// pconnector.DestinationPlugin, the same interfaces Conduit uses over gRPC.
// Nothing in the v2 tests depends on the Go SDK types, so the same scenarios
// can be pointed at a plugin client for a standalone binary (see the design
// document 20261013-acceptance-suite-v2.md). In-process, the plugins are
// built with NewSourcePlugin and NewDestinationPlugin.

// v2Stream is an in-memory stand-in for a gRPC bidirectional stream. Unlike the
// unbuffered stream the SDK unit tests use, it has configurable buffers: a
// real gRPC stream buffers in the transport, so a host can have several
// batches in flight before the plugin reads the first one.
type v2Stream[REQ, RES any] struct {
	ctx  context.Context //nolint:containedctx // mimics a gRPC stream
	req  chan REQ
	resp chan RES
	stop chan struct{}

	m      sync.Mutex
	reason error
}

func newV2Stream[REQ, RES any](ctx context.Context, reqBuf, respBuf int) *v2Stream[REQ, RES] {
	return &v2Stream[REQ, RES]{
		ctx:  ctx,
		req:  make(chan REQ, reqBuf),
		resp: make(chan RES, respBuf),
		stop: make(chan struct{}),
	}
}

// Close closes the stream. The host side sees reason (nil is io.EOF), the
// plugin side sees io.EOF.
func (s *v2Stream[REQ, RES]) Close(reason error) {
	s.m.Lock()
	defer s.m.Unlock()
	select {
	case <-s.stop:
	default:
		s.reason = reason
		close(s.stop)
	}
}

func (s *v2Stream[REQ, RES]) closeReason() error {
	s.m.Lock()
	defer s.m.Unlock()
	if s.reason == nil {
		return io.EOF
	}
	return s.reason
}

type v2StreamClient[REQ, RES any] v2Stream[REQ, RES]

func (s *v2StreamClient[REQ, RES]) Send(req REQ) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.stop:
		return io.EOF
	case s.req <- req:
		return nil
	}
}

func (s *v2StreamClient[REQ, RES]) Recv() (RES, error) {
	var zero RES
	select {
	case <-s.ctx.Done():
		return zero, s.ctx.Err()
	case r := <-s.resp:
		return r, nil
	case <-s.stop:
		// deliver what the plugin sent before the stream was closed
		select {
		case r := <-s.resp:
			return r, nil
		default:
		}
		return zero, (*v2Stream[REQ, RES])(s).closeReason()
	}
}

type v2StreamServer[REQ, RES any] v2Stream[REQ, RES]

func (s *v2StreamServer[REQ, RES]) Send(resp RES) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.stop:
		return io.EOF
	case s.resp <- resp:
		return nil
	}
}

func (s *v2StreamServer[REQ, RES]) Recv() (REQ, error) {
	var zero REQ
	select {
	case <-s.ctx.Done():
		return zero, s.ctx.Err()
	case r := <-s.req:
		return r, nil
	case <-s.stop:
		select {
		case r := <-s.req:
			return r, nil
		default:
		}
		return zero, io.EOF
	}
}

type v2DestStream struct {
	*v2Stream[pconnector.DestinationRunRequest, pconnector.DestinationRunResponse]
}

func (s v2DestStream) Client() pconnector.DestinationRunStreamClient {
	return (*v2StreamClient[pconnector.DestinationRunRequest, pconnector.DestinationRunResponse])(s.v2Stream)
}

func (s v2DestStream) Server() pconnector.DestinationRunStreamServer {
	return (*v2StreamServer[pconnector.DestinationRunRequest, pconnector.DestinationRunResponse])(s.v2Stream)
}

type v2SrcStream struct {
	*v2Stream[pconnector.SourceRunRequest, pconnector.SourceRunResponse]
}

func (s v2SrcStream) Client() pconnector.SourceRunStreamClient {
	return (*v2StreamClient[pconnector.SourceRunRequest, pconnector.SourceRunResponse])(s.v2Stream)
}

func (s v2SrcStream) Server() pconnector.SourceRunStreamServer {
	return (*v2StreamServer[pconnector.SourceRunRequest, pconnector.SourceRunResponse])(s.v2Stream)
}

const (
	v2StopTimeout = 30 * time.Second
	// abortTimeout bounds the wait for Run to return after the context of an
	// aborted plugin was cancelled.
	abortTimeout = 15 * time.Second
)

// -- Destination session -----------------------------------------------------

type v2DestOptions struct {
	// reqBuf is how many batches the host can have in flight before Send
	// blocks. Default is 1024, like a transport that buffers.
	reqBuf int
	// respBuf is how many ack messages the plugin can send before it blocks
	// waiting for the host. 0 means the plugin blocks on every ack until the
	// host receives it.
	respBuf int
	// ackDelay is slept by the host after receiving each ack message, which
	// simulates a host (or network) that acknowledges slowly.
	ackDelay time.Duration
	// extraConfig is merged over the driver's destination config.
	extraConfig config.Config
}

// v2Dest is the host side of a destination plugin under test.
type v2Dest struct {
	a      *acceptanceV2
	t      *testing.T
	plugin pconnector.DestinationPlugin
	stream v2DestStream
	cancel context.CancelFunc
	ctx    context.Context //nolint:containedctx // test helper

	runDone chan error
	recvEnd chan struct{}

	mu      sync.Mutex
	sent    []opencdc.Record
	acks    map[string]string // position -> ack error ("" is success)
	dupAcks int
	changed chan struct{} // closed and replaced on every change

	tornDown bool
}

func (a *acceptanceV2) openDest(t *testing.T, opts v2DestOptions) *v2Dest {
	t.Helper()
	if opts.reqBuf == 0 {
		opts.reqBuf = 1024
	}

	rootCtx := a.context(t)
	ctx, cancel := context.WithCancel(rootCtx)

	spec := a.driver.Connector().NewSpecification()
	plugin := NewDestinationPlugin(
		a.driver.Connector().NewDestination(),
		pconnector.PluginConfig{},
		spec.DestinationParams,
	)

	cfg := a.cloneConfig(a.driver.DestinationConfig(t))
	for k, v := range opts.extraConfig {
		cfg[k] = v
	}
	if _, err := plugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: cfg}); err != nil {
		cancel()
		t.Fatalf("destination Configure failed: %v", err)
	}
	if _, err := plugin.Open(ctx, pconnector.DestinationOpenRequest{}); err != nil {
		cancel()
		_, _ = plugin.Teardown(rootCtx, pconnector.DestinationTeardownRequest{})
		t.Fatalf("destination Open failed: %v", err)
	}

	d := &v2Dest{
		a:       a,
		t:       t,
		plugin:  plugin,
		stream:  v2DestStream{newV2Stream[pconnector.DestinationRunRequest, pconnector.DestinationRunResponse](ctx, opts.reqBuf, opts.respBuf)},
		cancel:  cancel,
		ctx:     rootCtx,
		runDone: make(chan error, 1),
		recvEnd: make(chan struct{}),
		acks:    make(map[string]string),
		changed: make(chan struct{}),
	}

	go func() { d.runDone <- plugin.Run(ctx, d.stream) }()
	go d.collectAcks(opts.ackDelay)
	t.Cleanup(func() { d.crash() }) // safety net, no-op if already torn down
	return d
}

func (d *v2Dest) collectAcks(delay time.Duration) {
	defer close(d.recvEnd)
	client := d.stream.Client()
	for {
		resp, err := client.Recv()
		if err != nil {
			return
		}
		d.mu.Lock()
		for _, ack := range resp.Acks {
			if _, ok := d.acks[string(ack.Position)]; ok {
				d.dupAcks++
			}
			d.acks[string(ack.Position)] = ack.Error
		}
		d.notifyLocked()
		d.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func (d *v2Dest) notifyLocked() {
	close(d.changed)
	d.changed = make(chan struct{})
}

// send hands a batch to the plugin without waiting for acks.
func (d *v2Dest) send(recs []opencdc.Record) error {
	d.mu.Lock()
	d.sent = append(d.sent, recs...)
	d.mu.Unlock()
	// A real host sends bytes over gRPC and never shares memory with the
	// plugin. In-process, the SDK middleware mutates the records it receives,
	// so hand over a copy and keep the originals for comparison.
	return d.stream.Client().Send(pconnector.DestinationRunRequest{Records: cloneRecords(recs)})
}

func cloneRecords(recs []opencdc.Record) []opencdc.Record {
	out := make([]opencdc.Record, len(recs))
	for i, r := range recs {
		out[i] = r.Clone()
	}
	return out
}

// waitAcks waits until every position in recs was acked, or the timeout hits.
func (d *v2Dest) waitAcks(recs []opencdc.Record, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		d.mu.Lock()
		pending := 0
		for _, r := range recs {
			if _, ok := d.acks[string(r.Position)]; !ok {
				pending++
			}
		}
		ch := d.changed
		d.mu.Unlock()
		if pending == 0 {
			return nil
		}
		select {
		case <-ch:
		case <-deadline:
			return fmt.Errorf("%d of %d records were not acked within %v", pending, len(recs), timeout)
		}
	}
}

// ackedRecords returns the sent records that were acked successfully, and the
// ones that were nacked.
func (d *v2Dest) ackedRecords() (ok, nacked []opencdc.Record) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range d.sent {
		e, acked := d.acks[string(r.Position)]
		switch {
		case !acked:
		case e == "":
			ok = append(ok, r)
		default:
			nacked = append(nacked, r)
		}
	}
	return ok, nacked
}

func (d *v2Dest) dupAckCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dupAcks
}

// stopGraceful does what Conduit does on a normal shutdown: Stop with the last
// position that was sent, then close the stream and tear down.
func (d *v2Dest) stopGraceful() error {
	d.mu.Lock()
	var last opencdc.Position
	if len(d.sent) > 0 {
		last = d.sent[len(d.sent)-1].Position
	}
	d.mu.Unlock()

	stopCtx, cancel := context.WithTimeout(d.ctx, v2StopTimeout)
	defer cancel()
	_, stopErr := d.plugin.Stop(stopCtx, pconnector.DestinationStopRequest{LastPosition: last})

	d.stream.Close(nil)
	runErr := d.waitRun(v2StopTimeout)
	tdErr := d.teardown()
	if stopErr != nil {
		return fmt.Errorf("stop: %w", stopErr)
	}
	if runErr != nil {
		return fmt.Errorf("run: %w", runErr)
	}
	return tdErr
}

func (d *v2Dest) waitRun(timeout time.Duration) error {
	select {
	case err := <-d.runDone:
		d.runDone <- err // keep for later waiters
		return err
	case <-time.After(timeout):
		return fmt.Errorf("Run did not return within %v", timeout)
	}
}

func (d *v2Dest) runEnded(timeout time.Duration) bool {
	select {
	case err := <-d.runDone:
		d.runDone <- err
		return true
	case <-time.After(timeout):
		return false
	}
}

func (d *v2Dest) teardown() error {
	d.mu.Lock()
	if d.tornDown {
		d.mu.Unlock()
		return nil
	}
	d.tornDown = true
	d.mu.Unlock()

	d.cancel()
	ctx, cancel := context.WithTimeout(d.ctx, v2StopTimeout)
	defer cancel()
	_, err := d.plugin.Teardown(ctx, pconnector.DestinationTeardownRequest{})
	<-d.recvEnd
	return err
}

// abort ends the destination abruptly as far as that is possible in-process:
// the context is cancelled, the stream is closed, nothing is flushed through
// Stop. It waits (bounded) for Run to return and reports whether it did. It
// does not call Teardown: a connector that flushes in Teardown would otherwise
// be rescued before the caller can check what is durable. Call teardown (or
// crash) when the check is done.
//
// A real kill -9 needs the standalone harness.
func (d *v2Dest) abort() (runEnded bool) {
	d.stream.Close(context.Canceled)
	d.cancel()
	return d.runEnded(abortTimeout)
}

// crash is abort followed by teardown, to release resources.
func (d *v2Dest) crash() {
	d.abort()
	_ = d.teardown()
}

// -- Source session ----------------------------------------------------------

// v2Src is the host side of a source plugin under test.
type v2Src struct {
	watcher *ackWatcher
	a       *acceptanceV2
	t       *testing.T
	plugin  pconnector.SourcePlugin
	stream  v2SrcStream
	cancel  context.CancelFunc
	ctx     context.Context //nolint:containedctx // test helper

	runDone chan error
	recvEnd chan struct{}

	mu      sync.Mutex
	got     []opencdc.Record
	taken   int
	known   map[string]struct{} // identities delivered in earlier requireRead calls
	changed chan struct{}

	tornDown bool
}

func (a *acceptanceV2) openSrc(t *testing.T, pos opencdc.Position) *v2Src {
	t.Helper()

	rootCtx := a.context(t)
	ctx, cancel := context.WithCancel(rootCtx)

	spec := a.driver.Connector().NewSpecification()
	watcher := &ackWatcher{Source: a.driver.Connector().NewSource(), done: map[string]struct{}{}, changed: make(chan struct{})}
	plugin := NewSourcePlugin(
		watcher,
		pconnector.PluginConfig{},
		spec.SourceParams,
	)

	cfg := a.cloneConfig(a.driver.SourceConfig(t))
	if _, err := plugin.Configure(ctx, pconnector.SourceConfigureRequest{Config: cfg}); err != nil {
		cancel()
		t.Fatalf("source Configure failed: %v", err)
	}
	if _, err := plugin.Open(ctx, pconnector.SourceOpenRequest{Position: pos}); err != nil {
		cancel()
		_, _ = plugin.Teardown(rootCtx, pconnector.SourceTeardownRequest{})
		t.Fatalf("source Open failed: %v", err)
	}

	s := &v2Src{
		watcher: watcher,
		a:       a,
		t:       t,
		plugin:  plugin,
		stream:  v2SrcStream{newV2Stream[pconnector.SourceRunRequest, pconnector.SourceRunResponse](ctx, 1024, 1024)},
		cancel:  cancel,
		ctx:     rootCtx,
		runDone: make(chan error, 1),
		recvEnd: make(chan struct{}),
		changed: make(chan struct{}),
		known:   map[string]struct{}{},
	}
	go func() { s.runDone <- plugin.Run(ctx, s.stream) }()
	go s.collect()
	t.Cleanup(func() {
		s.crash() // no-op if already ended
		_ = s.teardown()
	})
	return s
}

func (s *v2Src) collect() {
	defer close(s.recvEnd)
	client := s.stream.Client()
	for {
		resp, err := client.Recv()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.got = append(s.got, cloneRecords(resp.Records)...)
		close(s.changed)
		s.changed = make(chan struct{})
		s.mu.Unlock()
	}
}

// next returns up to n records that were not returned before, waiting until n
// are available or the timeout elapses. It returns what it has at timeout.
func (s *v2Src) next(n int, timeout time.Duration) []opencdc.Record {
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		avail := len(s.got) - s.taken
		ch := s.changed
		if avail >= n {
			out := append([]opencdc.Record(nil), s.got[s.taken:s.taken+n]...)
			s.taken += n
			s.mu.Unlock()
			return out
		}
		s.mu.Unlock()
		select {
		case <-ch:
		case <-deadline:
			s.mu.Lock()
			out := append([]opencdc.Record(nil), s.got[s.taken:]...)
			s.taken = len(s.got)
			s.mu.Unlock()
			return out
		}
	}
}

// drain returns everything the source delivers until it was quiet for the
// quiet duration, or limit records were collected.
func (s *v2Src) drain(limit int, quiet time.Duration) []opencdc.Record {
	var out []opencdc.Record
	for len(out) < limit {
		recs := s.next(1, quiet)
		if len(recs) == 0 {
			break
		}
		out = append(out, recs...)
	}
	return out
}

// drainAck is drain, but acks every record as it arrives, like a pipeline.
func (s *v2Src) drainAck(limit int, quiet time.Duration) []opencdc.Record {
	var out []opencdc.Record
	for len(out) < limit {
		recs := s.next(1, quiet)
		if len(recs) == 0 {
			break
		}
		if err := s.ack(recs...); err != nil {
			s.t.Errorf("ack failed: %v", err)
			break
		}
		out = append(out, recs...)
	}
	return out
}

// ack acknowledges the positions of recs in order.
func (s *v2Src) ack(recs ...opencdc.Record) error {
	if len(recs) == 0 {
		return nil
	}
	pos := make([]opencdc.Position, len(recs))
	for i, r := range recs {
		pos[i] = r.Position
	}
	if err := s.stream.Client().Send(pconnector.SourceRunRequest{AckPositions: pos}); err != nil {
		// the stream is closed when Run ends, usually because the connector's
		// Ack failed; the error of Run is the one worth reporting
		if runErr := s.runError(); runErr != nil {
			return fmt.Errorf("%w (Run ended with: %w)", err, runErr)
		}
		return err
	}
	return nil
}

// stopGraceful stops the source like Conduit does and returns the last
// position the plugin reports it sent. The caller may still ack records that
// were delivered before the stop, so the stream stays open until closeStream.
func (s *v2Src) stop() (opencdc.Position, error) {
	ctx, cancel := context.WithTimeout(s.ctx, v2StopTimeout)
	defer cancel()
	resp, err := s.plugin.Stop(ctx, pconnector.SourceStopRequest{})
	return resp.LastPosition, err
}

// stopGraceful runs Stop, closes the stream and tears down.
func (s *v2Src) stopGraceful() error {
	_, stopErr := s.stop()
	s.stream.Close(nil)
	runErr := s.waitRun(v2StopTimeout)
	tdErr := s.teardown()
	if stopErr != nil {
		return fmt.Errorf("stop: %w", stopErr)
	}
	if runErr != nil {
		return fmt.Errorf("run: %w", runErr)
	}
	return tdErr
}

func (s *v2Src) waitRun(timeout time.Duration) error {
	select {
	case err := <-s.runDone:
		s.runDone <- err
		return err
	case <-time.After(timeout):
		return fmt.Errorf("Run did not return within %v", timeout)
	}
}

func (s *v2Src) teardown() error {
	s.mu.Lock()
	if s.tornDown {
		s.mu.Unlock()
		return nil
	}
	s.tornDown = true
	s.mu.Unlock()

	// close the stream first, Teardown waits for Run, which ends when the
	// stream does
	s.stream.Close(nil)
	ctx, cancel := context.WithTimeout(s.ctx, v2StopTimeout)
	defer cancel()
	_, err := s.plugin.Teardown(ctx, pconnector.SourceTeardownRequest{})
	s.cancel()
	<-s.recvEnd
	return err
}

// crash ends the source the way an aborted process would: the context is
// cancelled and the stream closed, there is no Stop, no further ack, and no
// Teardown.
//
// Teardown is deliberately not called. A source that commits its position only
// in Teardown (or flushes anything there) would otherwise be rescued by the
// harness and pass a restart check it would fail after a real kill. The
// resources of a crashed instance are released by reapCrashed, or at the end of
// the test by the cleanup registered in openSrc. It reports an error if Run does
// not return.
func (s *v2Src) crash() {
	s.stream.Close(context.Canceled)
	s.cancel()
	if !s.runEnded(abortTimeout) {
		s.t.Errorf("source Run did not return within %v after the context was cancelled", abortTimeout)
	} else if err := s.runError(); err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		// Run ended for a reason of its own before or while it was cancelled
		s.t.Errorf("source Run failed: %v", err)
	}
	s.a.mu.Lock()
	s.a.crashed = append(s.a.crashed, s)
	s.a.mu.Unlock()
	if h, ok := s.a.driver.(AcceptanceProcessDeathDriver); ok {
		id := h.KilledSessionID(s.t)
		if err := h.ReleaseSession(context.Background(), id); err != nil {
			s.t.Errorf("releasing session %q after the kill: %v", id, err)
		}
		s.a.report.mu.Lock()
		s.a.report.ProcessDeathHookCalls++
		s.a.report.mu.Unlock()
	}
}

// reapCrashed tears down the sources that were killed. A killed process loses
// its connections when the operating system reaps it, which in-process only
// happens at Teardown. It is called once the instance that replaced them has
// opened and read, so that Teardown cannot rescue the check that the
// replacement starts from what was acked, but before the replacement is
// stopped, so that the dead instances no longer hold the system's resources
// (a replication slot, a lock) the way a live one would.
func (a *acceptanceV2) reapCrashed() {
	a.mu.Lock()
	crashed := a.crashed
	a.crashed = nil
	a.mu.Unlock()
	for _, s := range crashed {
		_ = s.teardown()
	}
}

// runError returns the error Run returned, or nil if it has not returned.
func (s *v2Src) runError() error {
	select {
	case err := <-s.runDone:
		s.runDone <- err
		return err
	default:
		return nil
	}
}

func (s *v2Src) runEnded(timeout time.Duration) bool {
	select {
	case err := <-s.runDone:
		s.runDone <- err
		return true
	case <-time.After(timeout):
		return false
	}
}

// ackWatcher wraps the connector's Source to learn when an Ack call returned.
// The plugin adapter delivers acks asynchronously from the stream, so without
// this a test that wants "the connector has processed my acks" can only sleep.
type ackWatcher struct {
	Source

	mu      sync.Mutex
	done    map[string]struct{}
	changed chan struct{}
}

func (w *ackWatcher) Ack(ctx context.Context, pos opencdc.Position) error {
	err := w.Source.Ack(ctx, pos)
	w.mu.Lock()
	w.done[string(pos)] = struct{}{}
	close(w.changed)
	w.changed = make(chan struct{})
	w.mu.Unlock()
	return err
}

// waitAckProcessed waits until Ack returned for pos.
func (s *v2Src) waitAckProcessed(pos opencdc.Position, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		s.watcher.mu.Lock()
		_, ok := s.watcher.done[string(pos)]
		ch := s.watcher.changed
		s.watcher.mu.Unlock()
		if ok {
			return nil
		}
		if runErr := s.runError(); runErr != nil {
			return fmt.Errorf("Run ended before the connector finished Ack for position %q: %w", pos, runErr)
		}
		select {
		case <-ch:
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			return fmt.Errorf("the connector did not finish Ack for position %q within %v", pos, timeout)
		}
	}
}

// waitDelivered waits until the record with position pos was received, and
// fails if records were received after it (the position Stop reported was not
// the last one sent) or if it never arrives.
func (s *v2Src) waitDelivered(pos opencdc.Position, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		idx := -1
		for i := len(s.got) - 1; i >= 0; i-- {
			if string(s.got[i].Position) == string(pos) {
				idx = i
				break
			}
		}
		total := len(s.got)
		ch := s.changed
		s.mu.Unlock()
		switch {
		case idx >= 0 && idx == total-1:
			return nil
		case idx >= 0:
			return fmt.Errorf("%d records were delivered after the record at that position", total-1-idx)
		}
		select {
		case <-ch:
		case <-deadline:
			return fmt.Errorf("no record with that position was delivered within %v", timeout)
		}
	}
}
