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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

// readTimeoutFor is how long to wait for n records from a source. Like
// ackTimeout it only guards against lost records.
func (a *acceptanceV2) readTimeoutFor(n int) time.Duration {
	d := 10 * a.driver.ReadTimeout()
	if d < 10*time.Second {
		d = 10 * time.Second
	}
	return d + time.Duration(n)*2*time.Millisecond
}

// requireRead reads exactly len(want) records from s (more than that is
// reported by the caller if it cares) and fails if any of want is missing.
func (a *acceptanceV2) requireRead(t *testing.T, s *v2Src, want []opencdc.Record) []opencdc.Record {
	t.Helper()
	got := s.next(len(want), a.readTimeoutFor(len(want)))
	missing, _, _ := v2Diff(want, got)
	if len(missing) > 0 {
		t.Fatalf("source returned %d of %d records, %d are missing (first: %q)", len(got), len(want), len(missing), missing[0])
	}
	return got
}

// requireReadAck is requireRead followed by acking what was read, which is what
// a running pipeline does. Some sources only continue from the snapshot into
// change capture once the snapshot records are acked, so tests that don't
// withhold acks on purpose must send them.
func (a *acceptanceV2) requireReadAck(t *testing.T, s *v2Src, want []opencdc.Record) []opencdc.Record {
	t.Helper()
	got := a.requireRead(t, s, want)
	if err := s.ack(got...); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	return got
}

// requireNoGap fails if a record of want is neither in acked nor in seen. This
// is at-least-once from the source side: after a restart from the last acked
// position, everything that was not acked must be delivered again.
func requireNoGap(t *testing.T, want, seen []opencdc.Record, what string) {
	t.Helper()
	missing, _, _ := v2Diff(want, seen)
	if len(missing) > 0 {
		t.Fatalf("%s: %d of %d records were never delivered (first: %q), that is a gap", what, len(missing), len(want), missing[0])
	}
}

// SRC-01: acks are sent from another goroutine, lagging behind reads, while
// the source keeps reading. Nothing may deadlock or be dropped, and the source
// has to cope with acks arriving long after the record was delivered.
func (a *acceptanceV2) testSrcLaggingAcks(t *testing.T) {
	want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, 200))

	s := a.openSrc(t, nil)

	const lag = 50
	ackCh := make(chan opencdc.Record, len(want))
	var wg sync.WaitGroup
	var ackErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		var pending []opencdc.Record
		for r := range ackCh {
			pending = append(pending, r)
			if len(pending) > lag {
				if err := s.ack(pending[0]); err != nil && ackErr == nil {
					ackErr = err
				}
				pending = pending[1:]
				time.Sleep(time.Millisecond)
			}
		}
		if err := s.ack(pending...); err != nil && ackErr == nil {
			ackErr = err
		}
	}()

	var got []opencdc.Record
	for len(got) < len(want) {
		recs := s.next(1, a.readTimeoutFor(len(want)))
		if len(recs) == 0 {
			break
		}
		got = append(got, recs...)
		ackCh <- recs[0]
	}
	close(ackCh)
	wg.Wait()
	if ackErr != nil {
		t.Fatalf("ack failed: %v", ackErr)
	}
	requireNoGap(t, want, got, "reading with lagging acks")

	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}

	// Everything is acked. A restart from the last position must work and may
	// not deliver anything the source had not delivered before.
	last := got[len(got)-1].Position
	s2 := a.openSrc(t, last)
	again := s2.drain(len(want), a.driver.ReadTimeout())
	if len(again) > 0 {
		if _, _, unexpected := v2Diff(want, again); unexpected > 0 {
			t.Fatalf("resuming from the last acked position returned %d records that were never written", unexpected)
		}
	}
	if err := s2.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// SRC-02: 1, 100 and MaxBatchSize records are written to the system and
// read by one running source. The first write happens before the source opens
// (snapshot), the others while it runs.
func (a *acceptanceV2) testSrcBatchSizes(t *testing.T) {
	first := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, 1))
	s := a.openSrc(t, nil)

	positions := map[string]struct{}{}
	checkPositions := func(recs []opencdc.Record) {
		for _, r := range recs {
			if _, dup := positions[string(r.Position)]; dup {
				t.Fatalf("position %q was delivered for two records; positions identify records for acks and resume", r.Position)
			}
			positions[string(r.Position)] = struct{}{}
		}
	}

	checkPositions(a.requireReadAck(t, s, first))
	for _, n := range []int{100, a.caps.maxBatchSize()} {
		want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationCreate, n))
		got := a.requireReadAck(t, s, want)
		checkPositions(got)
		if _, dups, unexpected := v2Diff(want, got); dups > 0 || unexpected > 0 {
			t.Fatalf("batch of %d: %d duplicates and %d unexpected records", n, dups, unexpected)
		}
	}
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// SRC-03: the source is killed several times at different points, with some
// records acked and some only delivered. After each kill it resumes from the
// last acked position. Across all runs every record must be delivered at least
// once (no gap). Redelivery is allowed (at-least-once).
func (a *acceptanceV2) testSrcCrashResume(t *testing.T) {
	want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, 40))

	var (
		seen     []opencdc.Record
		resumeAt opencdc.Position
	)
	// read, ack, kill: (read this many, ack this many)
	cycles := [][2]int{{10, 4}, {7, 7}, {15, 1}, {3, 0}}
	for i, c := range cycles {
		s := a.openSrc(t, resumeAt)
		if i == len(cycles)-1 {
			// the last cycle also sees records written while the source runs
			cdc := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationCreate, 20))
			want = append(want, cdc...)
		}
		got := s.next(c[0], a.readTimeoutFor(c[0]))
		seen = append(seen, got...)
		if c[1] > 0 && len(got) >= c[1] {
			if err := s.ack(got[:c[1]]...); err != nil {
				t.Fatalf("cycle %d: ack failed: %v", i, err)
			}
			resumeAt = got[c[1]-1].Position
			// give the connector the chance to process the ack, the kill is
			// meant to land after it
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("cycle %d: read %d, acked %d, killing source", i, len(got), c[1])
		s.crash()
	}

	s := a.openSrc(t, resumeAt)
	rest := s.drainAck(len(want)*2, a.driver.ReadTimeout())
	seen = append(seen, rest...)
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
	requireNoGap(t, want, seen, "after repeated kill and resume")
}

// SRC-04: records of one key are delivered in the order they were written.
func (a *acceptanceV2) testSrcOrdering(t *testing.T) {
	want := a.driver.WriteToSource(t, a.keyedRecords(t, []string{"key-a", "key-b", "key-c"}, 30))

	s := a.openSrc(t, nil)
	got := a.requireReadAck(t, s, want)
	requireKeyOrder(t, want, got, "source claims OrdersByKey")
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// SRC-05: a field that appears while the source runs reaches the output
// intact.
func (a *acceptanceV2) testSrcSchemaEvolution(t *testing.T) {
	base, ok := a.structuredRecords(t, 10)
	if !ok {
		t.Skip("driver generates raw payloads, schema evolution needs structured data (use GenerateStructuredData)")
	}
	before := a.driver.WriteToSource(t, base[:5])

	s := a.openSrc(t, nil)
	a.requireReadAck(t, s, before)

	after := a.driver.WriteToSource(t, withAddedField(base[5:]))
	got := a.requireReadAck(t, s, after)
	if missing, _, _ := v2Diff(after, got); len(missing) > 0 {
		t.Fatalf("records with the new field %q arrived altered", v2AddedField)
	}
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// SRC-06: a record of MaxRecordBytes survives intact.
func (a *acceptanceV2) testSrcLargeRecord(t *testing.T) {
	want := a.driver.WriteToSource(t, []opencdc.Record{a.largeRecord(t, a.caps.maxRecordBytes())})

	s := a.openSrc(t, nil)
	a.requireReadAck(t, s, want)
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// SRC-07: Stop is called with records delivered but not acked. Stop must
// return the position of the last record it delivered, the shutdown must
// complete, nothing may leak, and a resume from the last acked position must
// cover everything that was not acked (not checked for synthetic sources).
func (a *acceptanceV2) testSrcGracefulStop(t *testing.T) {
	defer a.verifyGoleaks(t)

	var want []opencdc.Record
	if !a.caps.Source.Synthetic {
		want = a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, 30))
	}

	s := a.openSrc(t, nil)
	var got []opencdc.Record
	if a.caps.Source.Synthetic {
		got = s.next(30, a.readTimeoutFor(30))
		if len(got) < 30 {
			t.Fatalf("source delivered only %d of the 30 records the test needs", len(got))
		}
	} else {
		got = a.requireRead(t, s, want)
	}
	if err := s.ack(got[:10]...); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	start := time.Now()
	last, err := s.stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	t.Logf("Stop returned after %v", time.Since(start).Round(time.Millisecond))

	// A source that keeps producing may have delivered more than the 30 the
	// test took; Stop reports the last one that was sent, so compare with
	// everything received.
	s.mu.Lock()
	lastDelivered := s.got[len(s.got)-1].Position
	s.mu.Unlock()
	if !bytes.Equal(last, lastDelivered) {
		t.Fatalf("Stop reported last position %q, the last delivered record has position %q; Conduit drains acks up to this position, a wrong value loses or hangs records", last, lastDelivered)
	}
	// Conduit still acks what it holds after Stop, this source gets none.
	s.stream.Close(nil)
	if err := s.waitRun(v2StopTimeout); err != nil {
		t.Fatalf("Run failed after stop: %v", err)
	}
	if err := s.teardown(); err != nil {
		t.Fatalf("Teardown failed: %v", err)
	}
	if a.caps.Source.Synthetic {
		return
	}

	s2 := a.openSrc(t, got[9].Position)
	rest := s2.drainAck(len(want), a.driver.ReadTimeout())
	if err := s2.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
	requireNoGap(t, want, append(got[:10:10], rest...), "resume after Stop with unacked records")
}

// SRC-08: Read returns when its deadline passes and when its context is
// cancelled, and the plugin stops when idle. A Read that ignores its context wedges every shutdown.
func (a *acceptanceV2) testSrcContextCancel(t *testing.T) {
	defer a.verifyGoleaks(t)

	ctx := a.context(t)
	source := a.driver.Connector().NewSource()
	if err := a.configureSource(ctx, t, source); err != nil {
		t.Fatalf("Configure failed: %v", err)
	}
	openCtx, cancelOpen := context.WithCancel(ctx)
	if err := source.Open(openCtx, nil); err != nil {
		cancelOpen()
		t.Fatalf("Open failed: %v", err)
	}
	// Teardown is bounded here: a source that wedges on shutdown is a finding,
	// not a reason to hang the run.
	var tornDown bool
	teardown := func() {
		if tornDown {
			return
		}
		tornDown = true
		cancelOpen()
		done := make(chan error, 1)
		go func() { done <- source.Teardown(ctx) }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Teardown failed: %v", err)
			}
		case <-time.After(v2StopTimeout):
			t.Errorf("Teardown did not return within %v", v2StopTimeout)
		}
	}
	defer teardown()

	// An idle Read with a deadline returns when the deadline passes.
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, a.driver.ReadTimeout())
	deadlineDone := make(chan error, 1)
	go func() {
		for {
			_, err := readOne(deadlineCtx, source)
			if errors.Is(err, ErrBackoffRetry) && deadlineCtx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			deadlineDone <- err // nil only if another test left records behind, which is fine
			return
		}
	}()
	select {
	case err := <-deadlineDone:
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrBackoffRetry) {
			deadlineCancel()
			t.Fatalf("idle Read returned %v at its deadline, expected context.DeadlineExceeded or ErrBackoffRetry", err)
		}
	case <-time.After(a.readTimeoutFor(0)):
		deadlineCancel()
		t.Fatalf("Read did not return within %v although its deadline (%v) passed", a.readTimeoutFor(0), a.driver.ReadTimeout())
	}
	deadlineCancel()

	readCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		// the source may deliver records another test left behind, keep
		// reading until it is idle
		for {
			_, err := readOne(readCtx, source)
			if err != nil {
				done <- err
				return
			}
		}
	}()
	time.Sleep(a.driver.ReadTimeout() / 2)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrBackoffRetry) {
			t.Fatalf("Read returned %v after its context was cancelled, expected context.Canceled or ErrBackoffRetry", err)
		}
	case <-time.After(a.readTimeoutFor(0)):
		t.Fatalf("Read did not return within %v after its context was cancelled", a.readTimeoutFor(0))
	}
	teardown()

	// The same through the plugin: an idle source stops and tears down.
	s := a.openSrc(t, nil)
	time.Sleep(a.driver.ReadTimeout() / 2)
	start := time.Now()
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("stopping an idle source failed: %v", err)
	}
	t.Logf("idle source stopped in %v", time.Since(start).Round(time.Millisecond))
}

// SRC-09: records/s for reading a pre-written set (for a synthetic source:
// the next 2000 records). Recorded, never asserted.
func (a *acceptanceV2) testSrcThroughput(t *testing.T) {
	const n = 2000
	var want []opencdc.Record
	if !a.caps.Source.Synthetic {
		want = a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, n))
	}

	s := a.openSrc(t, nil)
	start := time.Now()
	var got []opencdc.Record
	if a.caps.Source.Synthetic {
		got = s.next(n, a.readTimeoutFor(n))
		if len(got) < n {
			t.Fatalf("source delivered only %d of %d records", len(got), n)
		}
	} else {
		got = a.requireRead(t, s, want)
	}
	elapsed := time.Since(start)
	if err := s.ack(got...); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
	a.metric(t, "source_throughput", float64(len(got))/elapsed.Seconds(), "records/s",
		fmt.Sprintf("%d records, in-process plugin adapter", len(got)))
}
