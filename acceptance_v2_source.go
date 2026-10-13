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

// requireRead reads len(want) records from s, then keeps reading until the
// source has been quiet for ReadTimeout, so that an extra record (a repeat of an
// earlier one, a record nobody wrote) is seen instead of taking the place of a
// missing one. It fails if any of want is missing or if records were delivered
// that were never written. A record delivered twice is at-least-once and is
// only logged; SRC-02 is the case that requires exactly one delivery, see
// requireReadOnce. The failure message reports missing and unexpected records
// separately, because they point at different bugs.
func (a *acceptanceV2) requireRead(t *testing.T, s *v2Src, want []opencdc.Record) []opencdc.Record {
	t.Helper()
	return a.readAndCheck(t, s, want, false)
}

// requireReadOnce is requireRead that also fails on a record delivered twice.
func (a *acceptanceV2) requireReadOnce(t *testing.T, s *v2Src, want []opencdc.Record) []opencdc.Record {
	t.Helper()
	return a.readAndCheck(t, s, want, true)
}

func (a *acceptanceV2) readAndCheck(t *testing.T, s *v2Src, want []opencdc.Record, once bool) []opencdc.Record {
	t.Helper()
	got := s.next(len(want), a.readTimeoutFor(len(want)))
	got = append(got, s.drain(len(want), a.driver.ReadTimeout())...)
	// A repeat of a record this session delivered in an earlier read is a
	// duplicate, not a record nobody wrote.
	fresh := make([]opencdc.Record, 0, len(got))
	repeats := 0
	for _, r := range got {
		if _, ok := s.known[v2ID(r)]; ok {
			repeats++
			continue
		}
		fresh = append(fresh, r)
	}
	missing, dups, unexpected := v2Diff(want, fresh)
	dups += repeats
	for _, r := range fresh {
		s.known[v2ID(r)] = struct{}{}
	}
	if len(missing) > 0 || unexpected > 0 || (once && dups > 0) {
		first := ""
		if len(missing) > 0 {
			first = fmt.Sprintf(" (first missing: %q)", missing[0])
		}
		t.Fatalf("source returned %d records for %d written: %d missing, %d duplicated, %d unexpected%s", len(got), len(want), len(missing), dups, unexpected, first)
	}
	if dups > 0 {
		t.Logf("source delivered %d records twice (allowed, SRC-02 requires exactly one delivery)", dups)
	}
	return got
}

// requireReadAck is requireRead followed by acking what was read, which is what
// a running pipeline does. Some sources only continue from the snapshot into
// change capture once the snapshot records are acked, so tests that don't
// withhold acks on purpose must send them.
func (a *acceptanceV2) requireReadAck(t *testing.T, s *v2Src, want []opencdc.Record) []opencdc.Record {
	t.Helper()
	return a.ackAfter(t, s, a.requireRead(t, s, want))
}

// requireReadOnceAck is requireReadOnce followed by acking what was read.
func (a *acceptanceV2) requireReadOnceAck(t *testing.T, s *v2Src, want []opencdc.Record) []opencdc.Record {
	t.Helper()
	return a.ackAfter(t, s, a.requireReadOnce(t, s, want))
}

func (a *acceptanceV2) ackAfter(t *testing.T, s *v2Src, got []opencdc.Record) []opencdc.Record {
	t.Helper()
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

	checkPositions(a.requireReadOnceAck(t, s, first))
	for _, n := range []int{100, a.caps.maxBatchSize()} {
		want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationCreate, n))
		got := a.requireReadOnceAck(t, s, want)
		checkPositions(got)
		if _, dups, unexpected := v2Diff(want, got); dups > 0 || unexpected > 0 {
			t.Fatalf("batch of %d: %d duplicates and %d unexpected records", n, dups, unexpected)
		}
	}
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// resumeRun tracks what a source session acked, for SRC-03.
type resumeRun struct {
	acked    []opencdc.Record
	ackedSet map[string]struct{}
	resumeAt opencdc.Position
}

func (r *resumeRun) countAcked(recs []opencdc.Record) int {
	n := 0
	for _, rec := range recs {
		if _, ok := r.ackedSet[v2ID(rec)]; ok {
			n++
		}
	}
	return n
}

// fresh returns the records not acked before.
func (r *resumeRun) fresh(recs []opencdc.Record) []opencdc.Record {
	var out []opencdc.Record
	for _, rec := range recs {
		if _, ok := r.ackedSet[v2ID(rec)]; !ok {
			out = append(out, rec)
		}
	}
	return out
}

// ackFresh acks the first n records of got that were not acked before, waits
// until the connector processed the last ack, and moves the resume position
// there. Records delivered again are not acked again: that would move the
// resume position backwards and shrink the redelivery the case measures.
func (a *acceptanceV2) ackFresh(t *testing.T, s *v2Src, run *resumeRun, got []opencdc.Record, n int, label string) {
	t.Helper()
	fresh := run.fresh(got)
	if n > len(fresh) {
		n = len(fresh)
	}
	if n == 0 {
		return
	}
	if err := s.ack(fresh[:n]...); err != nil {
		t.Fatalf("%s: ack failed: %v", label, err)
	}
	last := fresh[n-1].Position
	// the kill must land after the connector processed the ack, not while it
	// is still in the stream
	if err := s.waitAckProcessed(last, a.readTimeoutFor(0)); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	for _, r := range fresh[:n] {
		run.ackedSet[v2ID(r)] = struct{}{}
		run.acked = append(run.acked, r)
	}
	run.resumeAt = last
}

// SRC-03: the source is killed several times, first while it is reading the
// snapshot, then while it is capturing changes, with some records acked and
// some only delivered. After each kill it resumes from the last acked position,
// snapshot positions and CDC positions alike. Checked:
//
//   - no gap: every record is either acked in some run or delivered by the
//     final run. Records that were merely delivered before a kill do not count,
//     the host never handled them;
//   - bounded redelivery: a restart delivers at most MaxRedelivery records that
//     were already acked (none with ExactResume). A source that ignores the
//     position and starts over fails, snapshot or CDC;
//   - the kill is a kill: Teardown does not run, so a source that commits its
//     position only in Teardown is caught.
//
// The data set is scaled with the declared bound so that the records acked
// before the last restart are at least five times the bound; bounds above
// MaxAcceptedRedelivery fail the case.
func (a *acceptanceV2) testSrcCrashResume(t *testing.T) {
	bound := a.caps.maxRedelivery()
	if !a.caps.Source.ExactResume && a.caps.Source.MaxRedelivery > MaxAcceptedRedelivery {
		t.Fatalf("MaxRedelivery %d is above the largest bound the suite accepts (%d): a bound that large says nothing about resume",
			a.caps.Source.MaxRedelivery, MaxAcceptedRedelivery)
	}
	k := 1
	if bound > DefaultMaxRedelivery {
		k = (bound + DefaultMaxRedelivery - 1) / DefaultMaxRedelivery
	}

	want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, 100*k))
	run := &resumeRun{ackedSet: map[string]struct{}{}}
	check := func(label string, got []opencdc.Record) {
		t.Helper()
		if redelivered := run.countAcked(got); redelivered > bound {
			t.Fatalf("%s: after resuming from the last acked position the source delivered %d records that were already acked (allowed: %d, ExactResume=%v); a source that restarts from the beginning does this",
				label, redelivered, bound, a.caps.Source.ExactResume)
		}
	}

	// snapshot: read, ack, kill
	for i, c := range [][2]int{{30, 25}, {30, 20}, {20, 5}} {
		label := fmt.Sprintf("snapshot cycle %d", i)
		s := a.openSrc(t, run.resumeAt)
		got := s.next(c[0]*k, a.readTimeoutFor(c[0]*k))
		check(label, got)
		a.reapCrashed()
		a.ackFresh(t, s, run, got, c[1]*k, label)
		t.Logf("%s: read %d, acked %d, killing source", label, len(got), c[1]*k)
		s.crash()
	}

	// finish the snapshot in one run, so that the source moves on to change
	// capture, then write changes and kill it there
	s := a.openSrc(t, run.resumeAt)
	a.reapCrashed()
	rest := s.drainAck(len(want)*2, a.driver.ReadTimeout())
	check("snapshot completion", rest)
	for _, r := range rest {
		if _, ok := run.ackedSet[v2ID(r)]; !ok {
			run.ackedSet[v2ID(r)] = struct{}{}
			run.acked = append(run.acked, r)
		}
	}
	if len(rest) > 0 {
		run.resumeAt = rest[len(rest)-1].Position
		if err := s.waitAckProcessed(run.resumeAt, a.readTimeoutFor(0)); err != nil {
			t.Fatalf("snapshot completion: %v", err)
		}
	}
	cdc1 := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationCreate, 40*k))
	want = append(want, cdc1...)
	got := s.next(40*k, a.readTimeoutFor(40*k))
	a.ackFresh(t, s, run, got, 25*k, "cdc cycle 0")
	t.Logf("cdc cycle 0: read %d changes, acked %d, killing source", len(got), 25*k)
	s.crash()

	// resume from a change capture position, write more changes after Open
	s = a.openSrc(t, run.resumeAt)
	cdc2 := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationCreate, 20*k))
	want = append(want, cdc2...)
	got = s.next(35*k, a.readTimeoutFor(35*k))
	got = append(got, s.drain(len(want), a.driver.ReadTimeout())...)
	check("cdc cycle 1", got)
	a.reapCrashed()
	a.ackFresh(t, s, run, got, 30*k, "cdc cycle 1")
	t.Logf("cdc cycle 1: read %d, acked %d, killing source", len(got), 30*k)
	s.crash()

	s = a.openSrc(t, run.resumeAt)
	final := s.drainAck(len(want)*2, a.driver.ReadTimeout())
	a.reapCrashed()
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
	check("final run", final)
	requireNoGap(t, want, append(append([]opencdc.Record{}, run.acked...), final...), "after repeated kill and resume")
}

// SRC-04: records of one key are delivered in the order they were written, also
// across a restart: half of them are acked and the source is killed, the rest
// arrives after resuming from the last acked position.
func (a *acceptanceV2) testSrcOrdering(t *testing.T) {
	want := a.driver.WriteToSource(t, a.keyedRecords(t, []string{"key-a", "key-b", "key-c"}, 30))

	run := &resumeRun{ackedSet: map[string]struct{}{}}
	s := a.openSrc(t, nil)
	first := s.next(45, a.readTimeoutFor(45))
	a.ackFresh(t, s, run, first, len(first), "ordering")
	s.crash()

	s = a.openSrc(t, run.resumeAt)
	rest := s.drainAck(len(want)*2, a.driver.ReadTimeout())
	a.reapCrashed()
	if err := s.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
	// everything that was acked, then what the restart delivered beyond it
	got := append(append([]opencdc.Record{}, run.acked...), run.fresh(rest)...)
	requireNoGap(t, want, got, "ordering test restart")
	requireKeyOrder(t, want, got, "source claims OrdersByKey (across a restart)")
}

// SRC-05: a field that appears while the source runs reaches the output
// intact.
func (a *acceptanceV2) testSrcSchemaEvolution(t *testing.T) {
	base, ok := a.structuredRecords(t, 10)
	if !ok {
		t.Fatal("the connector claims schema evolution, but the driver generates raw payloads and the case needs structured data (use GenerateStructuredData)")
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

	// Stop reports the last position the plugin sent into the stream. The host
	// side may not have received everything yet, so wait for that record, and
	// then require that nothing came after it.
	if err := s.waitDelivered(last, a.readTimeoutFor(0)); err != nil {
		t.Fatalf("Stop reported last position %q: %v; Conduit drains acks up to this position, a wrong value loses or hangs records", last, err)
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
