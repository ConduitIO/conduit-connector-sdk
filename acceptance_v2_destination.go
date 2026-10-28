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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

// ackTimeout is how long the suite waits for n records to be acknowledged. It
// is deliberately generous: the suite looks for lost and wrong acks, not for
// slow systems (see the throughput check for that).
func (a *acceptanceV2) ackTimeout(n int) time.Duration {
	d := 10 * a.driver.WriteTimeout()
	if d < 10*time.Second {
		d = 10 * time.Second
	}
	return d + time.Duration(n)*2*time.Millisecond
}

// splitBatches splits recs into batches of the given size.
func splitBatches(recs []opencdc.Record, size int) [][]opencdc.Record {
	var out [][]opencdc.Record
	for len(recs) > 0 {
		n := size
		if n > len(recs) {
			n = len(recs)
		}
		out = append(out, recs[:n])
		recs = recs[n:]
	}
	return out
}

// canReadAll reports whether the driver can return duplicates: it implements
// AcceptanceReadAllDriver, or opted in through ReadAllFromSource.
func (a *acceptanceV2) canReadAll() bool {
	if _, ok := a.driver.(AcceptanceReadAllDriver); ok {
		return true
	}
	c, ok := a.driver.(interface{ readAllOptedIn() bool })
	return ok && c.readAllOptedIn()
}

// readAll returns everything stored in the destination. If the driver cannot
// return duplicates it falls back to ReadFromDestination with the expected
// records, which makes the caller's checks stricter.
func (a *acceptanceV2) readAll(t *testing.T, expected []opencdc.Record) []opencdc.Record {
	t.Helper()
	return a.decodeAll(t, a.readAllRaw(t, expected))
}

func (a *acceptanceV2) readAllRaw(t *testing.T, expected []opencdc.Record) []opencdc.Record {
	t.Helper()
	if p, ok := a.driver.(AcceptanceReadAllDriver); ok {
		return p.ReadAllFromDestination(t)
	}
	if p, ok := a.driver.(interface {
		ReadAllFromSource(*testing.T) ([]opencdc.Record, bool)
	}); ok {
		if recs, ok := p.ReadAllFromSource(t); ok {
			return recs
		}
	}
	return a.driver.ReadFromDestination(t, expected)
}

// decodeAll undoes source schema extraction on records read back through the
// connector's source (the default ReadFromDestination does that), see
// decodeSchemaEncoded.
func (a *acceptanceV2) decodeAll(t *testing.T, recs []opencdc.Record) []opencdc.Record {
	t.Helper()
	ctx := a.context(t)
	for i := range recs {
		rec, err := decodeSchemaEncoded(ctx, recs[i])
		if err != nil {
			t.Fatalf("a record read back has an attached schema but cannot be decoded with it: %v", err)
		}
		recs[i] = rec
	}
	return recs
}

// requireStored fails the test if any of want is not stored in the destination.
// That is what an ack promises (invariant 1).
func (a *acceptanceV2) requireStored(t *testing.T, want []opencdc.Record, what string) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	got := a.readAll(t, want)
	missing, _, _ := v2Diff(want, got)
	if len(missing) > 0 {
		t.Fatalf("%s: %d of %d records are not stored (first: %q); an acked record must be durably written", what, len(missing), len(want), missing[0])
	}
}

// requireAllAcked fails if any record of recs was not acked successfully, or if
// the connector sent an ack twice.
func (a *acceptanceV2) requireAllAcked(t *testing.T, d *v2Dest, recs []opencdc.Record) {
	t.Helper()
	if err := d.waitAcks(recs, a.ackTimeout(len(recs))); err != nil {
		t.Fatalf("%v", err)
	}
	ok, nacked := d.ackedRecords()
	if len(nacked) > 0 {
		d.mu.Lock()
		e := d.acks[string(nacked[0].Position)]
		d.mu.Unlock()
		t.Fatalf("%d records were nacked, first error: %s", len(nacked), e)
	}
	if len(ok) < len(recs) {
		t.Fatalf("expected %d acked records, got %d", len(recs), len(ok))
	}
	if n := d.dupAckCount(); n > 0 {
		t.Fatalf("%d positions were acked more than once", n)
	}
}

// requireAckOrder fails if the acks of recs did not arrive in the order the
// records were sent. The SDK declares ordered_acks in the destination Open
// response, and the engine relies on it to match acks to records when it keeps
// several writes in flight (ConduitIO/conduit#3292), so a reordered ack is a
// correctness bug, not a style issue. Call it after requireAllAcked.
func (a *acceptanceV2) requireAckOrder(t *testing.T, d *v2Dest, recs []opencdc.Record) {
	t.Helper()
	if err := d.ackOrderError(recs); err != nil {
		t.Fatalf("%v", err)
	}
}

func (a *acceptanceV2) mustStopGraceful(t *testing.T, d *v2Dest) {
	t.Helper()
	if err := d.stopGraceful(); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// DST-01: the host sends the next batches without waiting for earlier acks.
func (a *acceptanceV2) testDstPipelinedWrites(t *testing.T) {
	d := a.openDest(t, v2DestOptions{})

	want := a.generateRecords(t, opencdc.OperationCreate, 200)
	for _, b := range splitBatches(want, 10) {
		if err := d.send(b); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}
	a.requireAllAcked(t, d, want)
	a.requireAckOrder(t, d, want)
	a.mustStopGraceful(t, d)

	if a.caps.Destination.WriteOnly {
		return
	}
	got := a.readAll(t, want)
	missing, dups, unexpected := v2Diff(want, got)
	if len(missing) > 0 || dups > 0 || unexpected > 0 {
		t.Fatalf("stored data differs: %d missing, %d duplicated, %d unexpected of %d records written without any failure", len(missing), dups, unexpected, len(want))
	}
}

// DST-02: the host receives acks slowly and the ack stream applies
// backpressure to the plugin. The acks are sent by the SDK's destination
// adapter, not by the connector, so this case guards the adapter's ack path
// (no deadlock, no lost or reordered ack under backpressure) together with the
// connector's Write; a connector cannot block an ack, which is why no connector
// mutant exists for it (see the design document).
func (a *acceptanceV2) testDstSlowAcks(t *testing.T) {
	d := a.openDest(t, v2DestOptions{respBuf: 0, ackDelay: 25 * time.Millisecond})

	want := a.generateRecords(t, opencdc.OperationCreate, 150)
	sendErr := make(chan error, 1)
	go func() {
		for _, b := range splitBatches(want, 5) {
			if err := d.send(b); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	a.requireAllAcked(t, d, want)
	a.requireAckOrder(t, d, want)
	if err := <-sendErr; err != nil {
		t.Fatalf("send failed: %v", err)
	}
	a.mustStopGraceful(t, d)

	if !a.caps.Destination.WriteOnly {
		a.requireStored(t, want, "after slow acks")
	}
}

// DST-03: batches of 1, 100 and the configured maximum (10000 by default).
func (a *acceptanceV2) testDstBatchSizes(t *testing.T) {
	var all []opencdc.Record
	for _, size := range []int{1, 100, a.caps.maxBatchSize()} {
		count := size
		if size == 1 {
			count = 50 // 50 batches of one record
		}
		recs := a.generateRecords(t, opencdc.OperationCreate, count)

		d := a.openDest(t, v2DestOptions{})
		for _, b := range splitBatches(recs, size) {
			if err := d.send(b); err != nil {
				t.Fatalf("batch size %d: send failed: %v", size, err)
			}
		}
		a.requireAllAcked(t, d, recs)
		a.mustStopGraceful(t, d)
		all = append(all, recs...)
	}

	if a.caps.Destination.WriteOnly {
		return
	}
	got := a.readAll(t, all)
	missing, dups, unexpected := v2Diff(all, got)
	if len(missing) > 0 || dups > 0 || unexpected > 0 {
		t.Fatalf("stored data differs: %d missing, %d duplicated, %d unexpected of %d records", len(missing), dups, unexpected, len(all))
	}
}

// crashKillPoint is where in the stream the first destination instance is
// killed.
type crashKillPoint struct {
	name string
	// acked is how many batches must be acked before the kill. Negative means
	// the kill follows the sends immediately, after a short random delay, which
	// tends to land while the connector is inside Write.
	acked int
	// batch is the batch size, 0 means 10 records per batch for 200 records.
	// A large batch (more than 500) is its own kill point because connectors
	// switch strategy on batch size (a bulk path that buffers, a COPY).
	batch int
}

var crashKillPoints = []crashKillPoint{
	{name: "during_write", acked: -1},
	{name: "after_first_ack", acked: 1},
	{name: "after_half_acked", acked: 10},
	{name: "large_batch_after_first_ack", acked: 1, batch: 600},
}

// crashReplay writes 200 records in 20 batches (or 1200 in 2 for the large
// batch kill point), kills the destination at the
// given point, checks that everything acked is stored, replays the whole batch
// on a new instance and returns what was written and what is stored.
//
// The durability check runs before Teardown. A connector that keeps acked
// records in memory and flushes them in Teardown would otherwise be rescued by
// the harness's own cleanup; a real kill -9 gives it no Teardown.
func (a *acceptanceV2) crashReplay(t *testing.T, kp crashKillPoint) (want []opencdc.Record, got []opencdc.Record) {
	size, count := 10, 200
	if kp.batch > 0 {
		size, count = kp.batch, 2*kp.batch
	}
	want = a.generateRecords(t, opencdc.OperationCreate, count)

	d1 := a.openDest(t, v2DestOptions{})
	batches := splitBatches(want, size)
	for _, b := range batches {
		if err := d1.send(b); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}
	if kp.acked < 0 {
		time.Sleep(time.Duration(rand.Intn(4000)) * time.Microsecond) //nolint:gosec // jitter
	} else if err := d1.waitAcks(flatten(batches[:kp.acked]), a.ackTimeout(size*kp.acked)); err != nil {
		t.Fatalf("%v", err)
	}
	if !d1.abort() {
		t.Errorf("Run did not return within %v after the context was cancelled", abortTimeout)
	}

	// Invariant 1: whatever the first instance acknowledged is durably stored,
	// although it never got to shut down.
	acked, _ := d1.ackedRecords()
	t.Logf("kill point %s: first instance acked %d of %d records before it was killed", kp.name, len(acked), len(want))
	a.requireStored(t, acked, "after the destination was killed ("+kp.name+")")
	if err := d1.teardown(); err != nil {
		t.Logf("Teardown after the kill returned %v", err)
	}

	// The host does not know which records made it, it replays all of them.
	d2 := a.openDest(t, v2DestOptions{})
	for _, b := range batches {
		if err := d2.send(b); err != nil {
			t.Fatalf("replay send failed: %v", err)
		}
	}
	a.requireAllAcked(t, d2, want)
	a.mustStopGraceful(t, d2)

	return want, a.readAll(t, want)
}

// resetBetween gives the second and later iterations of a case a clean
// destination, the way the driver does between cases.
func (a *acceptanceV2) resetBetween(t *testing.T, iteration int) {
	t.Helper()
	if iteration == 0 {
		return
	}
	a.driver.AfterTest(t)
	a.driver.BeforeTest(t)
}

func flatten(batches [][]opencdc.Record) []opencdc.Record {
	var out []opencdc.Record
	for _, b := range batches {
		out = append(out, b...)
	}
	return out
}

// DST-04: at-least-once. After a crash and a replay nothing is missing;
// duplicates are tolerated unless the connector claimed idempotence.
func (a *acceptanceV2) testDstCrashReplay(t *testing.T) {
	for i, kp := range crashKillPoints {
		a.resetBetween(t, i)
		want, got := a.crashReplay(t, kp)
		missing, dups, unexpected := v2Diff(want, got)
		if len(missing) > 0 {
			t.Fatalf("%s: %d of %d records are missing after crash and replay (first: %q), that is data loss", kp.name, len(missing), len(want), missing[0])
		}
		if unexpected > 0 {
			t.Fatalf("%s: %d records were stored that were never written", kp.name, unexpected)
		}
		t.Logf("%s: replay produced %d duplicates of %d records (allowed unless idempotence is claimed)", kp.name, dups, len(want))
	}
}

// DST-05: same as DST-04 for connectors that claim idempotence, where
// duplicates are not allowed. The claim cannot be verified with a driver that
// cannot return duplicates, which fails the case.
func (a *acceptanceV2) testDstCrashReplayIdempotent(t *testing.T) {
	if !a.canReadAll() {
		t.Fatal("the connector claims idempotent writes, but the driver cannot return duplicates and so cannot verify it: implement AcceptanceReadAllDriver on the driver or set ReadAllFromSource in its config")
	}
	for i, kp := range crashKillPoints {
		a.resetBetween(t, i)
		want, got := a.crashReplay(t, kp)
		missing, dups, unexpected := v2Diff(want, got)
		if len(missing) > 0 {
			t.Fatalf("%s: %d of %d records are missing after crash and replay (first: %q), that is data loss", kp.name, len(missing), len(want), missing[0])
		}
		if dups > 0 || unexpected > 0 {
			t.Fatalf("%s: destination claims idempotent writes but replay left %d duplicates and %d unexpected records", kp.name, dups, unexpected)
		}
	}
}

// keyedRecords returns n records per key, interleaved round robin, so records
// of one key are never adjacent.
func (a *acceptanceV2) keyedRecords(t *testing.T, keys []string, perKey int) []opencdc.Record {
	t.Helper()
	var out []opencdc.Record
	for i := 0; i < perKey; i++ {
		for _, k := range keys {
			var r opencdc.Record
			if g, ok := a.driver.(AcceptanceKeyedRecordGenerator); ok {
				r = g.GenerateRecordWithKey(t, opencdc.OperationCreate, k)
			} else {
				r = a.driver.GenerateRecord(t, opencdc.OperationCreate)
				r.Key = opencdc.RawData(k)
			}
			out = append(out, r)
		}
	}
	return out
}

// requireKeyOrder fails if records of the same key appear in got in another
// order than in want.
func requireKeyOrder(t *testing.T, want, got []opencdc.Record, claim string) {
	t.Helper()
	wantID := v2Reconcile(want, got)
	keyOf := make(map[string]string, len(want))
	seq := make(map[string]int, len(want)) // position of the record in its key's sequence
	next := map[string]int{}
	for _, r := range want {
		k := string(r.Key.Bytes())
		id := wantID(r)
		keyOf[id] = k
		seq[id] = next[k]
		next[k]++
	}

	last := map[string]int{}
	for i, r := range got {
		id := v2ID(r)
		k, ok := keyOf[id]
		if !ok {
			continue
		}
		if prev, seen := last[k]; seen && seq[id] < prev {
			t.Fatalf("%s: record %d for key %q (sequence %d) came after sequence %d", claim, i, k, seq[id], prev)
		}
		last[k] = seq[id]
	}
}

// DST-06: records with the same key are stored in write order, in small and in
// large batches, and across a restart of the destination: the second part is
// written by a new instance after the first was stopped.
func (a *acceptanceV2) testDstOrdering(t *testing.T) {
	keys := []string{"key-a", "key-b", "key-c"}
	part1 := a.keyedRecords(t, keys, 30)  // 90 records in batches of 9
	part2 := a.keyedRecords(t, keys, 200) // 600 records in batches of 300

	d := a.openDest(t, v2DestOptions{})
	for _, b := range splitBatches(part1, 9) {
		if err := d.send(b); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}
	a.requireAllAcked(t, d, part1)
	a.mustStopGraceful(t, d)

	d = a.openDest(t, v2DestOptions{})
	for _, b := range splitBatches(part2, 300) {
		if err := d.send(b); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}
	a.requireAllAcked(t, d, part2)
	a.mustStopGraceful(t, d)

	want := append(append([]opencdc.Record{}, part1...), part2...)
	got := a.readAll(t, want)
	if missing, _, _ := v2Diff(want, got); len(missing) > 0 {
		t.Fatalf("%d of %d records are missing", len(missing), len(want))
	}
	requireKeyOrder(t, want, got, "destination claims OrdersByKey (small batches, a batch of 300, across a restart)")
}

// structuredRecords returns n generated records whose payload is structured. It
// reports false if the driver does not generate structured data.
func (a *acceptanceV2) structuredRecords(t *testing.T, n int) ([]opencdc.Record, bool) {
	t.Helper()
	recs := a.generateRecords(t, opencdc.OperationCreate, n)
	for i := range recs {
		if _, ok := recs[i].Payload.After.(opencdc.StructuredData); !ok {
			return nil, false
		}
	}
	return recs, true
}

const v2AddedField = "acceptance_added_field"

// withAddedField returns copies of recs with an additional payload field.
func withAddedField(recs []opencdc.Record) []opencdc.Record {
	out := make([]opencdc.Record, len(recs))
	for i, r := range recs {
		sd, _ := r.Payload.After.(opencdc.StructuredData)
		evolved := make(opencdc.StructuredData, len(sd)+1)
		for k, v := range sd {
			evolved[k] = v
		}
		evolved[v2AddedField] = fmt.Sprintf("added-%d", i)
		r.Payload.After = evolved
		out[i] = r
	}
	return out
}

// DST-07: schema evolution follows the declared policy, and never silently
// drops or alters data (invariant 6).
func (a *acceptanceV2) testDstSchemaEvolution(t *testing.T) {
	base, ok := a.structuredRecords(t, 10)
	if !ok {
		t.Fatal("the connector claims a schema evolution policy, but the driver generates raw payloads and the case needs structured data (use GenerateStructuredData)")
	}
	before := base[:5]
	after := withAddedField(base[5:])

	d := a.openDest(t, v2DestOptions{})
	if err := d.send(before); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	a.requireAllAcked(t, d, before)

	if err := d.send(after); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if err := d.waitAcks(after, a.ackTimeout(len(after))); err != nil {
		t.Fatalf("%v", err)
	}
	okRecs, nacked := d.ackedRecords()

	switch a.caps.Destination.SchemaEvolution {
	case SchemaEvolutionEvolve:
		if len(nacked) > 0 {
			d.mu.Lock()
			e := d.acks[string(nacked[0].Position)]
			d.mu.Unlock()
			t.Fatalf("destination claims to evolve but nacked %d records with a new field: %s", len(nacked), e)
		}
		a.mustStopGraceful(t, d)
		if !a.caps.Destination.WriteOnly {
			want := append(append([]opencdc.Record{}, before...), after...)
			got := a.readAll(t, want)
			missing, _, _ := v2Diff(want, got)
			if len(missing) > 0 {
				t.Fatalf("%d records are missing or were stored without the added field %q (first: %q)", len(missing), v2AddedField, missing[0])
			}
		}
	case SchemaEvolutionReject:
		if len(nacked) != len(after) {
			t.Fatalf("destination claims to reject unknown fields but acked %d of %d records with a new field; accepting them means the field was dropped or coerced", len(okRecs)-len(before), len(after))
		}
		d.mu.Lock()
		e := d.acks[string(nacked[0].Position)]
		d.mu.Unlock()
		if e == "" {
			t.Fatal("a nack must carry an error message")
		}
		a.mustStopGraceful(t, d)
		if !a.caps.Destination.WriteOnly {
			a.requireStored(t, before, "records written before the rejected ones")
		}
	case SchemaEvolutionNone:
		t.Fatal("test ran without a declared schema evolution policy") // the case's precondition prevents this
	default:
		t.Fatalf("unknown schema evolution policy %q", a.caps.Destination.SchemaEvolution)
	}
}

// largeRecord returns a record with a payload of about size bytes.
func (a *acceptanceV2) largeRecord(t *testing.T, size int) opencdc.Record {
	t.Helper()
	if g, ok := a.driver.(AcceptanceLargeRecordGenerator); ok {
		return g.GenerateLargeRecord(t, opencdc.OperationCreate, size)
	}
	r := a.driver.GenerateRecord(t, opencdc.OperationCreate)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // test data
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = letters[rnd.Intn(len(letters))]
	}
	switch p := r.Payload.After.(type) {
	case opencdc.StructuredData:
		p["acceptance_padding"] = string(buf)
	default:
		r.Payload.After = opencdc.RawData(buf)
	}
	return r
}

// DST-08: a record of MaxRecordBytes (1 MiB by default) survives intact.
func (a *acceptanceV2) testDstLargeRecord(t *testing.T) {
	size := a.caps.maxRecordBytes()
	want := []opencdc.Record{a.largeRecord(t, size)}

	d := a.openDest(t, v2DestOptions{})
	if err := d.send(want); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	a.requireAllAcked(t, d, want)
	a.mustStopGraceful(t, d)

	if !a.caps.Destination.WriteOnly {
		a.requireStored(t, want, fmt.Sprintf("%d byte record", size))
	}
}

// DST-09: a graceful stop drains. Stop is called while many batches are still
// in flight; after it returns every record was acked and is stored (invariant
// 7). The suite also verifies that nothing leaks.
func (a *acceptanceV2) testDstGracefulStop(t *testing.T) {
	defer a.verifyGoleaks(t)

	d := a.openDest(t, v2DestOptions{})
	want := a.generateRecords(t, opencdc.OperationCreate, 500)
	for _, b := range splitBatches(want, 10) {
		if err := d.send(b); err != nil {
			t.Fatalf("send failed: %v", err)
		}
	}
	a.mustStopGraceful(t, d) // immediately, most batches are still queued

	ok, nacked := d.ackedRecords()
	if len(nacked) > 0 || len(ok) != len(want) {
		t.Fatalf("graceful stop returned with %d of %d records acked (%d nacked); records in flight must be written and acked before Stop returns", len(ok), len(want), len(nacked))
	}
	if !a.caps.Destination.WriteOnly {
		a.requireStored(t, want, "after graceful stop")
	}
}

// DST-10: cancelling the context while writes are in flight ends the plugin
// promptly, and whatever it acked is stored.
func (a *acceptanceV2) testDstContextCancel(t *testing.T) {
	defer a.verifyGoleaks(t)

	d := a.openDest(t, v2DestOptions{})
	want := a.generateRecords(t, opencdc.OperationCreate, 500)
	go func() {
		for _, b := range splitBatches(want, 10) {
			if d.send(b) != nil {
				return // stream closed by the crash
			}
		}
	}()
	if err := d.waitAcks(want[:10], a.ackTimeout(10)); err != nil {
		t.Fatalf("%v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the connector get into the next writes

	start := time.Now()
	ended := d.abort()
	if !ended {
		t.Errorf("Run did not end within %v after the context was cancelled", abortTimeout)
	}
	t.Logf("plugin ended %v after cancellation", time.Since(start).Round(time.Millisecond))

	// check what is durable before Teardown, see crashReplay
	acked, _ := d.ackedRecords()
	if !a.caps.Destination.WriteOnly {
		a.requireStored(t, acked, "after context cancellation")
	}
	if err := d.teardown(); err != nil {
		t.Logf("Teardown after cancellation returned %v", err)
	}
}

// DST-11: records/s over ThroughputDuration. Recorded in the report, never
// asserted. All acks must still be successful.
func (a *acceptanceV2) testDstThroughput(t *testing.T) {
	const (
		batchSize = 100
		window    = 20 // batches in flight
	)
	dur := a.caps.throughputDuration()
	d := a.openDest(t, v2DestOptions{})

	start := time.Now()
	var sent []opencdc.Record
	var batches [][]opencdc.Record
	for time.Since(start) < dur {
		b := a.generateRecords(t, opencdc.OperationCreate, batchSize)
		if err := d.send(b); err != nil {
			t.Fatalf("send failed: %v", err)
		}
		sent = append(sent, b...)
		batches = append(batches, b)
		if len(batches) > window {
			// keep at most `window` batches in flight
			if err := d.waitAcks(batches[len(batches)-window-1], a.ackTimeout(batchSize)); err != nil {
				t.Fatalf("%v", err)
			}
		}
	}
	a.requireAllAcked(t, d, sent)
	elapsed := time.Since(start)
	a.mustStopGraceful(t, d)

	a.metric(t, "destination_throughput", float64(len(sent))/elapsed.Seconds(), "records/s",
		fmt.Sprintf("%d records in %.1fs, batch size %d, %d batches in flight, in-process plugin adapter", len(sent), elapsed.Seconds(), batchSize, window))
}
