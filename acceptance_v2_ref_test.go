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
	_ "embed"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
)

// The reference connector for the tests of the v2 suite itself. It lives apart
// from internal/refconnector on purpose: that one is deliberately free of
// fault injection and of goleak options, so that it fails first when the SDK or
// a dependency breaks the contract (conduit-connector-sdk#392). This one has a
// hook for every class of bug the v2 suite is meant to catch, selected by
// environment variable, and the mutant tests check that the suite catches each.

const (
	refMutantEnv = "ACCEPTANCE_V2_REF_MUTANT"
	refClaimsEnv = "ACCEPTANCE_V2_REF_CLAIMS"
)

func refMutant() string { return os.Getenv(refMutantEnv) }

func refClaim(name string) bool {
	for _, c := range strings.Split(os.Getenv(refClaimsEnv), ",") {
		if c == name {
			return true
		}
	}
	return false
}

type refStore struct {
	mu      sync.Mutex
	recs    []opencdc.Record
	pending []opencdc.Record
	seen    map[string]struct{}

	// snapEnd is the number of records the first source Open found: records
	// before it are snapshot records, records after it are changes.
	snapEnd   int
	snapSet   bool
	committed int // position a source that commits in Teardown resumes from
}

var refStores sync.Map // name -> *refStore

func getRefStore(name string) *refStore {
	s, _ := refStores.LoadOrStore(name, &refStore{seen: map[string]struct{}{}})
	return s.(*refStore)
}

func (s *refStore) append(recs ...opencdc.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range recs {
		s.recs = append(s.recs, r.Clone())
	}
}

func (s *refStore) get(i int) (opencdc.Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.recs) {
		return opencdc.Record{}, false
	}
	return s.recs[i].Clone(), true
}

// snapshotEnd fixes the end of the snapshot at the first call.
func (s *refStore) snapshotEnd() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.snapSet {
		s.snapSet = true
		s.snapEnd = len(s.recs)
	}
	return s.snapEnd
}

func (s *refStore) length() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.recs)
}

//go:embed testdata/acceptance_v2_reference.yaml
var refSpecYAML string

type refSourceConfig struct {
	DefaultSourceMiddleware
	Store string `json:"store" validate:"required"`
}

func (refSourceConfig) Validate(context.Context) error { return nil }

type refSource struct {
	UnimplementedSource
	config refSourceConfig
	store  *refStore

	mu        sync.Mutex
	next      int
	snapEnd   int
	resumed   bool
	lastAcked int
	acks      int
}

func newRefSource() Source {
	return SourceWithMiddleware(&refSource{config: refSourceConfig{
		DefaultSourceMiddleware: DefaultSourceMiddleware{
			SourceWithSchemaExtraction: SourceWithSchemaExtraction{
				PayloadEnabled: lang.Ptr(false),
				KeyEnabled:     lang.Ptr(false),
			},
		},
	}})
}

func (s *refSource) Config() SourceConfig { return &s.config }

// refResumes counts Open calls with a position in the process, for the mutant
// that misbehaves on one particular restart.
var refResumes atomic.Int64

// Positions are "s<n>" for snapshot records and "c<n>" for changes.
func (s *refSource) Open(_ context.Context, pos opencdc.Position) error {
	s.store = getRefStore(s.config.Store)
	s.lastAcked = -1
	if pos == nil {
		s.snapEnd = s.store.snapshotEnd()
		return nil
	}
	s.snapEnd = s.store.snapshotEnd()
	s.resumed = true
	if len(pos) < 2 {
		return errors.New("invalid position")
	}
	kind := pos[0]
	last, err := strconv.Atoi(string(pos[1:]))
	if err != nil {
		return err
	}
	s.next = last + 1 // the correct, exact resume
	switch refMutant() {
	case "source_gap":
		s.next++ // one record too late: the first unacked record is skipped
	case "source_ignores_position":
		s.next = 0 // starts over
	case "source_dups_on_resume":
		s.next = max(0, last-2) // delivers the last three acked records again
	case "source_redelivers_k":
		k, _ := strconv.Atoi(os.Getenv("ACCEPTANCE_V2_REF_REDELIVER"))
		s.next = max(0, last+1-k)
	case "r3_boundary_restart":
		if kind == 's' && last == s.snapEnd-1 {
			s.next = 0 // resuming from the last snapshot position restarts the snapshot
		}
	case "r3_boundary_drop":
		if kind == 's' && last == s.snapEnd-1 {
			s.next = last + 2 // and drops the first change after it
		}
	case "r3_resume_n_loses_one":
		n, _ := strconv.Atoi(os.Getenv("ACCEPTANCE_V2_REF_REDELIVER"))
		if int(refResumes.Add(1)) == n {
			s.next++
		}
	case "cdc_resume_loses_one":
		if kind == 'c' {
			s.next++
		}
	case "cdc_resume_starts_over":
		if kind == 'c' {
			s.next = s.snapEnd
		}
	case "source_commits_in_teardown":
		// ignores the position the host passes and resumes from what it
		// committed in Teardown
		s.store.mu.Lock()
		s.next = s.store.committed
		s.store.mu.Unlock()
	}
	return nil
}

// recordAt returns the record the source emits as number idx.
func (s *refSource) recordAt(idx int) (opencdc.Record, bool) {
	src := idx
	switch refMutant() {
	case "source_emits_dups":
		src = idx / 2 // every record twice
	case "source_reorders", "source_reorders_after_resume", "r3_cdc_reorders":
		// reverse blocks of 9 once they are complete
		if refMutant() == "source_reorders" || (refMutant() == "r3_cdc_reorders" && idx >= s.snapEnd) || (refMutant() == "source_reorders_after_resume" && s.resumed) {
			if blk := idx / 9 * 9; blk+9 <= s.store.length() {
				src = blk + 8 - idx%9
			}
		}
	case "source_stalls":
		if idx >= 1000 {
			return opencdc.Record{}, false
		}
	}
	r, ok := s.store.get(src)
	if !ok {
		return r, false
	}
	kind := "c"
	if idx < s.snapEnd {
		kind = "s"
	}
	r.Position = opencdc.Position(kind + strconv.Itoa(idx))
	switch refMutant() {
	case "source_drops_new_field":
		r.Payload.After = withoutField(r.Payload.After, v2AddedField)
	case "source_truncates_large":
		r.Payload.After = truncateField(r.Payload.After, "acceptance_padding", 512<<10)
	}
	return r, true
}

func (s *refSource) Read(ctx context.Context) (opencdc.Record, error) {
	for {
		s.mu.Lock()
		r, ok := s.recordAt(s.next)
		if ok {
			s.next++
		}
		s.mu.Unlock()
		if ok {
			return r, nil
		}
		if refMutant() == "source_ignores_ctx" {
			time.Sleep(15 * time.Second) // a Read that never looks at its context
			continue
		}
		select {
		case <-ctx.Done():
			return opencdc.Record{}, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (s *refSource) Ack(_ context.Context, pos opencdc.Position) error {
	if idx, err := strconv.Atoi(string(pos[1:])); err == nil {
		s.mu.Lock()
		s.lastAcked = max(s.lastAcked, idx)
		s.mu.Unlock()
	}
	if refMutant() == "r3_ack_error" {
		s.mu.Lock()
		s.acks++
		n := s.acks
		s.mu.Unlock()
		if n == 5 {
			return errors.New("r3 injected ack failure")
		}
	}
	if refMutant() == "source_ack_blocks_read" {
		// Ack holds the lock that Read needs. With acks that lag reads, Read
		// is blocked for as long as an Ack takes.
		s.mu.Lock()
		defer s.mu.Unlock()
		time.Sleep(15 * time.Second)
	}
	return nil
}

func (s *refSource) Teardown(context.Context) error {
	if refMutant() == "source_commits_in_teardown" && s.store != nil {
		// the position is only made durable here
		s.mu.Lock()
		last := s.lastAcked
		s.mu.Unlock()
		s.store.mu.Lock()
		s.store.committed = last + 1
		s.store.mu.Unlock()
	}
	if refMutant() == "source_leaks_goroutine" {
		go func() { select {} }()
	}
	return nil
}

type refDestinationConfig struct {
	DefaultDestinationMiddleware
	Store string `json:"store" validate:"required"`
}

func (refDestinationConfig) Validate(context.Context) error { return nil }

type refDestination struct {
	UnimplementedDestination
	config refDestinationConfig
	store  *refStore
	closed chan struct{}
	calls  atomic.Int64
	// instance counts Open calls of destinations in the process, so a mutant
	// can misbehave only after a restart.
	instance int64
}

var refDestInstances atomic.Int64

// refWritten counts records written by all destinations of the process, for
// the mutant that fails under load.
var refWritten atomic.Int64

func newRefDestination() Destination {
	return DestinationWithMiddleware(&refDestination{})
}

func (d *refDestination) Config() DestinationConfig { return &d.config }

func (d *refDestination) Open(context.Context) error {
	d.store = getRefStore(d.config.Store)
	d.closed = make(chan struct{})
	d.instance = refDestInstances.Add(1)
	return nil
}

func (d *refDestination) Write(_ context.Context, recs []opencdc.Record) (int, error) {
	call := d.calls.Add(1)
	if refMutant() == "dest_ignores_ctx" && call >= 2 {
		time.Sleep(25 * time.Second) // a Write that never looks at its context
	}
	if refMutant() == "dest_nacks_under_load" && refWritten.Add(int64(len(recs))) > 1000 {
		return 0, errors.New("overloaded")
	}

	out := make([]opencdc.Record, 0, len(recs))
	for i, r := range recs {
		r, keep, err := d.apply(i, r)
		if err != nil {
			return i, err
		}
		if keep {
			out = append(out, r)
		}
	}

	switch refMutant() {
	case "dest_reorders", "dest_reorders_large_batch", "dest_reorders_after_restart":
		m := refMutant()
		if m == "dest_reorders" || (m == "dest_reorders_large_batch" && len(out) > 100) || (m == "dest_reorders_after_restart" && d.instance >= 2) {
			for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
				out[i], out[j] = out[j], out[i]
			}
		}
	case "dest_caps_batch":
		if len(out) > 1000 {
			out = out[:1000] // acked in full, stored in part
		}
	case "dest_acks_before_durable":
		d.storeLater(out)
		return len(recs), nil
	case "dest_flush_in_teardown":
		d.store.mu.Lock()
		d.store.pending = append(d.store.pending, out...)
		d.store.mu.Unlock()
		return len(recs), nil
	case "dest_flush_in_teardown_large":
		if len(out) > 500 {
			// only the bulk path buffers
			d.store.mu.Lock()
			d.store.pending = append(d.store.pending, out...)
			d.store.mu.Unlock()
			return len(recs), nil
		}
	}

	d.store.append(out...)
	return len(recs), nil
}

// apply runs the behavior of the configured mutant or claim on one record.
func (d *refDestination) apply(i int, r opencdc.Record) (_ opencdc.Record, keep bool, _ error) {
	switch refMutant() {
	case "dest_lossy":
		if i%7 == 6 {
			return r, false, nil // acked but never stored
		}
	case "dest_drops_new_field":
		r.Payload.After = withoutField(r.Payload.After, v2AddedField)
	case "dest_truncates_large":
		r.Payload.After = truncateField(r.Payload.After, "acceptance_padding", 512<<10)
	}
	if refClaim("reject") && hasField(r.Payload.After, v2AddedField) {
		return r, false, errors.New("unknown field " + v2AddedField + ", refusing to drop it")
	}
	if refClaim("idempotent") && refMutant() != "dest_dups" {
		d.store.mu.Lock()
		_, dup := d.store.seen[string(r.Position)]
		d.store.seen[string(r.Position)] = struct{}{}
		d.store.mu.Unlock()
		if dup {
			return r, false, nil
		}
	}
	return r, true, nil
}

// storeLater buffers the records and makes them durable only after a delay. A
// crash in between loses records that were already acked.
func (d *refDestination) storeLater(recs []opencdc.Record) {
	d.store.mu.Lock()
	d.store.pending = append(d.store.pending, recs...)
	d.store.mu.Unlock()
	go func() {
		select {
		case <-time.After(400 * time.Millisecond):
			d.store.mu.Lock()
			d.store.recs = append(d.store.recs, d.store.pending...)
			d.store.pending = nil
			d.store.mu.Unlock()
		case <-d.closed:
		}
	}()
}

func (d *refDestination) Teardown(context.Context) error {
	if (refMutant() == "dest_flush_in_teardown" || refMutant() == "dest_flush_in_teardown_large") && d.store != nil {
		// records acked earlier become durable only now
		d.store.mu.Lock()
		d.store.recs = append(d.store.recs, d.store.pending...)
		d.store.pending = nil
		d.store.mu.Unlock()
	}
	if d.closed != nil {
		close(d.closed)
	}
	return nil
}

func hasField(d opencdc.Data, name string) bool {
	sd, ok := d.(opencdc.StructuredData)
	if !ok {
		return false
	}
	_, has := sd[name]
	return has
}

func withoutField(d opencdc.Data, name string) opencdc.Data {
	sd, ok := d.(opencdc.StructuredData)
	if !ok || !hasField(d, name) {
		return d
	}
	cp := make(opencdc.StructuredData, len(sd))
	for k, v := range sd {
		if k != name {
			cp[k] = v
		}
	}
	return cp
}

// truncateField shortens a string field of a structured payload to n bytes.
func truncateField(d opencdc.Data, name string, n int) opencdc.Data {
	sd, ok := d.(opencdc.StructuredData)
	if !ok {
		return d
	}
	str, ok := sd[name].(string)
	if !ok || len(str) <= n {
		return d
	}
	cp := make(opencdc.StructuredData, len(sd))
	for k, v := range sd {
		cp[k] = v
	}
	cp[name] = str[:n]
	return cp
}

func refConnector() Connector {
	return Connector{
		NewSpecification: YAMLSpecification(refSpecYAML, ""),
		NewSource:        newRefSource,
		NewDestination:   newRefDestination,
	}
}
