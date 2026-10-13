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
	_ "embed"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

// This file tests the v2 acceptance suite itself. A reference connector backed
// by an in-memory store must pass it. For each known class of connector bug
// there is a mutant of the reference connector, and the suite must fail it on
// the expected test. A suite that passes broken connectors is worse than none.
//
// Mutants run in a subprocess (the test binary itself), because a failing
// suite cannot be observed from inside the process that runs it. Each
// subprocess runs only the case that is supposed to catch the mutant.

const (
	refMutantEnv = "ACCEPTANCE_V2_REF_MUTANT"
	refClaimsEnv = "ACCEPTANCE_V2_REF_CLAIMS"
)

// refGoleakOptions ignores the twmb/go-cache goroutine that leaks since
// go-cache v1.4.0 (https://github.com/ConduitIO/conduit-connector-sdk/issues/392).
// Remove this once the leak is fixed at the source.
func refGoleakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreAnyFunction("github.com/twmb/go-cache/cache.autoClean[...]"),
		goleak.IgnoreAnyFunction("github.com/twmb/go-cache/cache.New[...].func1"),
	}
}

type refStore struct {
	mu      sync.Mutex
	recs    []opencdc.Record
	pending []opencdc.Record
	seen    map[string]struct{}
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
	if i >= len(s.recs) {
		return opencdc.Record{}, false
	}
	return s.recs[i].Clone(), true
}

func refMutant() string { return os.Getenv(refMutantEnv) }

// refCurrentTest is the name of the running acceptance test, set in BeforeTest.
// A mutant can restrict itself to one test with it, so that unrelated tests
// don't wait for it.
var refCurrentTest atomic.Value

func refRunning(name string) bool {
	cur, _ := refCurrentTest.Load().(string)
	return strings.Contains(cur, name)
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
	next   int
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

func (s *refSource) Open(_ context.Context, pos opencdc.Position) error {
	s.store = getRefStore(s.config.Store)
	if pos != nil {
		last, err := strconv.Atoi(string(pos))
		if err != nil {
			return err
		}
		s.next = last + 1
		if refMutant() == "source_gap" {
			s.next++ // resume one record too late: the first unacked record is skipped
		}
	}
	return nil
}

func (s *refSource) Read(ctx context.Context) (opencdc.Record, error) {
	for {
		if r, ok := s.store.get(s.next); ok {
			r.Position = opencdc.Position(strconv.Itoa(s.next))
			s.next++
			return r, nil
		}
		if refMutant() == "source_ignores_ctx" && refRunning("SRC-08") {
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

func (s *refSource) Ack(context.Context, opencdc.Position) error { return nil }
func (s *refSource) Teardown(context.Context) error              { return nil }

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
}

func newRefDestination() Destination {
	return DestinationWithMiddleware(&refDestination{})
}

func (d *refDestination) Config() DestinationConfig { return &d.config }

func (d *refDestination) Open(context.Context) error {
	d.store = getRefStore(d.config.Store)
	d.closed = make(chan struct{})
	return nil
}

func (d *refDestination) Write(_ context.Context, recs []opencdc.Record) (int, error) {
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

	if refMutant() == "dest_reorders" {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}

	if refMutant() == "dest_acks_before_durable" {
		d.storeLater(out)
		return len(recs), nil
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
	}
	if os.Getenv(refClaimsEnv) == "reject" && hasField(r.Payload.After, v2AddedField) {
		return r, false, errors.New("unknown field " + v2AddedField + ", refusing to drop it")
	}
	if refMutant() != "dest_dups" && strings.Contains(os.Getenv(refClaimsEnv), "idempotent") {
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

func (d *refDestination) Teardown(context.Context) error {
	if d.closed != nil {
		close(d.closed)
	}
	return nil
}

func refConnector() Connector {
	return Connector{
		NewSpecification: YAMLSpecification(refSpecYAML, ""),
		NewSource:        newRefSource,
		NewDestination:   newRefDestination,
	}
}

func refDriver(t *testing.T, caps AcceptanceCapabilities) AcceptanceTestDriver {
	store := strings.ReplaceAll(t.Name(), "/", "_")
	refStores.Delete(store)
	cfg := config.Config{"store": store}
	return ConfigurableAcceptanceTestDriver{
		Config: ConfigurableAcceptanceTestDriverConfig{
			Connector:         refConnector(),
			SourceConfig:      cfg,
			DestinationConfig: cfg,
			GenerateDataType:  GenerateStructuredData,
			GoleakOptions:     refGoleakOptions(),
			BeforeTest: func(t *testing.T) {
				refStores.Delete(store)
				refCurrentTest.Store(t.Name())
			},
			ReadTimeout:  300 * time.Millisecond,
			WriteTimeout: 300 * time.Millisecond,
			Capabilities: caps,
		},
	}
}

func refCapabilities() AcceptanceCapabilities {
	caps := AcceptanceCapabilities{
		Destination: DestinationCapabilities{
			IdempotentWrites: strings.Contains(os.Getenv(refClaimsEnv), "idempotent"),
			OrdersByKey:      strings.Contains(os.Getenv(refClaimsEnv), "ordering"),
			SchemaEvolution:  SchemaEvolutionEvolve,
		},
		Source: SourceCapabilities{
			OrdersByKey:     strings.Contains(os.Getenv(refClaimsEnv), "ordering"),
			SchemaEvolution: SchemaEvolutionEvolve,
		},
		ThroughputDuration: 500 * time.Millisecond,
		MaxBatchSize:       2000,
	}
	if os.Getenv(refClaimsEnv) == "reject" {
		caps.Destination.SchemaEvolution = SchemaEvolutionReject
	}
	return caps
}

// TestAcceptanceV2_Reference is the entry point for the reference connector,
// both for the direct run and for the mutant subprocesses.
func TestAcceptanceV2_Reference(t *testing.T) {
	// Only the v2 group: mutants are built to fail it, and the v1 group
	// has its own tests.
	d := refDriver(t, refCapabilities())
	runAcceptanceV2(t, d, refCapabilities(), newAcceptanceReport(d, refCapabilities()))
}

func TestAcceptanceV2_ReferencePasses(t *testing.T) {
	t.Setenv(refClaimsEnv, "idempotent,ordering")
	report := filepath.Join(t.TempDir(), "report.json")
	t.Setenv(AcceptanceReportEnv, report)

	// registered first so that it runs after the report was written
	t.Cleanup(func() {
		is := is.New(t)
		b, err := os.ReadFile(report)
		is.NoErr(err)
		var rep AcceptanceReport
		is.NoErr(json.Unmarshal(b, &rep))
		is.Equal(rep.SuiteVersion, 2)
		is.Equal(rep.SuiteRevision, AcceptanceSuiteV2Revision)
		is.Equal(rep.Connector.Name, "acceptance-v2-reference")
		is.True(rep.Capabilities.Destination.IdempotentWrites)
		var core, v2 int
		for _, c := range rep.Cases {
			if strings.HasPrefix(c.ID, "CORE-") {
				core++
				continue
			}
			v2++
			is.Equal(c.Outcome, AcceptanceOutcomePassed) // all v2 cases apply to the reference
		}
		is.Equal(v2, len(v2Cases()))
		is.True(core > 0)
		is.True(len(rep.Metrics) == 2) // both throughput checks recorded a number
	})

	AcceptanceTestV2(t, refDriver(t, refCapabilities()))
}

func TestAcceptanceV2_ReferenceRejectPolicyPasses(t *testing.T) {
	t.Setenv(refClaimsEnv, "reject")
	caps := refCapabilities()
	caps.Source.SchemaEvolution = SchemaEvolutionNone
	AcceptanceTestV2(t, refDriver(t, caps))
}

func TestAcceptanceV2_UnclaimedCapabilitiesAreSkipped(t *testing.T) {
	report := filepath.Join(t.TempDir(), "report.json")
	t.Setenv(AcceptanceReportEnv, report)

	t.Cleanup(func() {
		is := is.New(t)
		b, err := os.ReadFile(report)
		is.NoErr(err)
		var rep AcceptanceReport
		is.NoErr(json.Unmarshal(b, &rep))
		skipped := map[string]string{}
		for _, c := range rep.Cases {
			if c.Outcome == AcceptanceOutcomeSkipped && !strings.HasPrefix(c.ID, "CORE-") {
				skipped[c.ID] = c.Reason
			}
		}
		for _, id := range []string{"DST-05", "DST-06", "DST-07", "SRC-04", "SRC-05"} {
			is.True(strings.HasPrefix(skipped[id], "capability not claimed")) // test must skip with the reason
		}
		is.Equal(len(skipped), 5)
	})

	AcceptanceTestV2(t, refDriver(t, AcceptanceCapabilities{ThroughputDuration: 200 * time.Millisecond, MaxBatchSize: 200}))
}

// TestAcceptanceV2_Mutants checks that the suite catches known bug classes.
func TestAcceptanceV2_Mutants(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the test binary once per mutant")
	}
	mutants := []struct {
		mutant string
		claims string
		// failing is the case that must fail
		failing string
	}{
		{"dest_lossy", "", "DST-01"},
		{"dest_acks_before_durable", "", "DST-04"},
		{"dest_dups", "idempotent", "DST-05"},
		{"dest_reorders", "ordering", "DST-06"},
		{"dest_drops_new_field", "", "DST-07"},
		{"source_gap", "", "SRC-03"},
		{"source_ignores_ctx", "", "SRC-08"},
	}
	for _, m := range mutants {
		t.Run(m.mutant, func(t *testing.T) {
			t.Parallel()
			cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestAcceptanceV2_Reference$/^V2$/^"+m.failing+"$", "-test.v=true")
			cmd.Env = append(os.Environ(), refMutantEnv+"="+m.mutant, refClaimsEnv+"="+m.claims)
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out
			err := cmd.Run()
			if err == nil {
				t.Fatalf("suite passed mutant %q, it must fail %s", m.mutant, m.failing)
			}
			if !strings.Contains(out.String(), "--- FAIL: TestAcceptanceV2_Reference/V2/"+m.failing+"/") {
				t.Fatalf("mutant %q: the run failed, but not in %s:\n%s", m.mutant, m.failing, tail(out.String(), 4000))
			}
		})
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
