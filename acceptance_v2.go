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
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

// AcceptanceSuiteV2Revision is the revision of the v2 acceptance suite. The
// major number is the bar ("suite v2"), the minor number is bumped when tests
// are added that only apply to connectors claiming a capability, and a patch
// number is bumped for fixes that do not change what a connector has to do.
// The value is written into the report (see [AcceptanceReportEnv]) so that
// whoever consumes the report knows which bar was passed.
const AcceptanceSuiteV2Revision = "2.0.0"

// AcceptanceReportEnv is the name of the environment variable that makes
// [AcceptanceTestV2] write a machine-readable [AcceptanceReport] (JSON) to the
// path it contains. The file is written when the test finishes, regardless of
// the outcome.
const AcceptanceReportEnv = "CONDUIT_ACCEPTANCE_REPORT"

// SchemaEvolutionPolicy is how a connector handles records whose structure
// changes while it runs (e.g. a new field appears). The connector declares one
// policy; the suite verifies that exactly that policy is followed. Silent
// coercion or truncation is never a valid policy (data integrity invariant 6).
type SchemaEvolutionPolicy string

const (
	// SchemaEvolutionNone means the connector makes no claim. The schema
	// evolution test is skipped.
	SchemaEvolutionNone SchemaEvolutionPolicy = ""
	// SchemaEvolutionEvolve means the connector adapts to new fields: records
	// with the new field are written/read with all fields intact.
	SchemaEvolutionEvolve SchemaEvolutionPolicy = "evolve"
	// SchemaEvolutionReject means the connector refuses records it cannot
	// represent: a destination nacks them with an error, and the data it
	// accepted earlier stays intact.
	SchemaEvolutionReject SchemaEvolutionPolicy = "reject"
)

// AcceptanceCapabilities declares what a connector claims to support, beyond
// the baseline every connector must meet. A conditional test runs only when the
// matching claim is made; a claim that is made is verified. Claims are part of
// the report, so a registry can show what a "passed" result actually covered.
//
// The zero value claims nothing, which is always safe: only the baseline tests
// run.
type AcceptanceCapabilities struct {
	Source      SourceCapabilities      `json:"source"`
	Destination DestinationCapabilities `json:"destination"`

	// MaxBatchSize is the largest batch the suite sends in the mixed batch
	// size tests. The default is 10000. Lower it only if the 3rd party system
	// has a hard limit that Conduit's batching configuration cannot work
	// around, and document why in the connector.
	MaxBatchSize int `json:"max_batch_size,omitempty"`
	// MaxRecordBytes is the size of the record used in the large record tests.
	// The default is 1 MiB. Values above 3 MiB are capped, because the default
	// maximum gRPC message size for connector plugins is 4 MiB.
	MaxRecordBytes int `json:"max_record_bytes,omitempty"`
	// ThroughputDuration is how long the throughput smoke checks run. The
	// default is 3 seconds. The result is recorded in the report, never
	// asserted.
	ThroughputDuration time.Duration `json:"throughput_duration_ns,omitempty"`
}

// SourceCapabilities are the optional claims of a source connector.
type SourceCapabilities struct {
	// OrdersByKey claims that records with the same key are produced in the
	// order they were written to the 3rd party system.
	OrdersByKey bool `json:"orders_by_key,omitempty"`
	// Synthetic declares that the source produces records on its own (a
	// generator) and cannot be fed through the 3rd party system. Tests that
	// compare what was written with what was read are skipped, the tests that
	// only need records to flow (graceful stop, cancellation, throughput) run.
	Synthetic bool `json:"synthetic,omitempty"`
	// SchemaEvolution is the policy for fields that appear while the source
	// is running. Only [SchemaEvolutionEvolve] can be verified on a source.
	SchemaEvolution SchemaEvolutionPolicy `json:"schema_evolution,omitempty"`
}

// DestinationCapabilities are the optional claims of a destination connector.
type DestinationCapabilities struct {
	// IdempotentWrites claims that writing the same records again (same
	// position and key) after a crash does not create duplicates. Without the
	// claim, the suite requires at-least-once only: replayed records may appear
	// twice but must never be missing.
	IdempotentWrites bool `json:"idempotent_writes,omitempty"`
	// OrdersByKey claims that records with the same key are stored in the order
	// they were written.
	OrdersByKey bool `json:"orders_by_key,omitempty"`
	// SchemaEvolution is the policy for records whose structure changes
	// while the destination is running.
	SchemaEvolution SchemaEvolutionPolicy `json:"schema_evolution,omitempty"`
	// WriteOnly declares that the destination cannot be read back (for
	// example a log or a webhook). Tests that only need acks still run, tests
	// that need to inspect the stored data are skipped and say so in the
	// report.
	WriteOnly bool `json:"write_only,omitempty"`
}

// skipCore returns the reason a v1 test is not part of the core group of suite
// v2. The v1 specification and configuration tests run unchanged. The v1 data
// path tests do not: they read and write synchronously and never acknowledge,
// which source connectors that gate reads on acks (they hold back the next batch
// until the previous one is acked, as the Postgres source does) cannot pass,
// and which cannot be used for generators or sinks. The v2 cases cover the same
// ground through the plugin interface, with acks, as Conduit does.
func (c AcceptanceCapabilities) skipCore(testName string) string {
	switch testName {
	case "TestSource_Read_Success":
		return "covered by SRC-02, which reads snapshot and change capture records and acks like a pipeline"
	case "TestSource_Read_Timeout":
		return "covered by SRC-08"
	case "TestSource_Open_ResumeAtPositionSnapshot", "TestSource_Open_ResumeAtPositionCDC":
		return "covered by SRC-03 and SRC-07"
	case "TestDestination_Write_Success":
		return "covered by DST-01 and DST-03"
	}
	return ""
}

func (c AcceptanceCapabilities) maxBatchSize() int {
	if c.MaxBatchSize <= 0 {
		return 10000
	}
	return c.MaxBatchSize
}

func (c AcceptanceCapabilities) maxRecordBytes() int {
	const (
		defSize = 1 << 20
		maxSize = 3 << 20
	)
	switch {
	case c.MaxRecordBytes <= 0:
		return defSize
	case c.MaxRecordBytes > maxSize:
		return maxSize
	}
	return c.MaxRecordBytes
}

func (c AcceptanceCapabilities) throughputDuration() time.Duration {
	if c.ThroughputDuration <= 0 {
		return 3 * time.Second
	}
	return c.ThroughputDuration
}

// AcceptanceCapabilitiesProvider is optionally implemented by an
// [AcceptanceTestDriver] to declare the connector's capabilities.
// [ConfigurableAcceptanceTestDriver] implements it using
// [ConfigurableAcceptanceTestDriverConfig.Capabilities]. [AcceptanceTest] (v1)
// ignores it.
type AcceptanceCapabilitiesProvider interface {
	AcceptanceCapabilities() AcceptanceCapabilities
}

// AcceptanceReadAllDriver is optionally implemented by an
// [AcceptanceTestDriver] that can return everything currently stored in the
// destination, duplicates included. The v2 replay tests need it to tell
// at-least-once (duplicates are fine) from lost records.
// [ConfigurableAcceptanceTestDriver] implements it by reading the whole source
// until it goes quiet. Drivers that do not implement it fall back to
// [AcceptanceTestDriver.ReadFromDestination], which makes the replay tests
// stricter (they then cannot tolerate duplicates).
type AcceptanceReadAllDriver interface {
	ReadAllFromDestination(*testing.T) []opencdc.Record
}

// AcceptanceKeyedRecordGenerator is optionally implemented by an
// [AcceptanceTestDriver] to produce records with a given key, for the ordering
// tests. Without it the suite takes a generated record and replaces its key
// with raw data.
type AcceptanceKeyedRecordGenerator interface {
	GenerateRecordWithKey(t *testing.T, op opencdc.Operation, key string) opencdc.Record
}

// AcceptanceLargeRecordGenerator is optionally implemented by an
// [AcceptanceTestDriver] to produce a record with a payload of roughly the
// given size, for connectors where padding a generated record would not fit
// the target (e.g. a fixed table schema).
type AcceptanceLargeRecordGenerator interface {
	GenerateLargeRecord(t *testing.T, op opencdc.Operation, size int) opencdc.Record
}

// AcceptanceTestV2 runs version 2 of the acceptance suite. The v1 specification
// and configuration tests run unchanged as the subtest "Core"; the v1 data path
// tests are replaced by the v2 cases, which cover the same ground through the
// plugin interface and with acks (see skipCore for why). The v2 cases run as the
// subtest "V2". A connector has passed suite v2 only if both groups pass.
// Existing connectors keep calling [AcceptanceTest]; nothing changes for them.
//
// The v2 tests drive the connector the way Conduit does, through the plugin
// interface with a pipelined, asynchronously acknowledged stream, so that the
// SDK's batching and ack handling are exercised together with the connector.
// Tests that only apply to connectors with a certain capability run only if the
// driver declares it (see [AcceptanceCapabilities]).
//
//	func TestAcceptanceV2(t *testing.T) {
//	    sdk.AcceptanceTestV2(t, sdk.ConfigurableAcceptanceTestDriver{
//	        Config: sdk.ConfigurableAcceptanceTestDriverConfig{
//	            Connector:         myConnector,
//	            SourceConfig:      config.Config{...},
//	            DestinationConfig: config.Config{...},
//	            Capabilities: sdk.AcceptanceCapabilities{
//	                Destination: sdk.DestinationCapabilities{IdempotentWrites: true},
//	            },
//	        },
//	    })
//	}
//
// Set the environment variable [AcceptanceReportEnv] to a file path to get a
// JSON [AcceptanceReport] of the run.
func AcceptanceTestV2(t *testing.T, driver AcceptanceTestDriver) {
	caps := AcceptanceCapabilities{}
	if p, ok := driver.(AcceptanceCapabilitiesProvider); ok {
		caps = p.AcceptanceCapabilities()
	}

	rep := newAcceptanceReport(driver, caps)
	t.Cleanup(func() { rep.finish(t) })

	t.Run("Core", func(t *testing.T) {
		acceptanceTest{driver: driver, skip: caps.skipCore, onResult: rep.addCase}.Test(t)
	})

	runAcceptanceV2(t, driver, caps, rep)
}

func runAcceptanceV2(t *testing.T, driver AcceptanceTestDriver, caps AcceptanceCapabilities, rep *AcceptanceReport) {
	v2 := &acceptanceV2{
		acceptanceTest: acceptanceTest{driver: driver},
		caps:           caps,
		report:         rep,
	}
	t.Run("V2", v2.run)
}

// AcceptanceReport is the machine-readable result of [AcceptanceTestV2].
type AcceptanceReport struct {
	// SuiteVersion is the major version of the suite (2).
	SuiteVersion int `json:"suite_version"`
	// SuiteRevision is the full revision, see [AcceptanceSuiteV2Revision].
	SuiteRevision string `json:"suite_revision"`
	// SDKVersion is the version of the SDK module the connector was tested
	// with, if it can be determined.
	SDKVersion string `json:"sdk_version,omitempty"`
	// Connector identifies the connector under test.
	Connector struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"connector"`
	// Capabilities are the claims the connector made.
	Capabilities AcceptanceCapabilities `json:"capabilities"`
	// Passed is true if no test failed. Skipped tests do not fail the run.
	Passed bool `json:"passed"`
	// Cases lists every v2 test with its outcome.
	Cases []AcceptanceCaseResult `json:"cases"`
	// Metrics are recorded measurements. They are never pass/fail criteria.
	Metrics []AcceptanceMetric `json:"metrics,omitempty"`
	// StartedAt is when the run started.
	StartedAt time.Time `json:"started_at"`
	// DurationSeconds is the wall-clock duration of the run.
	DurationSeconds float64 `json:"duration_seconds"`

	mu sync.Mutex
}

// Outcomes of an [AcceptanceCaseResult].
const (
	AcceptanceOutcomePassed  = "passed"
	AcceptanceOutcomeFailed  = "failed"
	AcceptanceOutcomeSkipped = "skipped"
)

// AcceptanceCaseResult is the outcome of one v2 test.
type AcceptanceCaseResult struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Outcome string `json:"outcome"` // one of the AcceptanceOutcome constants
	// Reason says why a test was skipped, e.g. which capability was not
	// claimed.
	Reason          string  `json:"reason,omitempty"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// AcceptanceMetric is one recorded measurement.
type AcceptanceMetric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
	// Detail documents the conditions of the measurement.
	Detail string `json:"detail,omitempty"`
}

func newAcceptanceReport(driver AcceptanceTestDriver, caps AcceptanceCapabilities) *AcceptanceReport {
	r := &AcceptanceReport{
		SuiteVersion:  2,
		SuiteRevision: AcceptanceSuiteV2Revision,
		SDKVersion:    sdkModuleVersion(),
		Capabilities:  caps,
		StartedAt:     time.Now().UTC(),
	}
	if c := driver.Connector(); c.NewSpecification != nil {
		spec := c.NewSpecification()
		r.Connector.Name = spec.Name
		r.Connector.Version = spec.Version
	}
	return r
}

func (r *AcceptanceReport) addCase(c AcceptanceCaseResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Cases = append(r.Cases, c)
}

func (r *AcceptanceReport) addMetric(m AcceptanceMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Metrics = append(r.Metrics, m)
}

func (r *AcceptanceReport) finish(t *testing.T) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.DurationSeconds = time.Since(r.StartedAt).Seconds()
	r.Passed = !t.Failed()

	path := os.Getenv(AcceptanceReportEnv)
	if path == "" {
		return
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Errorf("failed to marshal acceptance report: %v", err)
		return
	}
	//nolint:gosec,nolintlint // the path is chosen by whoever runs the tests
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		t.Errorf("failed to write acceptance report to %q: %v", path, err)
	}
}

const sdkModulePath = "github.com/conduitio/conduit-connector-sdk"

func sdkModuleVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	if bi.Main.Path == sdkModulePath {
		return "(devel)"
	}
	for _, d := range bi.Deps {
		if d.Path == sdkModulePath {
			if d.Replace != nil {
				return fmt.Sprintf("%s (replaced by %s)", d.Version, d.Replace.Path)
			}
			return d.Version
		}
	}
	return ""
}

// v2Case is one test in the v2 suite.
type v2Case struct {
	// id is stable and never reused. It is what reports, registry entries and
	// the language-neutral conformance suite refer to.
	id   string
	name string
	// needs returns a reason if the test does not apply to this connector, or
	// the empty string if it does.
	needs func(a *acceptanceV2) string
	fn    func(a *acceptanceV2, t *testing.T)
}

type acceptanceV2 struct {
	acceptanceTest
	caps   AcceptanceCapabilities
	report *AcceptanceReport
}

func (a *acceptanceV2) hasSource() string {
	if a.driver.Connector().NewSource == nil {
		return "connector has no source"
	}
	return ""
}

// hasSeededSource is the precondition of tests that write expectations into
// the 3rd party system and compare them with what the source returns.
func (a *acceptanceV2) hasSeededSource() string {
	if r := a.hasSource(); r != "" {
		return r
	}
	if a.caps.Source.Synthetic {
		return "source declared Synthetic, it cannot be seeded"
	}
	return ""
}

func (a *acceptanceV2) hasDestination() string {
	if a.driver.Connector().NewDestination == nil {
		return "connector has no destination"
	}
	return ""
}

// hasReadBack is the precondition of tests that inspect what the destination
// stored.
func (a *acceptanceV2) hasReadBack() string {
	if r := a.hasDestination(); r != "" {
		return r
	}
	if a.caps.Destination.WriteOnly {
		return "destination declared WriteOnly, stored data cannot be inspected"
	}
	return ""
}

func (a *acceptanceV2) run(t *testing.T) {
	for _, c := range v2Cases() {
		t.Run(c.id+"/"+c.name, func(t *testing.T) {
			start := time.Now()
			res := AcceptanceCaseResult{ID: c.id, Name: c.name}
			defer func() {
				res.DurationSeconds = time.Since(start).Seconds()
				switch {
				case t.Failed():
					res.Outcome = AcceptanceOutcomeFailed
				case t.Skipped():
					res.Outcome = AcceptanceOutcomeSkipped
				default:
					res.Outcome = AcceptanceOutcomePassed
				}
				a.report.addCase(res)
			}()

			if reason := c.needs(a); reason != "" {
				res.Reason = reason
				t.Skip(reason)
			}
			a.driver.BeforeTest(t)
			defer a.driver.AfterTest(t)

			c.fn(a, t)
		})
	}
}

// IDs of the v2 cases.
const (
	caseDst01 = "DST-01"
	caseDst02 = "DST-02"
	caseDst03 = "DST-03"
	caseDst04 = "DST-04"
	caseDst05 = "DST-05"
	caseDst06 = "DST-06"
	caseDst07 = "DST-07"
	caseDst08 = "DST-08"
	caseDst09 = "DST-09"
	caseDst10 = "DST-10"
	caseDst11 = "DST-11"
	caseSrc01 = "SRC-01"
	caseSrc02 = "SRC-02"
	caseSrc03 = "SRC-03"
	caseSrc04 = "SRC-04"
	caseSrc05 = "SRC-05"
	caseSrc06 = "SRC-06"
	caseSrc07 = "SRC-07"
	caseSrc08 = "SRC-08"
	caseSrc09 = "SRC-09"
)

// v2Cases lists all v2 tests in execution order. The IDs are a public
// contract: never renumber, never reuse.
func v2Cases() []v2Case {
	dst := func(a *acceptanceV2) string { return a.hasDestination() }
	src := func(a *acceptanceV2) string { return a.hasSource() }
	seeded := func(a *acceptanceV2) string { return a.hasSeededSource() }
	dstRead := func(a *acceptanceV2) string { return a.hasReadBack() }
	claim := func(ok bool, what string) string {
		if ok {
			return ""
		}
		return "capability not claimed: " + what
	}
	all := func(fs ...func(*acceptanceV2) string) func(*acceptanceV2) string {
		return func(a *acceptanceV2) string {
			for _, f := range fs {
				if r := f(a); r != "" {
					return r
				}
			}
			return ""
		}
	}

	return []v2Case{
		// Destination
		{caseDst01, "pipelined_writes", dst, (*acceptanceV2).testDstPipelinedWrites},
		{caseDst02, "slow_acks", dst, (*acceptanceV2).testDstSlowAcks},
		{caseDst03, "mixed_batch_sizes", dst, (*acceptanceV2).testDstBatchSizes},
		{caseDst04, "crash_replay_at_least_once", dstRead, (*acceptanceV2).testDstCrashReplay},
		{caseDst05, "crash_replay_idempotent", all(dstRead, func(a *acceptanceV2) string {
			return claim(a.caps.Destination.IdempotentWrites, "Destination.IdempotentWrites")
		}), (*acceptanceV2).testDstCrashReplayIdempotent},
		{caseDst06, "ordering_per_key", all(dstRead, func(a *acceptanceV2) string {
			return claim(a.caps.Destination.OrdersByKey, "Destination.OrdersByKey")
		}), (*acceptanceV2).testDstOrdering},
		{caseDst07, "schema_evolution", all(dst, func(a *acceptanceV2) string {
			return claim(a.caps.Destination.SchemaEvolution != SchemaEvolutionNone, "Destination.SchemaEvolution")
		}), (*acceptanceV2).testDstSchemaEvolution},
		{caseDst08, "large_record", dst, (*acceptanceV2).testDstLargeRecord},
		{caseDst09, "graceful_stop_drains_in_flight", dst, (*acceptanceV2).testDstGracefulStop},
		{caseDst10, "context_cancellation", dst, (*acceptanceV2).testDstContextCancel},
		{caseDst11, "throughput_smoke", dst, (*acceptanceV2).testDstThroughput},
		// Source
		{caseSrc01, "pipelined_and_lagging_acks", seeded, (*acceptanceV2).testSrcLaggingAcks},
		{caseSrc02, "mixed_batch_sizes", seeded, (*acceptanceV2).testSrcBatchSizes},
		{caseSrc03, "crash_resume_no_gap", seeded, (*acceptanceV2).testSrcCrashResume},
		{caseSrc04, "ordering_per_key", all(seeded, func(a *acceptanceV2) string {
			return claim(a.caps.Source.OrdersByKey, "Source.OrdersByKey")
		}), (*acceptanceV2).testSrcOrdering},
		{caseSrc05, "schema_evolution", all(seeded, func(a *acceptanceV2) string {
			return claim(a.caps.Source.SchemaEvolution == SchemaEvolutionEvolve, "Source.SchemaEvolution=evolve")
		}), (*acceptanceV2).testSrcSchemaEvolution},
		{caseSrc06, "large_record", seeded, (*acceptanceV2).testSrcLargeRecord},
		{caseSrc07, "graceful_stop_with_unacked", src, (*acceptanceV2).testSrcGracefulStop},
		{caseSrc08, "context_cancellation", src, (*acceptanceV2).testSrcContextCancel},
		{caseSrc09, "throughput_smoke", src, (*acceptanceV2).testSrcThroughput},
	}
}

func (a *acceptanceV2) metric(t *testing.T, name string, value float64, unit, detail string) {
	t.Helper()
	t.Logf("metric %s = %.1f %s (%s)", name, value, unit, detail)
	a.report.addMetric(AcceptanceMetric{Name: name, Value: value, Unit: unit, Detail: detail})
}

// v2ID returns the identity under which a record is tracked across a round
// trip through a 3rd party system: the payload bytes. Positions are rewritten
// by connectors and metadata is optional, so neither can be used.
func v2ID(r opencdc.Record) string {
	return string(r.Payload.After.Bytes())
}

// v2WholeID is the identity for destinations that store the whole record
// serialized as the payload (e.g. file), see acceptanceTest.isEqualRecords.
func v2WholeID(r opencdc.Record) string {
	return string(r.Bytes())
}

// v2Reconcile picks how the records read back map to the records written, and
// returns the identity function to apply to the written ones. got records
// are always identified by their payload bytes.
func v2Reconcile(want, got []opencdc.Record) (wantID func(opencdc.Record) string) {
	gotSet := make(map[string]struct{}, len(got))
	for _, r := range got {
		gotSet[v2ID(r)] = struct{}{}
	}
	var payloadHits, wholeHits int
	for _, r := range want {
		if _, ok := gotSet[v2ID(r)]; ok {
			payloadHits++
		}
		if _, ok := gotSet[v2WholeID(r)]; ok {
			wholeHits++
		}
	}
	if wholeHits > payloadHits {
		return v2WholeID
	}
	return v2ID
}

// v2Diff compares what was written with what is stored and returns the
// identities that are missing and the number of duplicates.
func v2Diff(want, got []opencdc.Record) (missing []string, duplicates int, unexpected int) {
	wantID := v2Reconcile(want, got)

	counts := make(map[string]int, len(got))
	for _, r := range got {
		counts[v2ID(r)]++
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, r := range want {
		id := wantID(r)
		wantSet[id] = struct{}{}
		if counts[id] == 0 {
			missing = append(missing, short(id))
		}
	}
	for id, n := range counts {
		if _, ok := wantSet[id]; !ok {
			unexpected += n
			continue
		}
		duplicates += n - 1
	}
	return missing, duplicates, unexpected
}

func short(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 60 {
		return s[:57] + "..."
	}
	return s
}
