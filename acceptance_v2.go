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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"sort"
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

// IdempotencyClaim is what a destination states about writing the same record
// twice. It is a recorded value rather than a flag so that "not idempotent" is
// a statement a registry can read, not the absence of one.
type IdempotencyClaim string

const (
	// IdempotenceUnspecified means the connector said nothing. The baseline
	// cases run (at-least-once), a registry should not certify the connector.
	IdempotenceUnspecified IdempotencyClaim = ""
	// IdempotenceYes claims that writing the same records again (same position
	// and key) after a crash does not create duplicates. DST-05 verifies it.
	IdempotenceYes IdempotencyClaim = "idempotent"
	// IdempotenceNo declares that replayed records may be stored twice
	// (at-least-once). DST-04 verifies that nothing is lost.
	IdempotenceNo IdempotencyClaim = "at_least_once"
)

// Limits a registry can require for a "certified" result. The suite runs at the
// declared values (a connector with a hard limit still gets tested), and the
// report says whether the floors are met.
const (
	// CertificationMinBatchSize is the smallest MaxBatchSize that counts for
	// certification: the default.
	CertificationMinBatchSize = 10000
	// CertificationMinRecordBytes is the smallest MaxRecordBytes that counts
	// for certification: the default.
	CertificationMinRecordBytes = 1 << 20
)

// DefaultMaxRedelivery is how many already acked records a source may deliver
// again after a restart when it does not claim ExactResume.
const DefaultMaxRedelivery = 10

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
	// around. The report states whether [CertificationMinBatchSize] is met.
	MaxBatchSize int `json:"max_batch_size,omitempty"`
	// MaxRecordBytes is the size of the record used in the large record tests.
	// The default is 1 MiB. Values above 3 MiB are capped, because the default
	// maximum gRPC message size for connector plugins is 4 MiB. The report
	// states whether [CertificationMinRecordBytes] is met.
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
	// ExactResume claims that after a restart from the last acked position the
	// source delivers no record that was already acked. Without the claim the
	// suite tolerates [AcceptanceCapabilities.Source.MaxRedelivery] repeated
	// records per restart (siblings of the last position, a coarse watermark),
	// but a source that restarts from the beginning fails either way.
	ExactResume bool `json:"exact_resume,omitempty"`
	// MaxRedelivery is the number of already acked records a source without
	// ExactResume may deliver again per restart. Zero means
	// [DefaultMaxRedelivery]. It is ignored if ExactResume is claimed.
	MaxRedelivery int `json:"max_redelivery,omitempty"`
	// Synthetic declares that the source produces records on its own (a
	// generator) and cannot be fed through the 3rd party system. Tests that
	// compare what was written with what was read are skipped, the tests that
	// only need records to flow (graceful stop, cancellation, throughput) run.
	// This removes most of the source checks, so a registry must treat it as a
	// per-connector exception that maintainers approve and list.
	Synthetic bool `json:"synthetic,omitempty"`
	// SchemaEvolution is the policy for fields that appear while the source
	// is running. Only [SchemaEvolutionEvolve] can be verified on a source.
	SchemaEvolution SchemaEvolutionPolicy `json:"schema_evolution,omitempty"`
}

// DestinationCapabilities are the optional claims of a destination connector.
type DestinationCapabilities struct {
	// Idempotence states whether replaying records creates duplicates, see
	// [IdempotencyClaim]. A claim of [IdempotenceYes] needs a driver that can
	// return duplicates (see [AcceptanceReadAllDriver]); without one the case
	// fails instead of silently checking less.
	Idempotence IdempotencyClaim `json:"idempotence,omitempty"`
	// OrdersByKey claims that records with the same key are stored in the order
	// they were written.
	OrdersByKey bool `json:"orders_by_key,omitempty"`
	// SchemaEvolution is the policy for records whose structure changes
	// while the destination is running.
	SchemaEvolution SchemaEvolutionPolicy `json:"schema_evolution,omitempty"`
	// WriteOnly declares that the destination cannot be read back (for
	// example a log or a webhook). Tests that only need acks still run, tests
	// that need to inspect the stored data are skipped and say so in the
	// report. This removes the durability checks, so a registry must treat it
	// as a per-connector exception that maintainers approve and list; a driver
	// can often avoid it by capturing the destination's output itself.
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

func (c AcceptanceCapabilities) maxRedelivery() int {
	switch {
	case c.Source.ExactResume:
		return 0
	case c.Source.MaxRedelivery > 0:
		return c.Source.MaxRedelivery
	}
	return DefaultMaxRedelivery
}

// belowFloor lists the declared limits that are under the certification floors.
func (c AcceptanceCapabilities) belowFloor() []string {
	var out []string
	if c.maxBatchSize() < CertificationMinBatchSize {
		out = append(out, "max_batch_size")
	}
	if c.maxRecordBytes() < CertificationMinRecordBytes {
		out = append(out, "max_record_bytes")
	}
	return out
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
// destination, duplicates included. The replay cases need it to tell
// at-least-once (duplicates are fine) from lost records, and the idempotence
// claim cannot be verified without it. It is an explicit opt-in: either
// implement this method on your driver, or set
// [ConfigurableAcceptanceTestDriverConfig.ReadAllFromSource] to read the whole
// source. [ConfigurableAcceptanceTestDriver] does not satisfy this interface by
// itself, so a driver that embeds it and overrides ReadFromDestination is never
// silently read through the source instead. Without either, the suite falls
// back to [AcceptanceTestDriver.ReadFromDestination], which cannot see
// duplicates.
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
//	                Destination: sdk.DestinationCapabilities{Idempotence: sdk.IdempotenceYes},
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
		acceptanceTest{
			driver:   driver,
			skip:     caps.skipCore,
			onResult: func(r AcceptanceCaseResult) { rep.addCase(classifyCoreResult(driver, r)) },
		}.Test(t)
	})

	runAcceptanceV2(t, driver, caps, rep)
}

func runAcceptanceV2(t *testing.T, driver AcceptanceTestDriver, caps AcceptanceCapabilities, rep *AcceptanceReport) {
	v2 := &acceptanceV2{
		acceptanceTest: acceptanceTest{driver: driver},
		caps:           caps,
		report:         rep,
		skipReasons:    map[string]string{},
	}
	t.Run("V2", v2.run)
}

// ReportSchemaVersion is the version of the JSON layout of [AcceptanceReport].
const ReportSchemaVersion = 1

// AcceptanceReport is the machine-readable result of [AcceptanceTestV2].
//
// The report is evidence that a run happened, not proof: it is written by the
// process that ran the tests. A registry that wants an authenticated result
// re-runs the suite in CI it controls and uses this layout to compare the
// outcome with the expected case set ([AcceptanceReport.ExpectedCaseIDs]).
type AcceptanceReport struct {
	// ReportSchemaVersion is the version of this layout.
	ReportSchemaVersion int `json:"report_schema_version"`
	// SuiteVersion is the major version of the suite (2).
	SuiteVersion int `json:"suite_version"`
	// SuiteRevision is the full revision, see [AcceptanceSuiteV2Revision]. The
	// revision together with CaseSetHash identifies exactly which cases a
	// connector had to pass.
	SuiteRevision string `json:"suite_revision"`
	// SDKVersion is the version of the SDK module linked into the test binary,
	// read from the binary's build information. It is "unknown" when the
	// build information has no entry for the SDK (it is stripped, or the SDK
	// is the main module), and "(devel)" for a replaced module. It is
	// informational: compatibility is decided by SuiteRevision and CaseSetHash.
	SDKVersion string `json:"sdk_version"`
	// Connector identifies the connector under test.
	Connector struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"connector"`
	// Capabilities are the claims the connector made.
	Capabilities AcceptanceCapabilities `json:"capabilities"`
	// Limits are the batch and record sizes the suite used, and whether they
	// meet the certification floors.
	Limits struct {
		MaxBatchSize          int      `json:"max_batch_size"`
		MaxRecordBytes        int      `json:"max_record_bytes"`
		CertificationFloorMet bool     `json:"certification_floor_met"`
		BelowFloor            []string `json:"below_floor,omitempty"`
	} `json:"limits"`
	// Goleak records what the driver told the goroutine leak check to ignore.
	// Names are only available for ignores declared through
	// [ConfigurableAcceptanceTestDriverConfig.GoleakIgnoreFunctions];
	// OtherOptions counts opaque goleak options, which cannot be inspected.
	Goleak struct {
		IgnoredFunctions []string `json:"ignored_functions,omitempty"`
		OtherOptions     int      `json:"other_options"`
	} `json:"goleak"`
	// ExpectedCaseIDs is every case that must have a result for the run to be
	// complete, in execution order.
	ExpectedCaseIDs []string `json:"expected_case_ids"`
	// ExpectedCaseCount is len(ExpectedCaseIDs).
	ExpectedCaseCount int `json:"expected_case_count"`
	// CaseSetHash is the SHA-256 of the suite revision and the sorted expected
	// IDs.
	CaseSetHash string `json:"case_set_hash"`
	// Complete is true if every expected case has a result. A run filtered with
	// -run is never complete.
	Complete bool `json:"complete"`
	// UnexplainedSkips counts skips that are not explained by a structural
	// reason, an unclaimed capability, or a declaration. A skip by the driver
	// or inside a case counts.
	UnexplainedSkips int `json:"unexplained_skips"`
	// Passed is true if no test failed, the run is complete and there are no
	// unexplained skips.
	Passed bool `json:"passed"`
	// Cases lists every case with its outcome.
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

// Kinds of skip, see [AcceptanceCaseResult.SkipKind].
const (
	// SkipNotApplicable: the connector has no such side (no source, no destination).
	SkipNotApplicable = "not_applicable"
	// SkipUnclaimed: the case needs a capability the connector did not claim.
	SkipUnclaimed = "unclaimed"
	// SkipDeclared: the connector declared Synthetic or WriteOnly.
	SkipDeclared = "declared"
	// SkipReplaced: a v1 data path test that suite v2 replaces with a v2 case.
	SkipReplaced = "replaced"
	// SkipInCase: the case skipped itself. Counted as unexplained.
	SkipInCase = "in_case"
	// SkipDriver: skipped before the case ran, by the driver's BeforeTest
	// (for example a Skip regular expression). The reason is not known to the
	// suite. Counted as unexplained.
	SkipDriver = "driver"
)

// AcceptanceCaseResult is the outcome of one case.
type AcceptanceCaseResult struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Outcome string `json:"outcome"` // one of the AcceptanceOutcome constants
	// SkipKind classifies a skip, see the Skip constants.
	SkipKind string `json:"skip_kind,omitempty"`
	// Reason says why a test was skipped, e.g. which capability was not
	// claimed.
	Reason          string  `json:"reason,omitempty"`
	DurationSeconds float64 `json:"duration_seconds"`
}

func (c AcceptanceCaseResult) unexplainedSkip() bool {
	return c.Outcome == AcceptanceOutcomeSkipped && (c.SkipKind == SkipInCase || c.SkipKind == SkipDriver || c.SkipKind == "")
}

// AcceptanceMetric is one recorded measurement.
type AcceptanceMetric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit"`
	// Detail documents the conditions of the measurement.
	Detail string `json:"detail,omitempty"`
}

// coreTestIDs maps the v1 test methods that make up the core group to stable
// IDs. The IDs are a public contract like the v2 case IDs: new v1 tests get new
// IDs, none are renumbered. A v1 test without an entry fails the run, so a test
// added to v1 cannot be silently missing from reports.
var coreTestIDs = map[string]string{
	"TestSpecifier_Exists":                     "CORE-01",
	"TestSpecifier_Specify_Success":            "CORE-02",
	"TestSource_Parameters_Success":            "CORE-03",
	"TestSource_Configure_Success":             "CORE-04",
	"TestSource_Configure_RequiredParams":      "CORE-05",
	"TestDestination_Parameters_Success":       "CORE-06",
	"TestDestination_Configure_Success":        "CORE-07",
	"TestDestination_Configure_RequiredParams": "CORE-08",
	"TestSource_Read_Success":                  "CORE-09",
	"TestSource_Read_Timeout":                  "CORE-10",
	"TestSource_Open_ResumeAtPositionSnapshot": "CORE-11",
	"TestSource_Open_ResumeAtPositionCDC":      "CORE-12",
	"TestDestination_Write_Success":            "CORE-13",
}

// expectedCaseIDs returns the IDs every complete run has results for.
func expectedCaseIDs() []string {
	ids := make([]string, 0, len(coreTestIDs)+len(v2Cases()))
	for i := 1; i <= len(coreTestIDs); i++ {
		ids = append(ids, fmt.Sprintf("CORE-%02d", i))
	}
	for _, c := range v2Cases() {
		ids = append(ids, c.id)
	}
	return ids
}

func caseSetHash(ids []string) string {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(AcceptanceSuiteV2Revision + "\n" + strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])
}

// goleakReporter is implemented by [ConfigurableAcceptanceTestDriver].
type goleakReporter interface {
	GoleakIgnoredFunctions() []string
}

func newAcceptanceReport(driver AcceptanceTestDriver, caps AcceptanceCapabilities) *AcceptanceReport {
	r := &AcceptanceReport{
		ReportSchemaVersion: ReportSchemaVersion,
		SuiteVersion:        2,
		SuiteRevision:       AcceptanceSuiteV2Revision,
		SDKVersion:          sdkModuleVersion(),
		Capabilities:        caps,
		StartedAt:           time.Now().UTC(),
	}
	r.Limits.MaxBatchSize = caps.maxBatchSize()
	r.Limits.MaxRecordBytes = caps.maxRecordBytes()
	r.Limits.BelowFloor = caps.belowFloor()
	r.Limits.CertificationFloorMet = len(r.Limits.BelowFloor) == 0
	r.ExpectedCaseIDs = expectedCaseIDs()
	r.ExpectedCaseCount = len(r.ExpectedCaseIDs)
	r.CaseSetHash = caseSetHash(r.ExpectedCaseIDs)
	if c := driver.Connector(); c.NewSpecification != nil {
		spec := c.NewSpecification()
		r.Connector.Name = spec.Name
		r.Connector.Version = spec.Version
	}
	if g, ok := driver.(goleakReporter); ok {
		r.Goleak.IgnoredFunctions = g.GoleakIgnoredFunctions()
	}
	// goleak.Option is opaque; count what the driver passes beyond the names
	// we know about. A nil *testing.T is fine for the drivers we know, a
	// custom driver may use it, so don't risk it: only Configurable is counted.
	if c, ok := driver.(interface{ GoleakOtherOptionCount() int }); ok {
		r.Goleak.OtherOptions = c.GoleakOtherOptionCount()
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

	have := make(map[string]bool, len(r.Cases))
	r.UnexplainedSkips = 0
	for _, c := range r.Cases {
		have[c.ID] = true
		if c.unexplainedSkip() {
			r.UnexplainedSkips++
		}
	}
	r.Complete = true
	for _, id := range r.ExpectedCaseIDs {
		if !have[id] {
			r.Complete = false
			break
		}
	}
	r.Passed = !t.Failed() && r.Complete && r.UnexplainedSkips == 0

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

// sdkModuleVersion reads the SDK version from the build information of the
// running test binary. That information is present in binaries built by
// `go test` (it is embedded by the linker, no -ldflags needed) and lists the
// SDK as a dependency of the connector's module. It is "unknown" if it is
// missing, which happens when the SDK is the main module (the SDK's own tests).
func sdkModuleVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, d := range bi.Deps {
		if d.Path == sdkModulePath {
			if d.Replace != nil {
				return fmt.Sprintf("%s (replaced by %s)", d.Version, d.Replace.Path)
			}
			return d.Version
		}
	}
	if bi.Main.Path == sdkModulePath {
		return "(devel)"
	}
	return "unknown"
}

// classifyCoreResult sets the skip kind of a v1 test result. The skip reasons
// of v1 tests are not visible from outside (testing does not expose them), so
// the kind is derived: a replaced test carries its reason already, a test of a
// side the connector does not implement is not applicable, anything else was
// skipped by the driver.
func classifyCoreResult(driver AcceptanceTestDriver, r AcceptanceCaseResult) AcceptanceCaseResult {
	if r.Outcome != AcceptanceOutcomeSkipped || r.SkipKind != "" {
		return r
	}
	conn := driver.Connector()
	switch {
	case strings.Contains(r.Name, "Source") && conn.NewSource == nil:
		r.SkipKind, r.Reason = SkipNotApplicable, "connector has no source"
	case strings.Contains(r.Name, "Destination") && conn.NewDestination == nil:
		r.SkipKind, r.Reason = SkipNotApplicable, "connector has no destination"
	case strings.Contains(r.Name, "Specif") && conn.NewSpecification == nil:
		r.SkipKind, r.Reason = SkipNotApplicable, "connector has no specification"
	default:
		r.SkipKind, r.Reason = SkipDriver, "skipped by the driver (BeforeTest); reason not available"
	}
	return r
}

// v2Applicability says whether a case applies to a connector. The zero value
// means it does.
type v2Applicability struct {
	kind   string
	reason string
}

func (v v2Applicability) applies() bool { return v.kind == "" }

// v2Case is one test in the v2 suite.
type v2Case struct {
	// id is stable and never reused. It is what reports, registry entries and
	// the language-neutral conformance suite refer to.
	id   string
	name string
	// needs says whether the case applies to this connector.
	needs func(a *acceptanceV2) v2Applicability
	// claimed returns the name of the capability whose claim this case
	// verifies, if the connector made it. A claimed capability that cannot be
	// verified is a failure, not a skip. Nil for cases without a claim.
	claimed func(a *acceptanceV2) string
	fn      func(a *acceptanceV2, t *testing.T)
}

type acceptanceV2 struct {
	acceptanceTest
	caps   AcceptanceCapabilities
	report *AcceptanceReport

	mu          sync.Mutex
	skipReasons map[string]string
}

// skipInCase skips the running case and records why. Do not use it for a case
// whose capability was claimed; fail instead.
func (a *acceptanceV2) skipInCase(t *testing.T, reason string) {
	t.Helper()
	a.mu.Lock()
	a.skipReasons[t.Name()] = reason
	a.mu.Unlock()
	t.Skip(reason)
}

func (a *acceptanceV2) takeSkipReason(name string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.skipReasons[name]
}

func notApplicable(reason string) v2Applicability {
	return v2Applicability{kind: SkipNotApplicable, reason: reason}
}

func (a *acceptanceV2) hasSource() v2Applicability {
	if a.driver.Connector().NewSource == nil {
		return notApplicable("connector has no source")
	}
	return v2Applicability{}
}

// hasSeededSource is the precondition of tests that write expectations into
// the 3rd party system and compare them with what the source returns.
func (a *acceptanceV2) hasSeededSource() v2Applicability {
	if r := a.hasSource(); !r.applies() {
		return r
	}
	if a.caps.Source.Synthetic {
		return v2Applicability{kind: SkipDeclared, reason: "source declared Synthetic, it cannot be seeded"}
	}
	return v2Applicability{}
}

func (a *acceptanceV2) hasDestination() v2Applicability {
	if a.driver.Connector().NewDestination == nil {
		return notApplicable("connector has no destination")
	}
	return v2Applicability{}
}

// hasReadBack is the precondition of tests that inspect what the destination
// stored.
func (a *acceptanceV2) hasReadBack() v2Applicability {
	if r := a.hasDestination(); !r.applies() {
		return r
	}
	if a.caps.Destination.WriteOnly {
		return v2Applicability{kind: SkipDeclared, reason: "destination declared WriteOnly, stored data cannot be inspected"}
	}
	return v2Applicability{}
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
					if res.SkipKind == "" {
						if reason := a.takeSkipReason(t.Name()); reason != "" {
							res.SkipKind, res.Reason = SkipInCase, reason
						} else {
							res.SkipKind, res.Reason = SkipDriver, "skipped by the driver (BeforeTest); reason not available"
						}
					}
				default:
					res.Outcome = AcceptanceOutcomePassed
				}
				a.report.addCase(res)
			}()

			app := c.needs(a)
			if c.claimed != nil {
				if what := c.claimed(a); what != "" && !app.applies() {
					t.Fatalf("the connector claims %s but the case cannot verify it: %s", what, app.reason)
				}
			}
			if !app.applies() {
				res.SkipKind, res.Reason = app.kind, app.reason
				t.Skip(app.reason)
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
	dst := (*acceptanceV2).hasDestination
	src := (*acceptanceV2).hasSource
	seeded := (*acceptanceV2).hasSeededSource
	dstRead := (*acceptanceV2).hasReadBack
	unclaimed := func(ok bool, what string) v2Applicability {
		if ok {
			return v2Applicability{}
		}
		return v2Applicability{kind: SkipUnclaimed, reason: "capability not claimed: " + what}
	}
	all := func(fs ...func(*acceptanceV2) v2Applicability) func(*acceptanceV2) v2Applicability {
		return func(a *acceptanceV2) v2Applicability {
			for _, f := range fs {
				if r := f(a); !r.applies() {
					return r
				}
			}
			return v2Applicability{}
		}
	}
	idem := func(a *acceptanceV2) bool { return a.caps.Destination.Idempotence == IdempotenceYes }
	dstOrder := func(a *acceptanceV2) bool { return a.caps.Destination.OrdersByKey }
	dstEvo := func(a *acceptanceV2) bool { return a.caps.Destination.SchemaEvolution != SchemaEvolutionNone }
	srcOrder := func(a *acceptanceV2) bool { return a.caps.Source.OrdersByKey }
	srcEvo := func(a *acceptanceV2) bool { return a.caps.Source.SchemaEvolution == SchemaEvolutionEvolve }
	claim := func(f func(*acceptanceV2) bool, what string) func(*acceptanceV2) string {
		return func(a *acceptanceV2) string {
			if f(a) {
				return what
			}
			return ""
		}
	}

	return []v2Case{
		// Destination
		{id: caseDst01, name: "pipelined_writes", needs: dst, fn: (*acceptanceV2).testDstPipelinedWrites},
		{id: caseDst02, name: "slow_acks", needs: dst, fn: (*acceptanceV2).testDstSlowAcks},
		{id: caseDst03, name: "mixed_batch_sizes", needs: dst, fn: (*acceptanceV2).testDstBatchSizes},
		{id: caseDst04, name: "crash_replay_at_least_once", needs: dstRead, fn: (*acceptanceV2).testDstCrashReplay},
		{
			id: caseDst05, name: "crash_replay_idempotent",
			needs: all(dstRead, func(a *acceptanceV2) v2Applicability {
				return unclaimed(idem(a), "Destination.Idempotence=idempotent")
			}),
			claimed: claim(idem, "Destination.Idempotence=idempotent"),
			fn:      (*acceptanceV2).testDstCrashReplayIdempotent,
		},
		{
			id: caseDst06, name: "ordering_per_key",
			needs: all(dstRead, func(a *acceptanceV2) v2Applicability {
				return unclaimed(dstOrder(a), "Destination.OrdersByKey")
			}),
			claimed: claim(dstOrder, "Destination.OrdersByKey"),
			fn:      (*acceptanceV2).testDstOrdering,
		},
		{
			id: caseDst07, name: "schema_evolution",
			needs: all(dst, func(a *acceptanceV2) v2Applicability {
				return unclaimed(dstEvo(a), "Destination.SchemaEvolution")
			}),
			claimed: claim(dstEvo, "Destination.SchemaEvolution"),
			fn:      (*acceptanceV2).testDstSchemaEvolution,
		},
		{id: caseDst08, name: "large_record", needs: dst, fn: (*acceptanceV2).testDstLargeRecord},
		{id: caseDst09, name: "graceful_stop_drains_in_flight", needs: dst, fn: (*acceptanceV2).testDstGracefulStop},
		{id: caseDst10, name: "context_cancellation", needs: dst, fn: (*acceptanceV2).testDstContextCancel},
		{id: caseDst11, name: "throughput_smoke", needs: dst, fn: (*acceptanceV2).testDstThroughput},
		// Source
		{id: caseSrc01, name: "pipelined_and_lagging_acks", needs: seeded, fn: (*acceptanceV2).testSrcLaggingAcks},
		{id: caseSrc02, name: "mixed_batch_sizes", needs: seeded, fn: (*acceptanceV2).testSrcBatchSizes},
		{id: caseSrc03, name: "crash_resume_no_gap", needs: seeded, fn: (*acceptanceV2).testSrcCrashResume},
		{
			id: caseSrc04, name: "ordering_per_key",
			needs: all(seeded, func(a *acceptanceV2) v2Applicability {
				return unclaimed(srcOrder(a), "Source.OrdersByKey")
			}),
			claimed: claim(srcOrder, "Source.OrdersByKey"),
			fn:      (*acceptanceV2).testSrcOrdering,
		},
		{
			id: caseSrc05, name: "schema_evolution",
			needs: all(seeded, func(a *acceptanceV2) v2Applicability {
				return unclaimed(srcEvo(a), "Source.SchemaEvolution=evolve")
			}),
			claimed: claim(srcEvo, "Source.SchemaEvolution=evolve"),
			fn:      (*acceptanceV2).testSrcSchemaEvolution,
		},
		{id: caseSrc06, name: "large_record", needs: seeded, fn: (*acceptanceV2).testSrcLargeRecord},
		{id: caseSrc07, name: "graceful_stop_with_unacked", needs: src, fn: (*acceptanceV2).testSrcGracefulStop},
		{id: caseSrc08, name: "context_cancellation", needs: src, fn: (*acceptanceV2).testSrcContextCancel},
		{id: caseSrc09, name: "throughput_smoke", needs: src, fn: (*acceptanceV2).testSrcThroughput},
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
