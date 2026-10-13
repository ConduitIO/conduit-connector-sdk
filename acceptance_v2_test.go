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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
)

// This file tests the v2 acceptance suite itself. A reference connector backed
// by an in-memory store (acceptance_v2_ref_test.go) must pass it. For each
// class of connector bug there is a mutant of the reference connector, and the
// suite must fail it on the case that is meant to catch it. A suite that passes
// broken connectors is worse than none.
//
// Mutants run in a subprocess (the test binary itself), because a failing
// suite cannot be observed from inside the process that runs it. Each
// subprocess runs only the case that is supposed to catch the mutant.

func refDriver(t *testing.T, caps AcceptanceCapabilities) AcceptanceTestDriver {
	store := strings.ReplaceAll(t.Name(), "/", "_")
	refStores.Delete(store)
	cfg := config.Config{"store": store}
	genType := GenerateStructuredData
	if refClaim("rawdata") {
		genType = GenerateRawData
	}
	return ConfigurableAcceptanceTestDriver{
		Config: ConfigurableAcceptanceTestDriverConfig{
			Connector:         refConnector(),
			SourceConfig:      cfg,
			DestinationConfig: cfg,
			GenerateDataType:  genType,
			BeforeTest:        func(*testing.T) { refStores.Delete(store) },
			ReadTimeout:       300 * time.Millisecond,
			WriteTimeout:      300 * time.Millisecond,
			ReadAllFromSource: !refClaim("noreadall"),
			Capabilities:      caps,
		},
	}
}

// refCapabilities derives the claims from the claims environment variable.
func refCapabilities() AcceptanceCapabilities {
	caps := AcceptanceCapabilities{
		Destination: DestinationCapabilities{
			Idempotence:     IdempotenceNo,
			OrdersByKey:     refClaim("ordering"),
			SchemaEvolution: SchemaEvolutionEvolve,
			WriteOnly:       refClaim("writeonly"),
		},
		Source: SourceCapabilities{
			OrdersByKey:     refClaim("ordering"),
			ExactResume:     refClaim("exact"),
			SchemaEvolution: SchemaEvolutionEvolve,
		},
		ThroughputDuration: 500 * time.Millisecond,
		MaxBatchSize:       2000,
	}
	if refClaim("idempotent") {
		caps.Destination.Idempotence = IdempotenceYes
	}
	if refClaim("redelivery1000") {
		caps.Source.MaxRedelivery = 1000
	}
	if refClaim("redelivery5000") {
		caps.Source.MaxRedelivery = 5000
	}
	if refClaim("reject") {
		caps.Destination.SchemaEvolution = SchemaEvolutionReject
	}
	return caps
}

// TestAcceptanceV2_Reference is the entry point for the mutant subprocesses.
func TestAcceptanceV2_Reference(t *testing.T) {
	AcceptanceTestV2(t, refDriver(t, refCapabilities()))
}

func readReport(t *testing.T, path string) *AcceptanceReport {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("report not written: %v", err)
	}
	var rep AcceptanceReport
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatal(err)
	}
	return &rep
}

func TestAcceptanceV2_ReferencePasses(t *testing.T) {
	t.Setenv(refClaimsEnv, "idempotent,ordering,exact")
	report := filepath.Join(t.TempDir(), "report.json")
	t.Setenv(AcceptanceReportEnv, report)

	// registered first so that it runs after the report was written
	t.Cleanup(func() {
		is := is.New(t)
		rep := readReport(t, report)
		is.Equal(rep.ReportSchemaVersion, ReportSchemaVersion)
		is.Equal(rep.SuiteVersion, 2)
		is.Equal(rep.SuiteRevision, AcceptanceSuiteV2Revision)
		is.True(rep.Finished)
		is.Equal(rep.SDKVersion, "local checkout") // the SDK's own tests run from the checkout
		is.Equal(rep.CaseContentHash, caseContentHash())
		is.Equal(rep.Limits.MaxRedelivery, 0) // ExactResume claimed
		is.True(!rep.Goleak.Unreviewable)
		is.Equal(rep.Connector.Name, "acceptance-v2-reference")
		is.Equal(rep.Capabilities.Destination.Idempotence, IdempotenceYes)
		is.True(rep.Capabilities.Source.ExactResume)
		is.Equal(rep.ExpectedCaseCount, len(rep.ExpectedCaseIDs))
		is.Equal(rep.ExpectedCaseCount, len(coreTestIDs)+len(v2Cases()))
		is.Equal(rep.CaseSetHash, caseSetHash(rep.ExpectedCaseIDs))
		is.Equal(rep.Exceptions, []string{})
		is.True(rep.RunID != "")
		is.Equal(rep.SuiteSourceHash, suiteSourceHash())
		is.True(rep.Complete)
		is.True(rep.Passed)
		is.Equal(rep.UnexplainedSkips, 0)

		is.Equal(rep.Limits.MaxBatchSize, 2000)
		is.True(!rep.Limits.CertificationFloorMet) // the test lowers MaxBatchSize
		is.Equal(rep.Limits.BelowFloor, []string{"max_batch_size"})
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
		is.Equal(core, len(coreTestIDs))
		is.Equal(len(rep.Metrics), 2) // both throughput checks recorded a number
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
		rep := readReport(t, report)
		skipped := map[string]AcceptanceCaseResult{}
		for _, c := range rep.Cases {
			if c.Outcome == AcceptanceOutcomeSkipped && !strings.HasPrefix(c.ID, "CORE-") {
				skipped[c.ID] = c
			}
		}
		for _, id := range []string{"DST-05", "DST-06", "DST-07", "SRC-04", "SRC-05"} {
			is.Equal(skipped[id].SkipKind, SkipUnclaimed) // must skip as unclaimed, with the reason
			is.True(strings.HasPrefix(skipped[id].Reason, "capability not claimed"))
		}
		is.Equal(len(skipped), 5)
		is.True(rep.Complete)
		is.Equal(rep.UnexplainedSkips, 0)
		is.True(rep.Passed) // unclaimed capabilities do not fail a run
		// the replaced v1 tests are skips of their own kind
		for _, c := range rep.Cases {
			if c.ID == "CORE-09" {
				is.Equal(c.SkipKind, SkipReplaced)
			}
		}
	})

	AcceptanceTestV2(t, refDriver(t, AcceptanceCapabilities{ThroughputDuration: 200 * time.Millisecond, MaxBatchSize: 200}))
}

func TestAcceptanceV2_SDKVersionFromPath(t *testing.T) {
	dir := t.TempDir()
	renamed := filepath.Join(dir, "my-fork-of-the-sdk")
	if err := os.MkdirAll(renamed, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(renamed, "go.mod"), []byte("module "+sdkModulePath+"\n\ngo 1.25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "go.mod"), []byte("module example.com/other\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string][2]string{
		"/home/u/go/pkg/mod/github.com/conduitio/conduit-connector-sdk@v0.15.0/acceptance_v2.go": {"v0.15.0", "module_cache_path"},
		"github.com/conduitio/conduit-connector-sdk@v0.15.1-rc.1/acceptance_v2.go":               {"v0.15.1-rc.1", "module_cache_path"}, // -trimpath
		"/app/vendor/github.com/conduitio/conduit-connector-sdk/acceptance_v2.go":                {"vendored", "vendored"},
		filepath.Join(renamed, "acceptance_v2.go"):                                               {"local checkout", "local_checkout"}, // the directory name does not matter
		filepath.Join(other, "acceptance_v2.go"):                                                 {"unknown", "unknown"},
		"/somewhere/else/acceptance_v2.go":                                                       {"unknown", "unknown"},
	} {
		if v, src := sdkVersionFromPath(in); v != want[0] || src != want[1] {
			t.Errorf("sdkVersionFromPath(%q) = %q, %q; want %q, %q", in, v, src, want[0], want[1])
		}
	}
}

// A BeforeTest skip by the driver must not produce a passing report.
func TestAcceptanceV2_DriverSkipIsUnexplained(t *testing.T) {
	report := filepath.Join(t.TempDir(), "report.json")
	t.Setenv(AcceptanceReportEnv, report)

	t.Cleanup(func() {
		is := is.New(t)
		rep := readReport(t, report)
		var found bool
		for _, c := range rep.Cases {
			if c.ID == "DST-01" {
				found = true
				is.Equal(c.Outcome, AcceptanceOutcomeSkipped)
				is.Equal(c.SkipKind, SkipDriver)
			}
		}
		is.True(found)
		is.True(rep.UnexplainedSkips >= 1)
		is.True(!rep.Passed) // skipped by the driver, nothing failed
	})

	d := refDriver(t, AcceptanceCapabilities{ThroughputDuration: 100 * time.Millisecond, MaxBatchSize: 100}).(ConfigurableAcceptanceTestDriver)
	d.Config.Skip = []string{`DST-01`}
	AcceptanceTestV2(t, d)
}

// A case that skips itself has its reason recorded, and the skip counts as
// unexplained.
func TestAcceptanceV2_InCaseSkipReasonIsRecorded(t *testing.T) {
	a := &acceptanceV2{skipReasons: map[string]string{}}
	var name string
	t.Run("skipper", func(t *testing.T) {
		name = t.Name()
		a.skipInCase(t, "the reason")
	})
	if got := a.takeSkipReason(name); got != "the reason" {
		t.Fatalf("reason = %q", got)
	}
	res := AcceptanceCaseResult{Outcome: AcceptanceOutcomeSkipped, SkipKind: SkipInCase}
	if !res.unexplainedSkip() {
		t.Fatal("an in-case skip must count as unexplained")
	}
}

// A run that dies leaves an unfinished report that is never passed, not the
// report of the previous run.
func TestAcceptanceV2_ReportIsWrittenAtStart(t *testing.T) {
	report := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(report, []byte(`{"passed": true, "finished": true}`), 0o600); err != nil { // the previous run
		t.Fatal(err)
	}
	t.Setenv(AcceptanceReportEnv, report)
	rep := newAcceptanceReport(refDriver(t, refCapabilities()), refCapabilities())
	rep.writeUnfinished(t)

	got := readReport(t, report)
	is := is.New(t)
	is.True(!got.Finished)
	is.True(!got.Passed)
	is.True(!got.Complete)
	is.Equal(got.CaseSetHash, rep.CaseSetHash)
}

// A filtered run must be reported as incomplete.
func TestAcceptanceV2_FilteredRunIsIncomplete(t *testing.T) {
	report := filepath.Join(t.TempDir(), "report.json")
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestAcceptanceV2_Reference$/^V2$/^DST-01$")
	cmd.Env = append(os.Environ(), AcceptanceReportEnv+"="+report)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the filtered run itself should pass: %v\n%s", err, tail(string(out), 2000))
	}
	rep := readReport(t, report)
	is := is.New(t)
	is.True(!rep.Complete)
	is.True(!rep.Passed)
	is.True(len(rep.Cases) < rep.ExpectedCaseCount)
}

// pinnedRevisions is the history of suite revisions. For each: the hash of what
// the cases are (caseContentHash, IDs, names and per-case logic versions) and
// the hash of the suite's source files (suiteSourceHash, which also covers the
// shared helpers, the constants for bounds, floors, timeouts and data sizes, and
// the Core bodies that the content hash cannot see).
//
// What the test below enforces:
//   - the newest revision must be the current one;
//   - the case content hash must match, always: adding, removing, renaming or
//     bumping the logic version of a case fails the build until the pin is
//     edited, so the edit shows in review;
//   - once the newest revision is released, the suite source hash must match
//     too: any edit to the harness, helpers, constants or Core then needs a new
//     revision and a new entry. While a revision is unreleased the source hash
//     is only logged, because every review round edits those files;
//   - every older entry must be released, and no two entries pin the same case
//     set.
//
// What it cannot enforce: an in-repo pin is editable by whoever edits the code.
// A released entry that is rewritten instead of superseded is visible in the
// diff of this table and nowhere else, so it is a review obligation (CODEOWNERS
// on this file is the intended backstop). The registry does not rely on the pin:
// it holds its own table of (revision, case content hash, suite source hash) for
// the revisions it supports.
var pinnedRevisions = []struct {
	revision string
	// hash is the case content hash
	hash string
	// sourceHash is the suite source hash; only enforced once released
	sourceHash string
	released   bool
}{
	{"2.0.0", "fd548736d83163b3800c31d6c3e997a0e18431a2856381d327c10df47cec3982", "unreleased", false},
}

func TestAcceptanceV2_CaseSetIsPinned(t *testing.T) {
	current := pinnedRevisions[len(pinnedRevisions)-1]
	if AcceptanceSuiteV2Revision != current.revision {
		t.Fatalf("AcceptanceSuiteV2Revision is %s but the newest pinned revision is %s: add an entry", AcceptanceSuiteV2Revision, current.revision)
	}
	if got := caseContentHash(); got != current.hash {
		t.Fatalf("the cases changed (content hash %s). Unreleased revision: update the pin. Released revision: bump AcceptanceSuiteV2Revision and add a pinned entry", got)
	}
	got := suiteSourceHash()
	if current.released && got != current.sourceHash {
		t.Fatalf("the suite source changed (hash %s) after revision %s was released: bump AcceptanceSuiteV2Revision and add a pinned entry", got, current.revision)
	}
	if !current.released {
		t.Logf("suite source hash of unreleased revision %s: %s (pinned: %s)", current.revision, got, current.sourceHash)
	}
	seen := map[string]string{}
	for i, p := range pinnedRevisions {
		if i < len(pinnedRevisions)-1 && !p.released {
			t.Errorf("revision %s is not the newest and is not marked released", p.revision)
		}
		if other, dup := seen[p.hash]; dup {
			t.Errorf("revisions %s and %s pin the same case set; a new revision must change something", other, p.revision)
		}
		seen[p.hash] = p.revision
	}
}

func TestAcceptanceV2_SourceHashIgnoresLineEndings(t *testing.T) {
	lf := hashFiles(map[string][]byte{"a.go": []byte("x\ny\n")})
	crlf := hashFiles(map[string][]byte{"a.go": []byte("x\r\ny\r\n")})
	if lf != crlf {
		t.Fatal("CRLF and LF checkouts must hash the same")
	}
	if lf == hashFiles(map[string][]byte{"a.go": []byte("x\nz\n")}) {
		t.Fatal("different content must hash differently")
	}
}

func TestAcceptanceV2_ExceptionsAreListed(t *testing.T) {
	is := is.New(t)
	d := refDriver(t, AcceptanceCapabilities{})
	is.Equal(exceptionsOf(d, AcceptanceCapabilities{}), []string{}) // nothing weakened, an empty list, not null

	caps := AcceptanceCapabilities{
		Source:      SourceCapabilities{Synthetic: true, MaxRedelivery: 100},
		Destination: DestinationCapabilities{WriteOnly: true},
		Waivers:     []string{"max_batch_size"},
	}
	is.Equal(exceptionsOf(d, caps), []string{"synthetic", "write_only", "max_redelivery_above_default", "waiver:max_batch_size"})
	// a driver with the process death hook says so
	is.Equal(exceptionsOf(hookDriver{d}, AcceptanceCapabilities{}), []string{"process_death_hook"})

	rep := newAcceptanceReport(d, caps)
	is.True(!rep.Limits.CertificationFloorMet) // removed checks count against the floors
	is.True(rep.RunID != "" && rep.RunID != "unavailable")
	is.True(newAcceptanceReport(d, caps).RunID != rep.RunID) // random per run
	is.Equal(len(rep.TestBinarySHA256), 64)
	is.Equal(len(rep.SuiteSourceHash), 64)
	is.True(rep.Build.GoVersion != "" && rep.Build.GOOS != "" && rep.Build.GOARCH != "") // build settings are recorded
}

type hookDriver struct{ AcceptanceTestDriver }

func (hookDriver) KilledSessionID(*testing.T) string                { return "slot" }
func (hookDriver) ReleaseSession(_ context.Context, _ string) error { return nil }

// Every v1 test method has a stable ID, and the IDs have no gaps.
func TestAcceptanceV2_CoreIDsCoverV1(t *testing.T) {
	typ := reflect.TypeOf(acceptanceTest{})
	seen := map[string]bool{}
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		if !strings.HasPrefix(name, "Test") || name == "Test" {
			continue
		}
		id, ok := coreTestIDs[name]
		if !ok {
			t.Errorf("v1 test %s has no entry in coreTestIDs", name)
		}
		seen[id] = true
	}
	if len(seen) != len(coreTestIDs) {
		t.Errorf("coreTestIDs has %d entries for %d v1 tests", len(coreTestIDs), len(seen))
	}
	for id := range seen {
		if !strings.HasPrefix(id, "CORE-") {
			t.Errorf("unexpected core ID %q", id)
		}
	}
}

// TestAcceptanceV2_Mutants checks that the suite catches known bug classes.
//
// Cases without a connector mutant, and why:
//   - DST-02 (slow acks): the acks are sent by the SDK's destination adapter,
//     a connector cannot block them. The case guards the adapter's ack path;
//     it would be mutation tested by an SDK change.
//   - CORE-*: the v1 specification and configuration tests are the v1 suite's
//     own, it has no mutants here.
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
		{"dest_caps_batch", "", "DST-03"},
		{"dest_acks_before_durable", "", "DST-04"},
		{"dest_flush_in_teardown", "", "DST-04"},
		{"dest_dups", "idempotent", "DST-05"},
		{"dest_reorders", "ordering", "DST-06"},
		{"dest_drops_new_field", "", "DST-07"},
		{"dest_truncates_large", "", "DST-08"},
		{"dest_acks_before_durable", "", "DST-09"},
		{"dest_ignores_ctx", "", "DST-10"},
		{"dest_nacks_under_load", "", "DST-11"},
		{"dest_flush_in_teardown_large", "", "DST-04"},
		{"dest_reorders_large_batch", "ordering", "DST-06"},
		{"dest_reorders_after_restart", "ordering", "DST-06"},
		{"source_ack_blocks_read", "", "SRC-01"},
		{"source_emits_dups", "", "SRC-02"},
		{"source_gap", "", "SRC-03"},
		{"source_ignores_position", "", "SRC-03"},
		{"source_dups_on_resume", "exact", "SRC-03"},
		{"cdc_resume_loses_one", "exact", "SRC-03"},
		{"cdc_resume_starts_over", "", "SRC-03"},
		{"source_commits_in_teardown", "", "SRC-03"},
		{"source_ignores_position", "redelivery1000", "SRC-03"},
		{"source_reorders", "ordering", "SRC-04"},
		{"r3_cdc_reorders", "ordering", "SRC-04"},
		{"r3_boundary_restart", "", "SRC-03"},
		{"r3_boundary_drop", "", "SRC-03"},
		{"source_reorders_after_resume", "ordering", "SRC-04"},
		{"source_drops_new_field", "", "SRC-05"},
		{"source_truncates_large", "", "SRC-06"},
		{"source_leaks_goroutine", "", "SRC-07"},
		{"source_ignores_ctx", "", "SRC-08"},
		{"source_stalls", "", "SRC-09"},
		// a bound above what the suite accepts fails the case
		{"none", "redelivery5000", "SRC-03"},
		// claims the suite cannot verify must fail, not skip
		{"none", "idempotent,writeonly", "DST-05"},
		{"none", "idempotent,noreadall", "DST-05"},
		{"none", "rawdata", "DST-07"},
		{"none", "rawdata", "SRC-05"},
	}
	for _, m := range mutants {
		t.Run(m.mutant+"/"+m.failing+"/"+m.claims, func(t *testing.T) {
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

// runRefCase runs one case of the reference connector in a subprocess and
// returns whether it passed, with the output.
func runRefCase(t *testing.T, mutant, claims, redeliver, caseID string) (bool, string) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestAcceptanceV2_Reference$/^V2$/^"+caseID+"$", "-test.v=true")
	cmd.Env = append(os.Environ(), refMutantEnv+"="+mutant, refClaimsEnv+"="+claims, "ACCEPTANCE_V2_REF_REDELIVER="+redeliver)
	out, err := cmd.CombinedOutput()
	return err == nil, string(out)
}

// A source that redelivers exactly MaxRedelivery already acked records per
// restart passes with the default bound; one more fails. This pins the
// effective bound to the declared one: acking redelivered records would let the
// resume position drift backwards and shrink it.
func TestAcceptanceV2_RedeliveryBoundIsExact(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the test binary")
	}
	if ok, out := runRefCase(t, "source_redelivers_k", "", "10", "SRC-03"); !ok {
		t.Fatalf("a source redelivering exactly %d records must pass:\n%s", DefaultMaxRedelivery, tail(out, 3000))
	}
	if ok, _ := runRefCase(t, "source_redelivers_k", "", "11", "SRC-03"); ok {
		t.Fatalf("a source redelivering %d records must fail", DefaultMaxRedelivery+1)
	}
	if ok, out := runRefCase(t, "source_redelivers_k", "redelivery1000", "1000", "SRC-03"); !ok {
		t.Fatalf("a source redelivering exactly the declared 1000 must pass:\n%s", tail(out, 3000))
	}
}

// SRC-03 restarts the source seven times with a position (two in the snapshot,
// the completion run, the boundary, two in change capture, the final run). A
// source that loses a record on any of them is caught; one that misbehaves only
// from its eighth restart on is not, and this test states that limit.
func TestAcceptanceV2_RestartCountLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the test binary")
	}
	for n := 1; n <= 7; n++ {
		if ok, _ := runRefCase(t, "r3_resume_n_loses_one", "", fmt.Sprint(n), "SRC-03"); ok {
			t.Errorf("a source that loses a record on restart %d must fail SRC-03", n)
		}
	}
	if ok, out := runRefCase(t, "r3_resume_n_loses_one", "", "8", "SRC-03"); !ok {
		t.Fatalf("restart 8 is beyond what SRC-03 exercises, the limit moved:\n%s", tail(out, 2000))
	}
}

// When the connector's Ack fails, the run fails with the error of the
// connector, not with a closed stream.
func TestAcceptanceV2_AckErrorIsSurfaced(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the test binary")
	}
	ok, out := runRefCase(t, "r3_ack_error", "", "", "SRC-01")
	if ok {
		t.Fatal("SRC-01 must fail when Ack fails")
	}
	if !strings.Contains(out, "r3 injected ack failure") {
		t.Fatalf("the failure does not carry the connector's Ack error:\n%s", tail(out, 3000))
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
