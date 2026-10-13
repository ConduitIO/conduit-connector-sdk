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
		is.Equal(rep.Connector.Name, "acceptance-v2-reference")
		is.Equal(rep.Capabilities.Destination.Idempotence, IdempotenceYes)
		is.True(rep.Capabilities.Source.ExactResume)
		is.Equal(rep.ExpectedCaseCount, len(rep.ExpectedCaseIDs))
		is.Equal(rep.ExpectedCaseCount, len(coreTestIDs)+len(v2Cases()))
		is.Equal(rep.CaseSetHash, caseSetHash(rep.ExpectedCaseIDs))
		is.True(rep.Complete)
		is.True(rep.Passed)
		is.Equal(rep.UnexplainedSkips, 0)
		is.True(rep.SDKVersion != "") // "unknown" in the SDK's own tests, never empty
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

// The case set is part of the contract. If this test fails, a case was added,
// removed or renamed: bump AcceptanceSuiteV2Revision (minor for new
// claim-dependent cases, major for anything that changes the baseline) and
// update the hash here in the same change.
func TestAcceptanceV2_CaseSetIsPinned(t *testing.T) {
	const wantRevision = "2.0.0"
	const wantHash = "89745d24d42ed9fccca645e44eb39cbaba9dbdc6fbbd05f12877faf445bde5c4"
	if AcceptanceSuiteV2Revision != wantRevision {
		t.Fatalf("revision is %s, this test pins %s: update the pinned hash too", AcceptanceSuiteV2Revision, wantRevision)
	}
	if got := caseSetHash(expectedCaseIDs()); got != wantHash {
		t.Fatalf("case set hash changed to %s without a revision bump", got)
	}
}

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
		{"source_ack_blocks_read", "", "SRC-01"},
		{"source_emits_dups", "", "SRC-02"},
		{"source_gap", "", "SRC-03"},
		{"source_ignores_position", "", "SRC-03"},
		{"source_dups_on_resume", "exact", "SRC-03"},
		{"source_reorders", "ordering", "SRC-04"},
		{"source_drops_new_field", "", "SRC-05"},
		{"source_truncates_large", "", "SRC-06"},
		{"source_leaks_goroutine", "", "SRC-07"},
		{"source_ignores_ctx", "", "SRC-08"},
		{"source_stalls", "", "SRC-09"},
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

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
