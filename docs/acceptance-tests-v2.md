# Acceptance suite v2

Acceptance suite v2 is the bar a connector has to pass to be listed as
"Conduit Certified" in the connector registry. It ships with the SDK: a
connector tests against the suite revision contained in the SDK version it
pins. SDK v0.16.0 contains revision 2.0.0; the next SDK release contains
revision **2.0.1** (see [Revision history](#revision-history)).

The design, with the reasoning behind every case, is
[`20261013-acceptance-suite-v2.md`](https://github.com/ConduitIO/conduit/blob/main/docs/design-documents/20261013-acceptance-suite-v2.md)
in the Conduit repository.

## How to run it

Add a test next to (or instead of) the v1 acceptance test:

```go
func TestAcceptanceV2(t *testing.T) {
	sdk.AcceptanceTestV2(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector:         Connector,
			SourceConfig:      map[string]string{ /* … */ },
			DestinationConfig: map[string]string{ /* … */ },
			Capabilities: sdk.AcceptanceCapabilities{
				Destination: sdk.DestinationCapabilities{Idempotence: sdk.IdempotenceYes},
			},
		},
	})
}
```

and run it with the race detector, writing the report to a fresh path. The
first line the suite logs names the revision and the suite source hash, so a
run without a report still says which bar it measured against:

```sh
rm -f acceptance-report.json
CONDUIT_ACCEPTANCE_REPORT=acceptance-report.json go test -race -run TestAcceptanceV2 ./...
```

`AcceptanceTestV2` runs the v1 specification and configuration tests as the
subtest `Core` (IDs `CORE-01` to `CORE-13`) and the v2 cases as the subtest
`V2`. A connector has passed suite v2 only if both groups pass. `AcceptanceTest`
(v1) is unchanged; existing connectors keep working without touching their
tests.

A filtered run (`-run` that selects only some cases) or a skip the suite did not
decide itself produces a report that is not `complete`. Such a report does not
count as a pass.

## Capabilities

Some cases only make sense for a connector with a given property. The driver
declares these in `AcceptanceCapabilities`. A claimed capability is verified;
a claim the suite cannot verify fails the case instead of skipping it. Without
claims only the baseline runs, and the report lists what was skipped and why.

| Claim | Enables |
| --- | --- |
| `Destination.Idempotence = IdempotenceYes` | DST-05 |
| `Destination.OrdersByKey` | DST-06 |
| `Destination.SchemaEvolution` | DST-07 |
| `Source.OrdersByKey` | SRC-04 |
| `Source.SchemaEvolution = SchemaEvolutionEvolve` | SRC-05 |
| `Source.ExactResume` | SRC-03 allows no redelivery after a restart |
| `Source.MaxRedelivery` | SRC-03 redelivery bound (default 10, at most 1000) |
| `Source.Synthetic`, `Destination.WriteOnly` | for source-only and sink-only connectors; listed as exceptions in the report |

`MaxBatchSize` (default 10000) and `MaxRecordBytes` (default 1 MiB) can be
lowered for systems with hard limits. The suite then runs at the declared
value, and the report says the certification floor is not met.

## Cases

IDs are stable and are never renumbered or reused.

| ID | Case | Runs for | Checks | Invariant |
| --- | --- | --- | --- | --- |
| DST-01 | `pipelined_writes` | destination | 20 batches in flight before any ack is read; each position acked once; stored data equals sent data | 1, 3 |
| DST-02 | `slow_acks` | destination | the host reads acks slowly; no deadlock, no loss | 1 |
| DST-03 | `mixed_batch_sizes` | destination | batches of 1, 100 and 10000; all acked, all stored once | 3 |
| DST-04 | `crash_replay_at_least_once` | destination with read-back | crash at four points; every acked record is stored before `Teardown`; replay leaves nothing missing | 1, 3 |
| DST-05 | `crash_replay_idempotent` | claim | as DST-04, and replay leaves no duplicates | 3 |
| DST-06 | `ordering_per_key` | claim | records sharing a key are stored in write order, also across a restart | 4 |
| DST-07 | `schema_evolution` | claim | a new field is stored (`evolve`) or the record is nacked (`reject`); silent drop fails | 6 |
| DST-08 | `large_record` | destination | a 1 MiB record is stored intact | 3, 6 |
| DST-09 | `graceful_stop_drains_in_flight` | destination | `Stop` with 50 batches queued returns only after all are acked and stored | 7 |
| DST-10 | `context_cancellation` | destination | cancelling mid-stream ends `Run` within 15s; acked data is stored | 1, 7 |
| DST-11 | `throughput_smoke` | destination | records/s for 3s, recorded only; all acks must succeed | none |
| SRC-01 | `pipelined_and_lagging_acks` | source | acks lag reads by 50 records from another goroutine; no deadlock or loss | 2, 3 |
| SRC-02 | `mixed_batch_sizes` | source | 1, 100 and 10000 records, snapshot then CDC, each delivered once | 3 |
| SRC-03 | `crash_resume_no_gap` | source | seven kills across snapshot, the snapshot/CDC boundary and CDC; nothing acked is lost and redelivery stays within the declared bound | 2, 3 |
| SRC-04 | `ordering_per_key` | claim | records sharing a key arrive in write order, also across a restart | 4 |
| SRC-05 | `schema_evolution` | claim | a field added while the source runs arrives intact | 6 |
| SRC-06 | `large_record` | source | a 1 MiB record arrives intact | 3, 6 |
| SRC-07 | `graceful_stop_with_unacked` | source | `Stop` with unacked records reports the right last position; resume leaves no gap | 2, 7 |
| SRC-08 | `context_cancellation` | source | an idle `Read` returns when its context is cancelled | 7 |
| SRC-09 | `throughput_smoke` | source | records/s reading 2000 records, recorded only | none |

The invariant column refers to the data integrity invariants in Conduit's
`CLAUDE.md`: 1 no early ack, 2 monotonic and crash-safe positions, 3
at-least-once, 4 per-key ordering, 6 no silent schema mangling, 7 graceful
shutdown.

Every case except DST-02 has a deliberately broken reference connector (a
mutant) in the SDK's own tests that the case is required to fail. DST-02 has
none because the acks it exercises are sent by the SDK, not the connector.

### The throughput smoke checks are not benchmarks

DST-11 and SRC-09 fail only if the connector stalls or rejects records. The
records/s they measure are written to the report for history and are never
compared to a threshold. They are not a performance claim and must not be
quoted as one; Conduit's performance numbers come from
[benchi](https://github.com/ConduitIO/benchi).

### What the in-process crash cases do not cover

DST-04, DST-05 and SRC-03 stop the connector without calling `Teardown` and
inside the same process. They catch acks sent before data is durable when the
data is flushed later (on a timer or in `Teardown`). They do not catch a
connector that flushes on context cancellation, and they cannot produce torn
writes. A kill -9 of a real process is the job of each connector's chaos
tests.

## The report

With `CONDUIT_ACCEPTANCE_REPORT` set, the suite writes a JSON
`AcceptanceReport`. It is written as unfinished when the run starts and
rewritten when it ends. It contains, among others:

- `suite_version` (2) and `suite_revision` (for example 2.0.1);
- `expected_case_ids`, `case_set_hash`, `case_content_hash` and
  `suite_source_hash`, which identify exactly which cases ran;
- the claims, the floors (`certification_floor_met`, `below_floor`) and the
  `exceptions` that weaken a run;
- the outcome of every case, with a skip kind and reason where skipped;
- `complete` and `finished`, and the throughput metrics.

CI should delete the report before the run and read it only if `finished` is
true. A missing report is a failed run.

## What "Conduit Certified" means today

A connector version is Certified when its suite v2 report for a supported 2.0.x
revision is
complete, every applicable case passed, the floors are met or waived by a
maintainer, and the core delivery claims are stated (idempotence either way,
bounded resume, ordering for ordered logs, a schema evolution policy for
schema-aware connectors). `Synthetic` and `WriteOnly` need per-connector
maintainer approval.

The report is written by whoever runs the tests, so it is evidence, not
proof. Until the registry re-runs the suite itself, Certified rests on
maintainer review of the report and the registry check runs in warning mode.

## Suite revisions

The revision is `major.minor.patch`:

- **major** is the bar. A new major adds baseline cases or tightens semantics.
- **minor** adds cases that run only when a capability is claimed. A connector
  that passed 2.0.x passes 2.1.x unchanged.
- **patch** fixes the harness without changing what a connector has to do.

Each revision is pinned in the SDK's tests: once released, any change to the
cases or to the suite's source files fails the SDK build until the revision is
bumped and a new entry is pinned. The registry keeps its own table of the
hashes it accepts.

## Schema extraction

Source schema extraction is on by default, and the suite runs with whatever the
connector's configuration says. Since 2.0.1 the suite decodes records that
carry a schema attached by extraction before comparing them, the same way the
destination side of a pipeline decodes them, and the generated test data uses
field names that are valid Avro names. A connector no longer has to turn
extraction off for the acceptance run, and should not: the run is meant to
cover the configuration users get.

## Revision history

- **2.0.1** (unreleased):
  - the suite logs its revision and source hash at the start of a run;
  - records with a schema attached by source schema extraction are decoded
    before comparison, and v1 compares structured values by value (an `int` and
    an `int64` with the same value are equal);
  - generated structured data uses Avro-valid field names
    ([#395](https://github.com/ConduitIO/conduit-connector-sdk/issues/395)).
  A connector that passed 2.0.0 passes 2.0.1 unchanged. The narrower field
  names mean the generated data no longer contains field names with spaces or
  punctuation; a destination that needs to handle such names should test them
  in its own tests.
- **2.0.0** (SDK v0.16.0): first release of suite v2.
