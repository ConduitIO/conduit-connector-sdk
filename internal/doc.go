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

// Package internal holds implementation details shared by the SDK that are not
// part of the connector-author-facing API. Nothing here is importable by
// connectors; the parent sdk package re-exports the few pieces authors need
// (for example ConnectorIDFromContext).
//
// It contains four largely independent concerns:
//
//   - Connector state machine ([ConnectorState], [ConnectorStateWatcher]). The
//     plugin adapters drive a connector through an ordered set of lifecycle
//     states (initial → configuring → configured → starting → ... → torn down,
//     plus a terminal errored state). DoWithLock serializes state transitions:
//     it takes the state lock, checks the current state against the expected
//     states (optionally waiting for one of them), runs the supplied function,
//     and applies the before/after states. This is what enforces that, for
//     example, Run only proceeds from the started state.
//
//   - Batching ([Batcher]). A generic size-and-delay batcher: items are
//     enqueued and flushed either when the accumulated size crosses a threshold
//     or after a delay elapses, whichever comes first. The destination batch
//     write strategy uses it to group records before writing.
//
//   - Context enrichment (Enrich and the ContextWith*/*FromContext helpers).
//     Every adapter call enriches the context with connector metadata (ID and
//     log level) so it is available to logging and to connector-utility calls
//     further down the stack.
//
//   - Standalone utilities ([StandaloneConnectorUtility],
//     InitStandaloneConnectorUtilities). A registry of utilities (such as the
//     schema service) that need a gRPC connection to Conduit when the connector
//     runs as a standalone plugin. Each utility registers itself in an init
//     function and is wired up once, during standalone startup.
package internal
