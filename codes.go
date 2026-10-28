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
	"regexp"
	"strings"
)

// Stable codes for conditions the SDK reports about a connector's
// configuration. They are part of the SDK's public contract: a code is never
// renamed or reused, and a retired code stays documented.
//
// The SDK has no error-code mechanism of its own, and a Configure error
// reaches Conduit as a plain string that other layers have wrapped. The codes
// therefore travel in two forms:
//
//   - warnings are logged with the code in a structured "code" field, next to
//     "config_path" and "fix";
//   - when a rule becomes a rejection, the code appears in the error message as
//     a delimited token. Consumers must find it with [CodeFromMessage] (or
//     equivalent token matching), not by checking the message prefix.
const (
	// CodeBatchUnboundedWait reports a source batching configuration that
	// waits for a full batch with no deadline, so a sparse source can hold
	// records unsent indefinitely. Fix: set sdk.batch.delay above 0, or set
	// sdk.batch.size to 0 to turn batching off.
	CodeBatchUnboundedWait = "sdk.batch.unbounded_wait"

	// CodeBatchSizeDefaulted reports that sdk.batch.delay was set while
	// sdk.batch.size is 0, and the SDK substituted a bounded batch size.
	// Fix: set sdk.batch.size explicitly.
	CodeBatchSizeDefaulted = "sdk.batch.size_defaulted"

	// CodeBatchDrainSemanticsPending reports an explicit sdk.batch.size above
	// 0 with sdk.batch.delay 0, which today waits until the batch is full and
	// will mean "return what is already queued" in a later SDK release. Fix:
	// set sdk.batch.delay above 0 to keep full batches.
	CodeBatchDrainSemanticsPending = "sdk.batch.drain_semantics_pending"
)

var batchCodes = []string{
	CodeBatchUnboundedWait,
	CodeBatchSizeDefaulted,
	CodeBatchDrainSemanticsPending,
}

// codeRegexps matches each known code as a delimited token: not preceded by an
// identifier character and not followed by one, so "x"+code and code+".y" do
// not match, while "...: code: message" and "code." at the end of a sentence do.
var codeRegexps = func() map[string]*regexp.Regexp {
	m := make(map[string]*regexp.Regexp, len(batchCodes))
	for _, c := range batchCodes {
		m[c] = regexp.MustCompile(`(?:^|[^A-Za-z0-9_.-])` + regexp.QuoteMeta(c) + `(?:$|[^A-Za-z0-9_.-]|\.(?:$|[^A-Za-z0-9_]))`)
	}
	return m
}()

// CodeFromMessage returns the first known SDK code that appears in msg as a
// delimited token, wherever it sits in the message. Errors returned from
// Configure are wrapped by several layers, so the code is not at the start.
func CodeFromMessage(msg string) (string, bool) {
	if !strings.Contains(msg, "sdk.") {
		return "", false
	}
	best, bestIdx := "", -1
	for _, c := range batchCodes {
		loc := codeRegexps[c].FindStringIndex(msg)
		if loc != nil && (bestIdx < 0 || loc[0] < bestIdx) {
			best, bestIdx = c, loc[0]
		}
	}
	return best, bestIdx >= 0
}
