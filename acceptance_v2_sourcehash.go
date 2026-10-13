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
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"sort"
)

// suiteSources holds the source files that make up the acceptance suite: the
// harness, the cases, the shared helpers and the v1 core. Their hash identifies
// the code behind a report, including the helpers, constants (bounds, floors,
// timeouts, data sizes) and Core bodies that the case content hash cannot see.
//
//go:embed acceptance_testing.go acceptance_v2.go acceptance_v2_destination.go acceptance_v2_harness.go acceptance_v2_source.go acceptance_v2_sourcehash.go
var suiteSources embed.FS

// suiteSourceHash returns the SHA-256 of the suite's source files, in name
// order, with line endings normalized.
func suiteSourceHash() string {
	entries, err := suiteSources.ReadDir(".")
	if err != nil {
		return hashUnavailable
	}
	files := make(map[string][]byte, len(entries))
	for _, e := range entries {
		b, err := suiteSources.ReadFile(e.Name())
		if err != nil {
			return hashUnavailable
		}
		files[e.Name()] = b
	}
	return hashFiles(files)
}

func hashFiles(files map[string][]byte) string {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		h.Write([]byte(n))
		h.Write([]byte{0})
		// a checkout with CRLF line endings must hash like one with LF
		h.Write(bytes.ReplaceAll(files[n], []byte("\r\n"), []byte("\n")))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
