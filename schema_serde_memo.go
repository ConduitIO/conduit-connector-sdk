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
	"fmt"
	"slices"
	"sync"

	"github.com/conduitio/conduit-connector-sdk/schema"
)

// serdeMemoEntries bounds the number of schemas a schemaSerdeMemo remembers.
// A connector works with a handful of schema versions at a time per collection.
const serdeMemoEntries = 256

// serdeMemoBytes bounds the schema text (plus subject) a schemaSerdeMemo keeps
// copies of. Schema texts come from the registry and can be large, so a count
// alone is not a memory bound. A schema bigger than a sixteenth of this is not
// memoized and costs what it cost before the memo.
const serdeMemoBytes = 16 << 20

// schemaSerdeMemo remembers the Serde of the schemas a middleware encodes or
// decodes with, so that the hot path doesn't have to fingerprint the schema
// text for every record.
//
// schema.Schema.Serde finds the serde in a process wide cache keyed by the
// Rabin fingerprint of the schema text, which means hashing the whole text (a
// few KB for a wide record) on every call: for the default chains this was
// several times the cost of the encoding itself.
//
// The memo is keyed by subject and version, but it does not trust the key.
// Callers still fetch the schema from the schema service for every record
// (that is what schema.Get caches), and a remembered serde is only used if it
// was built from exactly the schema text just fetched. A registry that hands
// out different bytes for the same subject and version (it must not, but it
// can be restored from a backup or replaced) is therefore noticed on the next
// record rather than after any TTL. Invariant 6: records are never encoded or
// decoded with a schema other than the one the service returned for them.
type schemaSerdeMemo struct {
	mu    sync.Mutex
	m     map[serdeMemoKey]serdeMemoEntry
	bytes int // accounted size of m, see serdeMemoBytes
}

type serdeMemoKey struct {
	subject string
	version int
}

type serdeMemoEntry struct {
	typ   schema.Type
	bytes []byte // private copy of the schema text the serde was built from
	serde schema.Serde
}

func (m *schemaSerdeMemo) serde(sch schema.Schema) (schema.Serde, error) {
	key := serdeMemoKey{subject: sch.Subject, version: sch.Version}

	m.mu.Lock()
	e, ok := m.m[key]
	m.mu.Unlock()
	if ok && e.typ == sch.Type && bytes.Equal(e.bytes, sch.Bytes) {
		return e.serde, nil
	}

	srd, err := sch.Serde()
	if err != nil {
		return nil, err //nolint:wrapcheck // callers add context, same as before
	}

	size := len(sch.Subject) + len(sch.Bytes)
	if size > serdeMemoBytes/16 {
		return srd, nil
	}

	m.mu.Lock()
	if m.m == nil {
		m.m = make(map[serdeMemoKey]serdeMemoEntry, 8)
	}
	if old, replace := m.m[key]; replace {
		m.bytes -= len(key.subject) + len(old.bytes)
		delete(m.m, key)
	}
	// Full (by count or by bytes): drop arbitrary entries. A miss only costs
	// what every call cost before the memo, so there is no need for an exact LRU.
	for k, old := range m.m {
		if len(m.m) < serdeMemoEntries && m.bytes+size <= serdeMemoBytes {
			break
		}
		delete(m.m, k)
		m.bytes -= len(k.subject) + len(old.bytes)
	}
	m.m[key] = serdeMemoEntry{typ: sch.Type, bytes: slices.Clone(sch.Bytes), serde: srd}
	m.bytes += size
	m.mu.Unlock()

	return srd, nil
}

// unmarshal is schema.Schema.Unmarshal using the memoized serde. The error
// text is identical.
func (m *schemaSerdeMemo) unmarshal(sch schema.Schema, b []byte, v any) error {
	srd, err := m.serde(sch)
	if err != nil {
		return err
	}
	if err := srd.Unmarshal(b, v); err != nil {
		return fmt.Errorf("failed to unmarshal data with schema %v:%v (id: %v): %w", sch.Subject, sch.Version, sch.ID, err)
	}
	return nil
}
