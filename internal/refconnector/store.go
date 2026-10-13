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

package refconnector

import (
	"sync"

	"github.com/conduitio/conduit-commons/opencdc"
)

// stores holds the in-memory stores by name. The connector's source and
// destination find their store through the "store" config parameter, and the
// acceptance test driver reaches the same store through GetStore.
var stores = struct {
	sync.Mutex
	m map[string]*Store
}{m: map[string]*Store{}}

// GetStore returns the store with the given name, creating it if needed.
func GetStore(name string) *Store {
	stores.Lock()
	defer stores.Unlock()
	s, ok := stores.m[name]
	if !ok {
		s = &Store{}
		stores.m[name] = s
	}
	return s
}

// ResetStore drops the store with the given name.
func ResetStore(name string) {
	stores.Lock()
	defer stores.Unlock()
	delete(stores.m, name)
}

// Store is an append-only list of records.
type Store struct {
	mu   sync.Mutex
	recs []opencdc.Record
}

// Append adds copies of recs to the end of the store.
func (s *Store) Append(recs ...opencdc.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range recs {
		s.recs = append(s.recs, r.Clone())
	}
}

// All returns copies of all records in the store.
func (s *Store) All() []opencdc.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]opencdc.Record, len(s.recs))
	for i, r := range s.recs {
		out[i] = r.Clone()
	}
	return out
}

func (s *Store) at(offset int) (opencdc.Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset >= len(s.recs) {
		return opencdc.Record{}, false
	}
	return s.recs[offset].Clone(), true
}
