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
	"context"
	"fmt"
	"strconv"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// SourceConfig is the source configuration.
type SourceConfig struct {
	sdk.DefaultSourceMiddleware

	// Store is the name of the in-memory store to read from.
	Store string `json:"store" validate:"required"`
}

// Source replays the records of a store. Its position is the index of the next
// record to read, so a restart resumes right after the last record it returned.
type Source struct {
	sdk.UnimplementedSource

	config SourceConfig
	store  *Store
	next   int
}

// NewSource creates the source with the default middleware. Source schema
// extraction is on, as it is by default for every connector: the acceptance
// harness decodes what extraction encoded before comparing, the way the
// destination side of a pipeline does (#395).
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		config: SourceConfig{},
	})
}

// Config returns the source configuration.
func (s *Source) Config() sdk.SourceConfig { return &s.config }

// Open attaches the source to its store and seeks to pos.
func (s *Source) Open(_ context.Context, pos opencdc.Position) error {
	s.store = GetStore(s.config.Store)
	if pos != nil {
		n, err := strconv.Atoi(string(pos))
		if err != nil || n < 0 {
			return fmt.Errorf("invalid position %q, expected a non-negative number", pos)
		}
		s.next = n
	}
	return nil
}

// Read returns the next record, or sdk.ErrBackoffRetry if there is none yet.
func (s *Source) Read(context.Context) (opencdc.Record, error) {
	rec, ok := s.store.at(s.next)
	if !ok {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
	s.next++
	rec.Position = opencdc.Position(strconv.Itoa(s.next))
	return rec, nil
}

// Ack is a no-op, the store is not consumed.
func (s *Source) Ack(context.Context, opencdc.Position) error { return nil }

// Teardown is a no-op.
func (s *Source) Teardown(context.Context) error { return nil }
