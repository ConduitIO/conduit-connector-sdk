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

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// DestinationConfig is the destination configuration.
type DestinationConfig struct {
	sdk.DefaultDestinationMiddleware

	// Store is the name of the in-memory store to write to.
	Store string `json:"store" validate:"required"`
}

// Destination appends records to a store.
type Destination struct {
	sdk.UnimplementedDestination

	config DestinationConfig
	store  *Store
}

// NewDestination creates the destination with default middleware.
func NewDestination() sdk.Destination { return sdk.DestinationWithMiddleware(&Destination{}) }

// Config returns the destination configuration.
func (d *Destination) Config() sdk.DestinationConfig { return &d.config }

// Open attaches the destination to its store.
func (d *Destination) Open(context.Context) error {
	d.store = GetStore(d.config.Store)
	return nil
}

// Write appends recs to the store.
func (d *Destination) Write(_ context.Context, recs []opencdc.Record) (int, error) {
	d.store.Append(recs...)
	return len(recs), nil
}

// Teardown is a no-op.
func (d *Destination) Teardown(context.Context) error { return nil }
