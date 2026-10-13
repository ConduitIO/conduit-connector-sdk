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

package refconnector_test

import (
	"context"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen"
	"github.com/conduitio/conduit-connector-sdk/internal/refconnector"
)

// TestAcceptanceV2_ReferenceConnector runs suite v2 against the same
// mutation-free reference connector, for the same reason as the v1 test next to
// it: a change to the SDK or a dependency that breaks the contract fails here
// first, with no goleak options of the connector's own. The fault-injecting
// reference connector used to test the suite itself is in the sdk package.
func TestAcceptanceV2_ReferenceConnector(t *testing.T) {
	spec, err := specgen.ExtractSpecification(context.Background(), refconnector.Connector)
	if err != nil {
		t.Fatalf("failed to extract specification: %v", err)
	}
	conn := refconnector.Connector
	conn.NewSpecification = func() sdk.Specification { return spec }

	sdk.AcceptanceTestV2(t, &refDriverV2{refDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:    conn,
				ReadTimeout:  time.Second,
				WriteTimeout: time.Second,
				Capabilities: sdk.AcceptanceCapabilities{
					Source:             sdk.SourceCapabilities{ExactResume: true},
					Destination:        sdk.DestinationCapabilities{Idempotence: sdk.IdempotenceNo},
					ThroughputDuration: 500 * time.Millisecond,
				},
			},
		},
	}})
}

// refDriverV2 can return duplicates: the store is a plain list.
type refDriverV2 struct{ refDriver }

func (d *refDriverV2) ReadAllFromDestination(t *testing.T) []opencdc.Record {
	return refconnector.GetStore(t.Name()).All()
}
