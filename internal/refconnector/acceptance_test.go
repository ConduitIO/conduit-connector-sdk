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

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen"
	"github.com/conduitio/conduit-connector-sdk/internal/refconnector"
)

// TestAcceptance_ReferenceConnector runs the SDK's own acceptance suite against
// the minimal in-memory connector in this package, with the SDK's default
// middleware, including schema extraction.
//
// The acceptance suite is the compatibility contract for every connector, so it
// has to stay green on the SDK's main branch. Before this test existed the SDK
// had no acceptance run of its own: a dependency bump (twmb/go-cache v1.4.0)
// turned the suite red for every connector and nothing here noticed.
//
// This lives in its own package, and so its own test binary, on purpose:
// goleak looks at every goroutine in the process, so tests of other SDK
// packages that leave goroutines behind would fail this suite for reasons
// unrelated to the connector.
//
// The connector deliberately sets no goleak options. Any goroutine the SDK or
// its dependencies leave behind has to be handled by the SDK's own, global
// ignore list in the suite, so a stale entry there fails this test first.
func TestAcceptance_ReferenceConnector(t *testing.T) {
	// Derive the parameters (including the middleware ones, which carry the
	// defaults the middleware relies on) from the config structs, the same way
	// specgen builds a connector.yaml.
	spec, err := specgen.ExtractSpecification(context.Background(), refconnector.Connector)
	if err != nil {
		t.Fatalf("failed to extract specification: %v", err)
	}
	conn := refconnector.Connector
	conn.NewSpecification = func() sdk.Specification { return spec }

	driver := &refDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:    conn,
				ReadTimeout:  time.Second,
				WriteTimeout: time.Second,
			},
		},
	}
	sdk.AcceptanceTest(t, driver)
}

// refDriver gives every acceptance test its own store, so tests cannot see each
// other's records.
type refDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func (d *refDriver) BeforeTest(t *testing.T) {
	refconnector.ResetStore(t.Name())
	d.ConfigurableAcceptanceTestDriver.BeforeTest(t)
}

func (d *refDriver) SourceConfig(t *testing.T) config.Config {
	return config.Config{"store": t.Name()}
}

func (d *refDriver) DestinationConfig(t *testing.T) config.Config {
	return config.Config{"store": t.Name()}
}

func (d *refDriver) WriteToSource(t *testing.T, recs []opencdc.Record) []opencdc.Record {
	refconnector.GetStore(t.Name()).Append(recs...)
	return recs
}

func (d *refDriver) ReadFromDestination(t *testing.T, _ []opencdc.Record) []opencdc.Record {
	return refconnector.GetStore(t.Name()).All()
}
