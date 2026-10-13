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

// Package refconnector is a minimal in-memory connector that the SDK runs its
// own acceptance suite against (see acceptance_test.go). It exists so a change
// to the SDK or to one of its dependencies that breaks the acceptance contract
// turns the SDK's CI red, instead of being found later by whichever connector
// first runs the suite against the new SDK.
//
// It uses the SDK's default middleware (source schema extraction is off, see
// NewSource). Its source and destination parameters are not written down
// anywhere: the acceptance test derives them from the config structs with
// specgen, the same way a real connector's connector.yaml is generated. It is
// not a supported connector.
package refconnector

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Connector is the reference connector.
var Connector = sdk.Connector{
	NewSpecification: func() sdk.Specification {
		return sdk.Specification{
			Name:        "reference",
			Summary:     "In-memory connector used to run the SDK acceptance suite in CI.",
			Description: "Stores records in process memory. Not for production use.",
			Version:     "(devel)",
			Author:      "Meroxa, Inc.",
		}
	},
	NewSource:      NewSource,
	NewDestination: NewDestination,
}
