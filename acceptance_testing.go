// Copyright © 2022 Meroxa, Inc.
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
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/matryer/is"
)

// AcceptanceTest is the acceptance test that all connector implementations
// should pass. It should manually be called from a test case in each
// implementation:
//
//   func TestAcceptance(t *testing.T) {
//       // set up test dependencies ...
//       sdk.AcceptanceTest(t, sdk.AcceptanceTestConfig{...})
//   }
//
func AcceptanceTest(t *testing.T, cfg AcceptanceTestConfig) {
	run(t, cfg, testSpecifier_Specify_Success)
}

type AcceptanceTestConfig struct {
	SpecFactory        func() Specification
	SourceFactory      func() Source
	DestinationFactory func() Destination

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig map[string]string
	// DestinationConfig should be a valid config for a destination connector,
	// writing to the same location as the source will read from.
	DestinationConfig map[string]string
}

func run(t *testing.T, cfg AcceptanceTestConfig, test func(*testing.T, AcceptanceTestConfig)) {
	name := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
	name = name[strings.LastIndex(name, ".")+1:]
	t.Run(name, func(t *testing.T) { test(t, cfg) })
}

func testSpecifier_Specify_Success(t *testing.T, cfg AcceptanceTestConfig) {
	is := is.NewRelaxed(t) // allow multiple failures for this test

	if cfg.SpecFactory == nil {
		t.Fatalf("acceptance test config is missing the field SpecFactory")
	}

	specification := cfg.SpecFactory()

	// -- general ---------------------

	is.True(specification.Name != "")                                    // Specification.Name is missing
	is.True(strings.TrimSpace(specification.Name) == specification.Name) // Specification.Name starts or ends with whitespace

	is.True(specification.Summary != "")                                       // Specification.Summary is missing
	is.True(strings.TrimSpace(specification.Summary) == specification.Summary) // Specification.Summary starts or ends with whitespace

	is.True(specification.Description != "")                                           // Specification.Description is missing
	is.True(strings.TrimSpace(specification.Description) == specification.Description) // Specification.Description starts or ends with whitespace

	is.True(specification.Version != "")                                       // Specification.Version is missing
	is.True(strings.TrimSpace(specification.Version) == specification.Version) // Specification.Version starts or ends with whitespace

	is.True(specification.Author != "")                                      // Specification.Author is missing
	is.True(strings.TrimSpace(specification.Author) == specification.Author) // Specification.Author starts or ends with whitespace

	// TODO connectors can also be only destinations or only sources
	// TODO should we enforce that there is at least 1 parameter? what source wouldn't need any parameters?
	is.True(specification.DestinationParams != nil)   // Specification.DestinationParams is missing
	is.True(len(specification.DestinationParams) > 0) // Specification.DestinationParams is empty

	is.True(specification.SourceParams != nil)   // Specification.SourceParams is missing
	is.True(len(specification.SourceParams) > 0) // Specification.SourceParams is empty

	// -- specifics -------------------

	semverRegex := regexp.MustCompile(`v([0-9]+)(\.[0-9]+)?(\.[0-9]+)?` +
		`(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?` +
		`(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?`)

	is.True(semverRegex.MatchString(specification.Version)) // Specification.Version is not a valid semantic version (vX.Y.Z)
}
