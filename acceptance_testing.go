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
	"context"
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
	acceptanceTest{config: cfg}.Test(t)
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

type acceptanceTest struct {
	config AcceptanceTestConfig
}

func (a acceptanceTest) Test(t *testing.T) {
	a.run(t, a.testSpecifier_Specify_Success)
	a.run(t, a.testSource_Configure_Success)
	a.run(t, a.testSource_Configure_RequiredParams)
}

func (acceptanceTest) run(t *testing.T, test func(*testing.T)) {
	name := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
	name = name[strings.LastIndex(name, ".")+1:]
	t.Run(name, func(t *testing.T) { test(t) })
}

func (a acceptanceTest) testSpecifier_Specify_Success(t *testing.T) {
	a.hasSpecFactory(t)
	is := is.NewRelaxed(t) // allow multiple failures for this test

	spec := a.config.SpecFactory()

	// -- general ---------------------

	is.True(spec.Name != "")                           // Specification.Name is missing
	is.True(strings.TrimSpace(spec.Name) == spec.Name) // Specification.Name starts or ends with whitespace

	is.True(spec.Summary != "")                              // Specification.Summary is missing
	is.True(strings.TrimSpace(spec.Summary) == spec.Summary) // Specification.Summary starts or ends with whitespace

	is.True(spec.Description != "")                                  // Specification.Description is missing
	is.True(strings.TrimSpace(spec.Description) == spec.Description) // Specification.Description starts or ends with whitespace

	is.True(spec.Version != "")                              // Specification.Version is missing
	is.True(strings.TrimSpace(spec.Version) == spec.Version) // Specification.Version starts or ends with whitespace

	is.True(spec.Author != "")                             // Specification.Author is missing
	is.True(strings.TrimSpace(spec.Author) == spec.Author) // Specification.Author starts or ends with whitespace

	// TODO connectors can also be only destinations or only sources
	// TODO should we enforce that there is at least 1 parameter? what source wouldn't need any parameters?
	is.True(spec.DestinationParams != nil)   // Specification.DestinationParams is missing
	is.True(len(spec.DestinationParams) > 0) // Specification.DestinationParams is empty

	is.True(spec.SourceParams != nil)   // Specification.SourceParams is missing
	is.True(len(spec.SourceParams) > 0) // Specification.SourceParams is empty

	// -- specifics -------------------

	semverRegex := regexp.MustCompile(`v([0-9]+)(\.[0-9]+)?(\.[0-9]+)?` +
		`(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?` +
		`(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?`)

	is.True(semverRegex.MatchString(spec.Version)) // Specification.Version is not a valid semantic version (vX.Y.Z)
}

func (a acceptanceTest) testSource_Configure_Success(t *testing.T) {
	a.hasSourceFactory(t)
	is := is.New(t)
	ctx := context.Background()

	source := a.config.SourceFactory()
	err := source.Configure(ctx, a.config.SourceConfig)
	is.NoErr(err)
}

func (a acceptanceTest) testSource_Configure_RequiredParams(t *testing.T) {
	a.hasSpecFactory(t)
	a.hasSourceFactory(t)
	is := is.New(t)
	ctx := context.Background()

	spec := a.config.SpecFactory()
	for name, p := range spec.SourceParams {
		if p.Required {
			// removing the required parameter from the config should provoke an error
			t.Run(name, func(t *testing.T) {
				srcCfg := a.cloneConfig(a.config.SourceConfig)
				delete(srcCfg, name)

				is.Equal(len(srcCfg)+1, len(a.config.SourceConfig)) // source config does not contain required parameter, please check the test setup

				source := a.config.SourceFactory()
				err := source.Configure(ctx, srcCfg)
				is.True(err != nil)
			})
		}
	}
}

func (a acceptanceTest) hasSpecFactory(t *testing.T) {
	if a.config.SpecFactory == nil {
		t.Fatalf("acceptance test config is missing the field SpecFactory")
	}
}

func (a acceptanceTest) hasSourceFactory(t *testing.T) {
	if a.config.SourceFactory == nil {
		t.Fatalf("acceptance test config is missing the field SourceFactory")
	}
}

func (a acceptanceTest) hasDestinationFactory(t *testing.T) {
	if a.config.DestinationFactory == nil {
		t.Fatalf("acceptance test config is missing the field DestinationFactory")
	}
}

func (a acceptanceTest) cloneConfig(orig map[string]string) map[string]string {
	cloned := make(map[string]string, len(orig))
	for k, v := range orig {
		cloned[k] = v
	}
	return cloned
}
