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
	"strings"
	"testing"
	"time"

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

	BeforeTest func(*testing.T, string)
	AfterTest  func(*testing.T, string)

	// Skip lets the caller define if any tests should be skipped (useful for
	// skipping destination/source tests if the connector only implements one
	// side of the connector)
	Skip []string
}

type acceptanceTest struct {
	config AcceptanceTestConfig
}

// Test runs all acceptance tests.
func (a acceptanceTest) Test(t *testing.T) {
	var skipRegexs []*regexp.Regexp
	for _, skipRegex := range a.config.Skip {
		r, err := regexp.Compile(skipRegex)
		if err != nil {
			t.Fatalf("could not compile skip regex %q: %v", skipRegex, err)
		}
		skipRegexs = append(skipRegexs, r)
	}

	av := reflect.ValueOf(a)
	at := av.Type()

	for i := 0; i < at.NumMethod(); i++ {
		testName := at.Method(i).Name
		if testName == "Test" || !strings.HasPrefix(testName, "Test") {
			// not a test method
			continue
		}
		t.Run(testName, func(t *testing.T) {
			a.skip(t, skipRegexs) // skip test if caller requested it
			if a.config.BeforeTest != nil {
				a.config.BeforeTest(t, testName)
			}
			av.Method(i).Call([]reflect.Value{reflect.ValueOf(t)})
			if a.config.AfterTest != nil {
				a.config.AfterTest(t, testName)
			}
		})
	}
}

func (a acceptanceTest) TestSpecifier_Specify_Success(t *testing.T) {
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

func (a acceptanceTest) TestSource_Configure_Success(t *testing.T) {
	a.hasSourceFactory(t)
	is := is.New(t)
	ctx := context.Background()

	source := a.config.SourceFactory()
	err := source.Configure(ctx, a.config.SourceConfig)
	is.NoErr(err)
}

func (a acceptanceTest) TestSource_Configure_RequiredParams(t *testing.T) {
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

func (a acceptanceTest) TestSource_Teardown_AfterConfigure(t *testing.T) {
	a.hasSourceFactory(t)
	is := is.New(t)
	ctx := context.Background()

	source := a.config.SourceFactory()
	err := source.Configure(ctx, a.config.SourceConfig)
	is.NoErr(err)

	// calling Teardown after Configure is valid and happens when connector is created
	err = source.Teardown(ctx)
	is.NoErr(err)
}

func (a acceptanceTest) TestSource_Read_Timeout(t *testing.T) {
	a.hasSourceFactory(t)
	is := is.New(t)
	ctx := context.Background()

	source := a.config.SourceFactory()
	err := source.Configure(ctx, a.config.SourceConfig)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	readCtx, cancel := context.WithTimeout(ctx, time.Second*5) // TODO should we lower timeout?
	defer cancel()
	r, err := source.Read(ctx)
	is.Equal(r, Record{}) // record should be empty
	is.Equal(err, readCtx.Err())
}

func (a acceptanceTest) TestSource_Read_Success(t *testing.T) {
	a.hasSourceFactory(t)
	a.hasDestinationFactory(t)
	is := is.New(t)
	ctx := context.Background()

	dest := a.config.DestinationFactory()
	err := dest.Configure(ctx, a.config.DestinationConfig)
	is.NoErr(err)

	source := a.config.SourceFactory()
	err = source.Configure(ctx, a.config.SourceConfig)
	is.NoErr(err)

	err = source.Open(ctx, nil) // listen from beginning
	is.NoErr(err)

	// writing something to the destination should result in the same record
	// being produced by the source
	err = dest.Open(ctx)
	is.NoErr(err)

	want := Record{
		Position:  Position("foo"), // TODO figure out how to set position
		Metadata:  nil,             // TODO metadata
		CreatedAt: time.Now(),
		Key:       nil, // TODO key
		Payload:   RawData("baz"),
	}
	// TODO detect if we should write async
	err = dest.Write(ctx, want)
	is.NoErr(err)

	err = dest.Teardown(ctx)
	is.NoErr(err)

	// now try to read from the source
	ctxRead, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	got, err := source.Read(ctxRead)
	is.NoErr(err)

	want.Position = got.Position   // position can't be determined in advance
	want.CreatedAt = got.CreatedAt // created at can't be determined in advance
	is.Equal(want, got)

	err = source.Teardown(ctx)
	is.NoErr(err)
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

func (a acceptanceTest) skip(t *testing.T, skipRegexs []*regexp.Regexp) {
	for _, skipRegex := range skipRegexs {
		if skipRegex.MatchString(t.Name()) {
			t.Skipf("caller requested to skip tests that match the regex %q", skipRegex.String())
		}
	}
}

func (a acceptanceTest) cloneConfig(orig map[string]string) map[string]string {
	cloned := make(map[string]string, len(orig))
	for k, v := range orig {
		cloned[k] = v
	}
	return cloned
}
