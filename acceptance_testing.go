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
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"go.uber.org/goleak"
)

// AcceptanceTest is the acceptance test that all connector implementations
// should pass. It should manually be called from a test case in each
// implementation:
//
//   func TestAcceptance(t *testing.T) {
//       // set up test dependencies ...
//       sdk.AcceptanceTest(t, sdk.DefaultAcceptanceTestDriver{
//           Config: sdk.DefaultAcceptanceTestDriverConfig{
//               Connector: myConnector,
//               SourceConfig: map[string]string{...},      // valid source config
//               DestinationConfig: map[string]string{...}, // valid destination config
//           },
//       })
//   }
func AcceptanceTest(t *testing.T, driver AcceptanceTestDriver) {
	acceptanceTest{driver: driver}.Test(t)
}

// AcceptanceTestDriver TODO
type AcceptanceTestDriver interface {
	// Connector is the connector to be tested.
	Connector() Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig() map[string]string
	// DestinationConfig should be a valid config for a destination connector,
	// writing to the same location as the source will read from.
	DestinationConfig() map[string]string

	BeforeTest(*testing.T, string)
	AfterTest(*testing.T, string)

	// Skip should return true and a reason if the test should be skipped,
	// otherwise it should return false and an empty string.
	Skip(testName string) (skip bool, reason string)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions(testName string) []goleak.Option

	// SourceReadExpectation returns a slice of records the source is expected
	// to return.
	SourceReadExpectation(t *testing.T) []Record
}

// DefaultAcceptanceTestDriver TODO
type DefaultAcceptanceTestDriver struct {
	Config DefaultAcceptanceTestDriverConfig
}

// DefaultAcceptanceTestDriverConfig TODO
type DefaultAcceptanceTestDriverConfig struct {
	// Connector is the connector to be tested.
	Connector Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig map[string]string
	// DestinationConfig should be a valid config for a destination connector,
	// writing to the same location as the source will read from.
	DestinationConfig map[string]string

	BeforeTest func(t *testing.T, testName string)
	AfterTest  func(t *testing.T, testName string)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions []goleak.Option

	// Skip lets the caller define if any tests should be skipped (useful for
	// skipping destination/source tests if the connector only implements one
	// side of the connector)
	Skip []string
}

func (d DefaultAcceptanceTestDriver) Connector() Connector {
	return d.Config.Connector
}

func (d DefaultAcceptanceTestDriver) SourceConfig() map[string]string {
	return d.Config.SourceConfig
}

func (d DefaultAcceptanceTestDriver) DestinationConfig() map[string]string {
	return d.Config.DestinationConfig
}

func (d DefaultAcceptanceTestDriver) BeforeTest(t *testing.T, testName string) {
	if d.Config.BeforeTest != nil {
		d.Config.BeforeTest(t, testName)
	}
}

func (d DefaultAcceptanceTestDriver) AfterTest(t *testing.T, testName string) {
	if d.Config.AfterTest != nil {
		d.Config.AfterTest(t, testName)
	}
}

func (d DefaultAcceptanceTestDriver) Skip(testName string) (bool, string) {
	var skipRegexs []*regexp.Regexp
	for _, skipRegex := range d.Config.Skip {
		r := regexp.MustCompile(skipRegex)
		skipRegexs = append(skipRegexs, r)
	}

	for _, skipRegex := range skipRegexs {
		if skipRegex.MatchString(testName) {
			return true, fmt.Sprintf("caller requested to skip tests that match the regex %q", skipRegex.String())
		}
	}

	return false, ""
}

func (d DefaultAcceptanceTestDriver) GoleakOptions(testName string) []goleak.Option {
	return d.Config.GoleakOptions
}

// SourceReadExpectation by default opens the destination and writes a record to
// the destination. It is expected that the destination is writing to the same
// location the source is reading from. If the connector does not implement a
// destination the function will fail the test.
func (d DefaultAcceptanceTestDriver) SourceReadExpectation(t *testing.T) []Record {
	if d.Connector().NewDestination == nil {
		t.Fatal("connector is missing the field NewDestination, either implement the destination or overwrite the driver method SourceReadExpectation")
	}

	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// writing something to the destination should result in the same record
	// being produced by the source
	dest := d.Connector().NewDestination()
	err := dest.Configure(ctx, d.DestinationConfig())
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	want := []Record{{
		Position:  Position("foo1"), // position doesn't matter, as long as it's unique
		Metadata:  nil,              // metadata is optional so don't enforce it to be written
		CreatedAt: time.Now(),
		Key:       RawData("bar1"),
		Payload:   RawData("baz1"),
	}, {
		Position:  Position("foo2"), // position doesn't matter, as long as it's unique
		Metadata:  nil,              // metadata is optional so don't enforce it to be written
		CreatedAt: time.Now(),
		Key:       RawData("bar2"),
		Payload:   RawData("baz2"),
	}}

	// try to write using WriteAsync and fallback to Write if it's not supported
	err = d.writeAsync(ctx, dest, want...)
	if errors.Is(err, ErrUnimplemented) {
		err = d.write(ctx, dest, want...)
	}
	is.NoErr(err)

	cancel() // cancel context to simulate stop
	err = dest.Teardown(context.Background())
	is.NoErr(err)

	return want
}

// writeAsync writes records to destination using Destination.WriteAsync.
func (d DefaultAcceptanceTestDriver) writeAsync(ctx context.Context, dest Destination, records ...Record) error {
	var waitForAck sync.WaitGroup
	var ackErr error

	for _, r := range records {
		waitForAck.Add(1)
		ack := func(err error) error {
			defer waitForAck.Done()
			if ackErr == nil { // only overwrite a nil error
				ackErr = err
			}
			return nil
		}
		err := dest.WriteAsync(ctx, r, ack)
		if err != nil {
			return err
		}
	}

	// flush to make sure the records get written to the destination
	err := dest.Flush(ctx)
	if err != nil && !errors.Is(err, ErrUnimplemented) {
		return err
	}

	waitForAck.Wait()
	if ackErr != nil {
		return ackErr
	}

	// records were successfully written
	return nil
}

// writeAsync writes records to destination using Destination.WriteAsync.
func (d DefaultAcceptanceTestDriver) write(ctx context.Context, dest Destination, records ...Record) error {
	for _, r := range records {
		err := dest.Write(ctx, r)
		if err != nil {
			return err
		}
	}

	// flush to make sure the records get written to the destination
	err := dest.Flush(ctx)
	if err != nil && !errors.Is(err, ErrUnimplemented) {
		return err
	}

	// records were successfully written
	return nil
}

type acceptanceTest struct {
	driver AcceptanceTestDriver
}

// Test runs all acceptance tests.
func (a acceptanceTest) Test(t *testing.T) {
	av := reflect.ValueOf(a)
	at := av.Type()

	for i := 0; i < at.NumMethod(); i++ {
		testName := at.Method(i).Name
		if testName == "Test" || !strings.HasPrefix(testName, "Test") {
			// not a test method
			continue
		}
		t.Run(testName, func(t *testing.T) {
			if skip, reason := a.driver.Skip(testName); skip {
				// skip test if caller requested it
				t.Skip(reason)
			}

			a.driver.BeforeTest(t, testName)
			t.Cleanup(func() { a.driver.AfterTest(t, testName) })

			av.Method(i).Call([]reflect.Value{reflect.ValueOf(t)})
		})
	}
}

func (a acceptanceTest) TestSpecifier_Exists(t *testing.T) {
	if a.driver.Connector().NewSpecification == nil {
		t.Fatal("connector is missing the field NewSpecification - connector specifications are required")
	}
}

func (a acceptanceTest) TestSpecifier_Specify_Success(t *testing.T) {
	a.skipIfNoSpecification(t)
	is := is.NewRelaxed(t) // allow multiple failures for this test

	spec := a.driver.Connector().NewSpecification()

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

	// TODO assert parameter format (camel case, dots allowed)

	semverRegex := regexp.MustCompile(`v([0-9]+)(\.[0-9]+)?(\.[0-9]+)?` +
		`(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?` +
		`(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?`)

	is.True(semverRegex.MatchString(spec.Version)) // Specification.Version is not a valid semantic version (vX.Y.Z)
}

func (a acceptanceTest) TestSource_Configure_Success(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()

	source := a.driver.Connector().NewSource()
	err := source.Configure(ctx, a.driver.SourceConfig())
	is.NoErr(err)

	// calling Teardown after Configure is valid and happens when connector is created
	err = source.Teardown(ctx)
	is.NoErr(err)
}

func (a acceptanceTest) TestSource_Configure_RequiredParams(t *testing.T) {
	a.skipIfNoSpecification(t)
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()

	spec := a.driver.Connector().NewSpecification()
	for name, p := range spec.SourceParams {
		if p.Required {
			// removing the required parameter from the config should provoke an error
			t.Run(name, func(t *testing.T) {
				srcCfg := a.cloneConfig(a.driver.SourceConfig())
				delete(srcCfg, name)

				is.Equal(len(srcCfg)+1, len(a.driver.SourceConfig())) // source config does not contain required parameter, please check the test setup

				source := a.driver.Connector().NewSource()
				err := source.Configure(ctx, srcCfg)
				is.True(err != nil)

				err = source.Teardown(ctx)
				is.NoErr(err)
			})
		}
	}
}

func (a acceptanceTest) TestSource_Read_Success(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t.Name())...)

	source := a.driver.Connector().NewSource()

	err := source.Configure(ctx, a.driver.SourceConfig())
	is.NoErr(err)

	openCtx, cancelOpenCtx := context.WithCancel(ctx)
	err = source.Open(openCtx, nil) // listen from beginning
	is.NoErr(err)

	defer func() {
		cancelOpenCtx()
		err = source.Teardown(ctx)
		is.NoErr(err)
	}()

	want := a.driver.SourceReadExpectation(t)

	for i := 0; i < len(want); i++ {
		// now try to read from the source
		readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		got, err := source.Read(readCtx)
		is.NoErr(err)

		want[i].Position = got.Position   // position can't be determined in advance
		want[i].CreatedAt = got.CreatedAt // created at can't be determined in advance

		// TODO do a smarter comparison that checks fields separately (e.g. metadata, position etc.)
		is.Equal(want[i], got)
	}
}

func (a acceptanceTest) TestSource_Read_Timeout(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t.Name())...)

	source := a.driver.Connector().NewSource()
	err := source.Configure(ctx, a.driver.SourceConfig())
	is.NoErr(err)

	openCtx, cancelOpenCtx := context.WithCancel(ctx)
	err = source.Open(openCtx, nil)
	is.NoErr(err)

	defer func() {
		cancelOpenCtx()
		err = source.Teardown(ctx)
		is.NoErr(err)
	}()

	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := source.Read(readCtx)
	is.Equal(r, Record{}) // record should be empty
	is.True(errors.Is(err, context.DeadlineExceeded))
}

func (a acceptanceTest) skipIfNoSpecification(t *testing.T) {
	if a.driver.Connector().NewSpecification == nil {
		t.Skip("connector is missing the field NewSpecification")
	}
}

func (a acceptanceTest) skipIfNoSource(t *testing.T) {
	if a.driver.Connector().NewSource == nil {
		t.Skip("connector is missing the field NewSource")
	}
}

func (a acceptanceTest) skipIfNoDestination(t *testing.T) {
	if a.driver.Connector().NewDestination == nil {
		t.Skip("connector is missing the field NewDestination")
	}
}

func (a acceptanceTest) cloneConfig(orig map[string]string) map[string]string {
	cloned := make(map[string]string, len(orig))
	for k, v := range orig {
		cloned[k] = v
	}
	return cloned
}
