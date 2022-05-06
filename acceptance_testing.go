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
	"math/rand"
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
//       sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
//           Config: sdk.ConfigurableAcceptanceTestDriverConfig{
//               Connector: myConnector,
//               SourceConfig: map[string]string{...},      // valid source config
//               DestinationConfig: map[string]string{...}, // valid destination config
//           },
//       })
//   }
//
// IMPORTANT: This function is not ready to be used yet, tests might fail while
// the connector could already be working correctly. This comment should be
// removed once we fix the following issues:
// - Move generating of records to driver (https://github.com/ConduitIO/conduit-connector-sdk/issues/16)
// - Generate structured data (https://github.com/ConduitIO/conduit-connector-sdk/issues/17)
// - Smarter record comparison (https://github.com/ConduitIO/conduit-connector-sdk/issues/18)
func AcceptanceTest(t *testing.T, driver AcceptanceTestDriver) {
	acceptanceTest{
		driver: driver,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}.Test(t)
}

// AcceptanceTestDriver is the object that each test uses for fetching the
// connector and its configurations. The SDK provides a default implementation
// ConfigurableAcceptanceTestDriver that should fit most use cases. In case more
// flexibility is needed you can create your own driver, include the default
// driver in the struct and override methods as needed.
type AcceptanceTestDriver interface {
	// Connector is the connector to be tested.
	Connector() Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig(*testing.T) map[string]string
	// DestinationConfig should be a valid config for a destination connector,
	// writing to the same location as the source will read from.
	DestinationConfig(*testing.T) map[string]string

	// BeforeTest is executed before each acceptance test.
	BeforeTest(*testing.T)
	// AfterTest is executed after each acceptance test.
	AfterTest(*testing.T)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions(*testing.T) []goleak.Option

	// WriteToSource receives a slice of records that should be prepared in the
	// 3rd party system so that the source will read them. The slice will be
	// used to verify the source connector can successfully execute reads.
	// It is discouraged for the driver to change the record slice, unless there
	// is no way to write the records to the 3rd party system, then the slice
	// should be modified to reflect the expected records a source should read.
	WriteToSource(*testing.T, *[]Record)
	// ReadFromDestination should populate the slice with the records that were
	// written to the destination. The slice will be used to verify the
	// destination has successfully executed writes.
	// The capacity of the slice is equal to the number of records that the test
	// expects to have been written to the destination.
	ReadFromDestination(*testing.T, *[]Record)
}

// ConfigurableAcceptanceTestDriver is the default implementation of
// AcceptanceTestDriver. It provides a convenient way of configuring the driver
// without the need of implementing a custom driver from scratch.
type ConfigurableAcceptanceTestDriver struct {
	Config ConfigurableAcceptanceTestDriverConfig
}

// ConfigurableAcceptanceTestDriverConfig contains the configuration for
// ConfigurableAcceptanceTestDriver.
type ConfigurableAcceptanceTestDriverConfig struct {
	// Connector is the connector to be tested.
	Connector Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig map[string]string
	// DestinationConfig should be a valid config for a destination connector,
	// writing to the same location as the source will read from.
	DestinationConfig map[string]string

	// BeforeTest is executed before each acceptance test.
	BeforeTest func(t *testing.T)
	// AfterTest is executed after each acceptance test.
	AfterTest func(t *testing.T)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions []goleak.Option

	// Skip is a slice of regular expressions used to identify tests that should
	// be skipped. The full test name will be matched against all regular
	// expressions and the test will be skipped if a match is found.
	Skip []string
}

func (d ConfigurableAcceptanceTestDriver) Connector() Connector {
	return d.Config.Connector
}

func (d ConfigurableAcceptanceTestDriver) SourceConfig(*testing.T) map[string]string {
	return d.Config.SourceConfig
}

func (d ConfigurableAcceptanceTestDriver) DestinationConfig(*testing.T) map[string]string {
	return d.Config.DestinationConfig
}

func (d ConfigurableAcceptanceTestDriver) BeforeTest(t *testing.T) {
	// before test check if the test should be skipped
	d.Skip(t)

	if d.Config.BeforeTest != nil {
		d.Config.BeforeTest(t)
	}
}

func (d ConfigurableAcceptanceTestDriver) AfterTest(t *testing.T) {
	if d.Config.AfterTest != nil {
		d.Config.AfterTest(t)
	}
}

func (d ConfigurableAcceptanceTestDriver) Skip(t *testing.T) {
	var skipRegexs []*regexp.Regexp
	for _, skipRegex := range d.Config.Skip {
		r := regexp.MustCompile(skipRegex)
		skipRegexs = append(skipRegexs, r)
	}

	for _, skipRegex := range skipRegexs {
		if skipRegex.MatchString(t.Name()) {
			t.Skip(fmt.Sprintf("caller requested to skip tests that match the regex %q", skipRegex.String()))
		}
	}
}

func (d ConfigurableAcceptanceTestDriver) GoleakOptions(_ *testing.T) []goleak.Option {
	return d.Config.GoleakOptions
}

// WriteToSource by default opens the destination and writes records to the
// destination. It is expected that the destination is writing to the same
// location the source is reading from. If the connector does not implement a
// destination the function will fail the test.
func (d ConfigurableAcceptanceTestDriver) WriteToSource(t *testing.T, records *[]Record) {
	if d.Connector().NewDestination == nil {
		t.Fatal("connector is missing the field NewDestination, either implement the destination or overwrite the driver method Write")
	}

	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// writing something to the destination should result in the same record
	// being produced by the source
	dest := d.Connector().NewDestination()
	err := dest.Configure(ctx, d.DestinationConfig(t))
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	// try to write using WriteAsync and fallback to Write if it's not supported
	err = d.writeAsync(ctx, dest, *records)
	if errors.Is(err, ErrUnimplemented) {
		err = d.write(ctx, dest, *records)
	}
	is.NoErr(err)

	cancel() // cancel context to simulate stop
	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

// ReadFromDestination by default opens the source and reads all records from
// the source. It is expected that the destination is writing to the same
// location the source is reading from. If the connector does not implement a
// source the function will fail the test.
func (d ConfigurableAcceptanceTestDriver) ReadFromDestination(t *testing.T, records *[]Record) {
	if d.Connector().NewSource == nil {
		t.Fatal("connector is missing the field NewSource, either implement the source or overwrite the driver method Read")
	}

	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// writing something to the destination should result in the same record
	// being produced by the source
	src := d.Connector().NewSource()
	err := src.Configure(ctx, d.SourceConfig(t))
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	for i := 0; i < cap(*records); i++ {
		// now try to read from the source
		readCtx, readCancel := context.WithTimeout(ctx, time.Second*5)
		defer readCancel()

		r, err := src.Read(readCtx)
		is.NoErr(err)
		*records = append(*records, r)
	}

	// try another read, there should be nothing so timeout after 1 second
	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	defer readCancel()
	_, err = src.Read(readCtx)

	is.True(errors.Is(err, context.DeadlineExceeded))

	cancel() // cancel context to simulate stop
	err = src.Teardown(context.Background())
	is.NoErr(err)
}

// writeAsync writes records to destination using Destination.WriteAsync.
func (d ConfigurableAcceptanceTestDriver) writeAsync(ctx context.Context, dest Destination, records []Record) error {
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
	if err != nil {
		return err
	}

	// TODO create timeout for wait to prevent deadlock for badly written connectors
	waitForAck.Wait()
	if ackErr != nil {
		return ackErr
	}

	// records were successfully written
	return nil
}

// write writes records to destination using Destination.Write.
func (d ConfigurableAcceptanceTestDriver) write(ctx context.Context, dest Destination, records []Record) error {
	for _, r := range records {
		err := dest.Write(ctx, r)
		if err != nil {
			return err
		}
	}

	// flush to make sure the records get written to the destination, but allow
	// it to be unimplemented
	err := dest.Flush(ctx)
	if err != nil && !errors.Is(err, ErrUnimplemented) {
		return err
	}

	// records were successfully written
	return nil
}

type acceptanceTest struct {
	driver AcceptanceTestDriver
	rand   *rand.Rand
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
			a.driver.BeforeTest(t)
			t.Cleanup(func() { a.driver.AfterTest(t) })

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
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

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

	isParamsCorrect := func(t *testing.T, params map[string]Parameter) {
		is := is.NewRelaxed(t)
		paramNameRegex := regexp.MustCompile(`^[a-zA-Z0-9.]+$`)

		for name, p := range params {
			is.True(paramNameRegex.MatchString(name)) // parameter contains invalid characters
			is.True(p.Description != "")              // parameter description is empty
		}
	}

	t.Run("sourceParams", func(t *testing.T) {
		a.skipIfNoSource(t)
		is := is.NewRelaxed(t)

		// we enforce that there is at least 1 parameter, any real source will
		// require some configuration
		is.True(spec.SourceParams != nil)   // Specification.SourceParams is missing
		is.True(len(spec.SourceParams) > 0) // Specification.SourceParams is empty

		isParamsCorrect(t, spec.SourceParams)
	})

	t.Run("destinationParams", func(t *testing.T) {
		a.skipIfNoDestination(t)
		is := is.NewRelaxed(t)

		// we enforce that there is at least 1 parameter, any real destination
		// will require some configuration
		is.True(spec.DestinationParams != nil)   // Specification.DestinationParams is missing
		is.True(len(spec.DestinationParams) > 0) // Specification.DestinationParams is empty

		isParamsCorrect(t, spec.DestinationParams)
	})

	semverRegex := regexp.MustCompile(`v([0-9]+)(\.[0-9]+)?(\.[0-9]+)?` +
		`(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?` +
		`(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?`)
	is.True(semverRegex.MatchString(spec.Version)) // Specification.Version is not a valid semantic version (vX.Y.Z)
}

func (a acceptanceTest) TestSource_Configure_Success(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	source := a.driver.Connector().NewSource()
	err := source.Configure(ctx, a.driver.SourceConfig(t))
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
	origCfg := a.driver.SourceConfig(t)

	for name, p := range spec.SourceParams {
		if p.Required {
			// removing the required parameter from the config should provoke an error
			t.Run(fmt.Sprintf("without required param: %s", name), func(t *testing.T) {
				haveCfg := a.cloneConfig(origCfg)
				delete(haveCfg, name)

				is.Equal(len(haveCfg)+1, len(origCfg)) // source config does not contain required parameter, please check the test setup

				source := a.driver.Connector().NewSource()
				err := source.Configure(ctx, haveCfg)
				is.True(err != nil)

				err = source.Teardown(ctx)
				is.NoErr(err)
			})
		}
	}
}

func (a acceptanceTest) TestSource_Open_ResumeAtPosition(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	// write expectations
	want := a.generateRecords(10)
	a.driver.WriteToSource(t, &want)

	// read all records, but stop acking them after we read half of them
	var lastPosition Position
	var lastIndex int
	for i := 0; i < len(want); i++ {
		readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		got, err := source.Read(readCtx)
		is.NoErr(err)

		if i < len(want)/2 {
			err = source.Ack(ctx, got.Position)
			is.NoErr(err)
			lastPosition = got.Position
			lastIndex = i
		}
	}
	sourceCleanup()

	// At this point we have read all records but acked only half and stopped
	// the connector. We should be able to resume from the last record that was
	// not acked.

	source, sourceCleanup = a.openSource(ctx, t, lastPosition) // listen from position
	defer sourceCleanup()

	for i := lastIndex + 1; i < len(want); i++ {
		readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		got, err := source.Read(readCtx)
		is.NoErr(err)

		err = source.Ack(ctx, got.Position)
		is.NoErr(err)

		want[i].Position = got.Position   // position can't be determined in advance
		want[i].CreatedAt = got.CreatedAt // created at can't be determined in advance

		// TODO do a smarter comparison that checks fields separately (e.g. metadata, position etc.)
		// TODO ensure position is unique
		// TODO ensure we check metadata and mark it as successful or skipped if it's not (don't fail test because of metadata)
		is.Equal(want[i], got)
	}
}

func (a acceptanceTest) TestSource_Read_Success(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	positions := make(map[string]bool)
	isUniquePosition := func(t *testing.T, p Position) {
		is := is.New(t)
		is.True(!positions[string(p)])
		positions[string(p)] = true
	}

	// write expectation before source exists
	want := a.generateRecords(10)
	a.driver.WriteToSource(t, &want)

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	t.Run("snapshot", func(t *testing.T) {
		is := is.New(t)
		for i := 0; i < len(want); i++ {
			// now try to read from the source
			readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()

			got, err := source.Read(readCtx)
			is.NoErr(err)
			isUniquePosition(t, got.Position)

			want[i].Position = got.Position   // position can't be determined in advance
			want[i].CreatedAt = got.CreatedAt // created at can't be determined in advance

			// TODO do a smarter comparison that checks fields separately (e.g. metadata, position etc.)
			// TODO ensure position is unique
			// TODO ensure we check metadata and mark it as successful or skipped if it's not (don't fail test because of metadata)
			is.Equal(want[i], got)
		}
	})

	// while connector is running write more data and make sure the connector
	// detects it
	want = a.generateRecords(20)
	a.driver.WriteToSource(t, &want)

	t.Run("cdc", func(t *testing.T) {
		is := is.New(t)
		for i := 0; i < len(want); i++ {
			// now try to read from the source
			readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()

			got, err := source.Read(readCtx)
			is.NoErr(err)
			isUniquePosition(t, got.Position)

			want[i].Position = got.Position   // position can't be determined in advance
			want[i].CreatedAt = got.CreatedAt // created at can't be determined in advance

			// TODO do a smarter comparison that checks fields separately (e.g. metadata, position etc.)
			// TODO ensure position is unique
			// TODO ensure we check metadata and mark it as successful or skipped if it's not (don't fail test because of metadata)
			is.Equal(want[i], got)
		}
	})
}

func (a acceptanceTest) TestSource_Read_Timeout(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := source.Read(readCtx)
	is.Equal(r, Record{}) // record should be empty
	is.True(errors.Is(err, context.DeadlineExceeded))
}

func (a acceptanceTest) TestDestination_Configure_Success(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	dest := a.driver.Connector().NewDestination()
	err := dest.Configure(ctx, a.driver.DestinationConfig(t))
	is.NoErr(err)

	// calling Teardown after Configure is valid and happens when connector is created
	err = dest.Teardown(ctx)
	is.NoErr(err)
}

func (a acceptanceTest) TestDestination_Configure_RequiredParams(t *testing.T) {
	a.skipIfNoSpecification(t)
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := context.Background()

	spec := a.driver.Connector().NewSpecification()
	origCfg := a.driver.DestinationConfig(t)

	for name, p := range spec.DestinationParams {
		if p.Required {
			// removing the required parameter from the config should provoke an error
			t.Run(name, func(t *testing.T) {
				haveCfg := a.cloneConfig(origCfg)
				delete(haveCfg, name)

				is.Equal(len(haveCfg)+1, len(origCfg)) // destination config does not contain required parameter, please check the test setup

				dest := a.driver.Connector().NewDestination()
				err := dest.Configure(ctx, haveCfg)
				is.True(err != nil)

				err = dest.Teardown(ctx)
				is.NoErr(err)
			})
		}
	}
}

func (a acceptanceTest) TestDestination_WriteOrWriteAsync(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	dest, cleanup := a.openDestination(ctx, t)
	defer cleanup()

	writeCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	errWrite := dest.Write(writeCtx, a.generateRecord())

	writeCtx, cancel = context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	errWriteAsync := dest.WriteAsync(writeCtx, a.generateRecord(), func(err error) error { return nil })

	is.True((errors.Is(errWrite, ErrUnimplemented)) != (errors.Is(errWriteAsync, ErrUnimplemented))) // either Write or WriteAsync should be implemented, not both

	// Flush in case it's an async write and the connector expects this call
	err := dest.Flush(ctx)
	if !errors.Is(err, ErrUnimplemented) {
		is.NoErr(err)
	}
}

func (a acceptanceTest) TestDestination_Write_Success(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	dest, cleanup := a.openDestination(ctx, t)
	defer cleanup()

	want := a.generateRecords(20)

	for i, r := range want {
		writeCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		err := dest.Write(writeCtx, r)
		if i == 0 && errors.Is(err, ErrUnimplemented) {
			t.Skip("Write not implemented")
		}
		is.NoErr(err)
	}

	// Flush is optional, we allow it to be unimplemented
	err := dest.Flush(ctx)
	if !errors.Is(err, ErrUnimplemented) {
		is.NoErr(err)
	}

	got := make([]Record, 0, len(want))
	a.driver.ReadFromDestination(t, &got)

	is.Equal(len(got), len(want)) // destination didn't write expected number of records
	for i := range want {
		want[i].Position = got[i].Position   // position can't be determined in advance
		want[i].CreatedAt = got[i].CreatedAt // created at can't be determined in advance

		is.Equal(want[i], got[i]) // records did not match
	}
}

func (a acceptanceTest) TestDestination_WriteAsync_Success(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	dest, cleanup := a.openDestination(ctx, t)
	defer cleanup()

	want := a.generateRecords(20)

	var ackWg sync.WaitGroup
	for i, r := range want {
		writeCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		ackWg.Add(1)
		err := dest.WriteAsync(writeCtx, r, func(err error) error {
			defer ackWg.Done()
			return err // TODO check error, but not here, we might not be in the right goroutine
		})
		if i == 0 && errors.Is(err, ErrUnimplemented) {
			t.Skip("WriteAsync not implemented")
		}
		is.NoErr(err)
	}

	err := dest.Flush(ctx)
	is.NoErr(err)

	// wait for acks to get called
	// TODO timeout if it takes too long
	ackWg.Done()

	got := make([]Record, 0, len(want))
	a.driver.ReadFromDestination(t, &got)

	is.Equal(len(got), len(want)) // destination didn't write expected number of records
	for i := range want {
		want[i].Position = got[i].Position   // position can't be determined in advance
		want[i].CreatedAt = got[i].CreatedAt // created at can't be determined in advance

		is.Equal(want[i], got[i])
	}
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

func (a acceptanceTest) openSource(ctx context.Context, t *testing.T, pos Position) (source Source, cleanup func()) {
	is := is.New(t)

	source = a.driver.Connector().NewSource()
	err := source.Configure(ctx, a.driver.SourceConfig(t))
	is.NoErr(err)

	openCtx, cancelOpenCtx := context.WithCancel(ctx)
	err = source.Open(openCtx, pos)
	is.NoErr(err)

	// make sure connector is cleaned up only once
	var cleanupOnce sync.Once
	cleanup = func() {
		cleanupOnce.Do(func() {
			cancelOpenCtx()
			err = source.Teardown(ctx)
			is.NoErr(err)
		})
	}

	return source, cleanup
}

func (a acceptanceTest) openDestination(ctx context.Context, t *testing.T) (dest Destination, cleanup func()) {
	is := is.New(t)

	dest = a.driver.Connector().NewDestination()
	err := dest.Configure(ctx, a.driver.DestinationConfig(t))
	is.NoErr(err)

	openCtx, cancelOpenCtx := context.WithCancel(ctx)
	err = dest.Open(openCtx)
	is.NoErr(err)

	// make sure connector is cleaned up only once
	var cleanupOnce sync.Once
	cleanup = func() {
		cleanupOnce.Do(func() {
			cancelOpenCtx()
			err = dest.Teardown(ctx)
			is.NoErr(err)
		})
	}

	return dest, cleanup
}

func (a acceptanceTest) generateRecords(count int) []Record {
	records := make([]Record, count)
	for i := range records {
		records[i] = a.generateRecord()
	}
	return records
}

func (a acceptanceTest) generateRecord() Record {
	return Record{
		Position:  Position(a.randString(32)), // position doesn't matter, as long as it's unique
		Metadata:  nil,                        // TODO metadata is optional so don't enforce it to be written, still create it
		CreatedAt: time.Unix(0, a.rand.Int63()),
		Key:       RawData(a.randString(32)), // TODO try structured data as well
		Payload:   RawData(a.randString(32)), // TODO try structured data as well
	}
}

// randString generates a random string of length n.
// (source: https://stackoverflow.com/a/31832326)
func (a acceptanceTest) randString(n int) string {
	const letterBytes = ` !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_abcdefghijklmnopqrstuvwxyz`
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	sb := strings.Builder{}
	sb.Grow(n)
	// src.Int63() generates 63 random bits, enough for letterIdxMax characters
	for i, cache, remain := n-1, a.rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = a.rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return sb.String()
}
