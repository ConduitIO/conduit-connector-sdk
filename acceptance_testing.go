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

/*

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jpillora/backoff"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"go.uber.org/goleak"
)

// AcceptanceTest is the acceptance test that all connector implementations
// should pass. It should manually be called from a test case in each
// implementation:
//
//	func TestAcceptance(t *testing.T) {
//	    // set up test dependencies ...
//	    sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
//	        Config: sdk.ConfigurableAcceptanceTestDriverConfig{
//	            Connector: myConnector,
//	            SourceConfig: config.Config{...},      // valid source config
//	            DestinationConfig: config.Config{...}, // valid destination config
//	        },
//	    })
//	}
func AcceptanceTest(t *testing.T, driver AcceptanceTestDriver) {
	acceptanceTest{
		driver: driver,
	}.Test(t)
}

// AcceptanceTestDriver is the object that each test uses for fetching the
// connector and its configurations. The SDK provides a default implementation
// ConfigurableAcceptanceTestDriver that should fit most use cases. In case more
// flexibility is needed you can create your own driver, include the default
// driver in the struct and override methods as needed.
//
//nolint:interfacebloat // We need a comprehensive interface for acceptance tests.
type AcceptanceTestDriver interface {
	// Context returns the context to use in tests.
	Context() context.Context
	// Connector is the connector to be tested.
	Connector() Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig(*testing.T) config.Config
	// DestinationConfig should be a valid config for a destination connector,
	// writing to the same location as the source will read from.
	DestinationConfig(*testing.T) config.Config

	// BeforeTest is executed before each acceptance test.
	BeforeTest(*testing.T)
	// AfterTest is executed after each acceptance test.
	AfterTest(*testing.T)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions(*testing.T) []goleak.Option

	// GenerateRecord will generate a new Record for a certain Operation. It's
	// the responsibility of the AcceptanceTestDriver implementation to provide
	// records with appropriate contents (e.g. appropriate type of payload).
	// The generated record will contain mixed data types in the field Key and
	// Payload (i.e. RawData and StructuredData), unless configured otherwise
	// (see ConfigurableAcceptanceTestDriverConfig.GenerateDataType).
	GenerateRecord(*testing.T, opencdc.Operation) opencdc.Record

	// WriteToSource receives a slice of records that should be prepared in the
	// 3rd party system so that the source will read them. The returned slice
	// will be used to verify the source connector can successfully execute
	// reads.
	// It is encouraged for the driver to return the same slice, unless there is
	// no way to write the records to the 3rd party system, then the returning
	// slice should contain the expected records a source should read.
	WriteToSource(*testing.T, []opencdc.Record) []opencdc.Record
	// ReadFromDestination should return a slice with the records that were
	// written to the destination. The slice will be used to verify the
	// destination has successfully executed writes.
	// The parameter contains records that were actually written to the
	// destination. These will be compared to the returned slice of records. It
	// is encouraged for the driver to only touch the input records to change
	// the order of records and to not change the records themselves.
	ReadFromDestination(*testing.T, []opencdc.Record) []opencdc.Record

	// ReadTimeout controls the time the test should wait for a read operation
	// to return before it considers the operation as failed.
	ReadTimeout() time.Duration
	// WriteTimeout controls the time the test should wait for a write operation
	// to return before it considers the operation as failed.
	WriteTimeout() time.Duration
}

// ConfigurableAcceptanceTestDriver is the default implementation of
// AcceptanceTestDriver. It provides a convenient way of configuring the driver
// without the need of implementing a custom driver from scratch.
type ConfigurableAcceptanceTestDriver struct {
	Config ConfigurableAcceptanceTestDriverConfig
	rand   *rand.Rand
}

// ConfigurableAcceptanceTestDriverConfig contains the configuration for
// ConfigurableAcceptanceTestDriver.
type ConfigurableAcceptanceTestDriverConfig struct {
	// Context is the context to use in tests. The default is a context with a
	// logger that writes to the test output.
	//nolint:containedctx // We are using this as a configuration option.
	Context context.Context
	// Connector is the connector to be tested.
	Connector Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig config.Config
	// DestinationConfig should be a valid config for a destination connector,
	// writing to the same location as the source will read from.
	DestinationConfig config.Config

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

	// GenerateDataType controls which Data type will be generated in test
	// records. The default is GenerateMixedData which will produce both RawData
	// and StructuredData. To generate only one type of data set this field to
	// GenerateRawData or GenerateStructuredData.
	GenerateDataType GenerateDataType

	// ReadTimeout controls the time the test should wait for a read operation
	// to return a record before it considers the operation as failed. The
	// default timeout is 5 seconds. This value should be changed only if there
	// is a good reason (uncontrollable limitations of the 3rd party system).
	ReadTimeout time.Duration
	// WriteTimeout controls the time the test should wait for a write operation
	// to return a record before it considers the operation as failed. The
	// default timeout is 5 seconds. This value should be changed only if there
	// is a good reason (uncontrollable limitations of the 3rd party system).
	WriteTimeout time.Duration
}

// GenerateDataType is used in acceptance tests to control what data type will
// be generated.
type GenerateDataType int

const (
	GenerateMixedData GenerateDataType = iota
	GenerateRawData
	GenerateStructuredData
)

func (d ConfigurableAcceptanceTestDriver) Context() context.Context {
	return d.Config.Context
}

func (d ConfigurableAcceptanceTestDriver) Connector() Connector {
	return d.Config.Connector
}

func (d ConfigurableAcceptanceTestDriver) SourceConfig(*testing.T) config.Config {
	return maps.Clone(d.Config.SourceConfig)
}

func (d ConfigurableAcceptanceTestDriver) DestinationConfig(*testing.T) config.Config {
	return maps.Clone(d.Config.DestinationConfig)
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
	for _, skipRegex := range d.Config.Skip {
		r := regexp.MustCompile(skipRegex)
		if r.MatchString(t.Name()) {
			t.Skipf("caller requested to skip tests that match the regex %q", skipRegex)
		}
	}
}

func (d ConfigurableAcceptanceTestDriver) GoleakOptions(_ *testing.T) []goleak.Option {
	return d.Config.GoleakOptions
}

func (d ConfigurableAcceptanceTestDriver) GenerateRecord(t *testing.T, op opencdc.Operation) opencdc.Record {
	// TODO we currently only generate records with operation "create" and
	//  "snapshot", because this is the only operation we know all connectors
	//  should be able to handle. We should make acceptance tests more
	//  sophisticated and also check how the connector handles other operations,
	//  specifically "update" and "delete".
	//  Once we generate different operations we need to adjust how we compare
	//  records!
	return opencdc.Record{
		Position:  opencdc.Position(d.randString(32)), // position doesn't matter, as long as it's unique
		Operation: op,
		Metadata:  map[string]string{d.randString(32): d.randString(32)},
		Key:       d.GenerateData(t),
		Payload: opencdc.Change{
			Before: nil,
			After:  d.GenerateData(t),
		},
	}
}

// GenerateData generates either RawData or StructuredData depending on the
// configured data type (see
// ConfigurableAcceptanceTestDriverConfig.GenerateDataType).
func (d ConfigurableAcceptanceTestDriver) GenerateData(t *testing.T) opencdc.Data {
	rand := d.getRand()

	gen := d.Config.GenerateDataType
	if gen == GenerateMixedData {
		gen = GenerateDataType(1 + (rand.Int63() % 2))
	}

	//nolint:exhaustive // The missing case is GenerateMixedData, which is handled above.
	switch gen {
	case GenerateRawData:
		return opencdc.RawData(d.randString(rand.Intn(1024) + 32))
	case GenerateStructuredData:
		data := opencdc.StructuredData{}
		for {
			data[d.randString(rand.Intn(1024)+32)] = d.GenerateValue(t)
			if rand.Int63()%2 == 0 {
				// 50% chance we stop adding fields
				break
			}
		}
		return data
	default:
		t.Fatalf("invalid config value for GenerateDataType %q", d.Config.GenerateDataType)
		return nil
	}
}

// GenerateValue generates a random value of a random builtin type.
func (d ConfigurableAcceptanceTestDriver) GenerateValue(t *testing.T) interface{} {
	rand := d.getRand()

	const (
		typeBool = iota
		typeInt
		typeFloat64
		typeString

		typeCount // typeCount needs to be last and contains the number of constants
	)

	switch rand.Int() % typeCount {
	case typeBool:
		return rand.Int63()%2 == 0
	case typeInt:
		i := rand.Int()
		if rand.Int63()%2 == 0 {
			return -i - 1 // negative
		}
		return i
	case typeFloat64:
		i := rand.Float64()
		if rand.Int63()%2 == 0 {
			return -i - 1 // negative
		}
		return i
	case typeString:
		return d.randString(rand.Intn(1024) + 32)
	}

	panic("bug in random value generation")
}

// getRand returns a new rand.Rand, with its seed set to current Unix time in nanoseconds.
// The method exists so that users of ConfigurableAcceptanceTestDriver do not have to do it themselves.
// (Most of the time, a custom configuration is not needed anyway.)
func (d ConfigurableAcceptanceTestDriver) getRand() *rand.Rand {
	if d.rand == nil {
		d.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return d.rand
}

// randString generates a random string of length n.
// (source: https://stackoverflow.com/a/31832326)
func (d ConfigurableAcceptanceTestDriver) randString(n int) string {
	const letterBytes = ` !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_abcdefghijklmnopqrstuvwxyz`
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	sb := strings.Builder{}
	sb.Grow(n)
	// src.Int63() generates 63 random bits, enough for letterIdxMax characters
	for i, cache, remain := n-1, d.getRand().Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = d.getRand().Int63(), letterIdxMax
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

// WriteToSource by default opens the destination and writes records to the
// destination. It is expected that the destination is writing to the same
// location the source is reading from. If the connector does not implement a
// destination the function will fail the test.
func (d ConfigurableAcceptanceTestDriver) WriteToSource(t *testing.T, records []opencdc.Record) []opencdc.Record {
	if d.Connector().NewDestination == nil {
		t.Fatal("connector is missing the field NewDestination, either implement the destination or overwrite the driver method Write")
	}

	is := is.New(t)
	rootCtx := d.Context()
	if rootCtx == nil {
		logger := zerolog.New(zerolog.NewTestWriter(t))
		rootCtx = logger.WithContext(context.Background())
	}
	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	// writing something to the destination should result in the same record
	// being produced by the source
	dest := d.Connector().NewDestination()
	err := dest.Configure(ctx, d.DestinationConfig(t))
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	defer func() {
		cancel()                     // cancel context to simulate stop
		err = dest.Teardown(rootCtx) // use rootCtx as it's not cancelled
		is.NoErr(err)
	}()

	_, err = dest.Write(ctx, records)
	is.NoErr(err)

	return records
}

// ReadFromDestination by default opens the source and reads all records from
// the source. It is expected that the destination is writing to the same
// location the source is reading from. If the connector does not implement a
// source the function will fail the test.
func (d ConfigurableAcceptanceTestDriver) ReadFromDestination(t *testing.T, records []opencdc.Record) []opencdc.Record {
	if d.Connector().NewSource == nil {
		t.Fatal("connector is missing the field NewSource, either implement the source or overwrite the driver method Read")
	}

	is := is.New(t)
	rootCtx := d.Context()
	if rootCtx == nil {
		logger := zerolog.New(zerolog.NewTestWriter(t))
		rootCtx = logger.WithContext(context.Background())
	}
	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	// writing something to the destination should result in the same record
	// being produced by the source
	src := d.Connector().NewSource()
	err := src.Configure(ctx, d.SourceConfig(t))
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	defer func() {
		cancel()                    // first cancel context to simulate stop
		err = src.Teardown(rootCtx) // use rootCtx as it's not cancelled
		is.NoErr(err)
	}()

	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second, // 8 tries
	}

	output := make([]opencdc.Record, 0, len(records))
	for i := 0; i < cap(output); i++ {
		// now try to read from the source
		readCtx, readCancel := context.WithTimeout(ctx, d.ReadTimeout())
		defer readCancel()

		for {
			r, err := src.Read(readCtx)
			if errors.Is(err, ErrBackoffRetry) {
				// the plugin wants us to retry reading later
				t.Logf("source returned backoff retry error, backing off for %v", b.Duration())
				select {
				case <-readCtx.Done():
					// store error and let it fail
					err = readCtx.Err()
				case <-time.After(b.Duration()):
					continue
				}
			}
			is.NoErr(err) // could not retrieve a record using the source, please check manually if the destination has successfully written the records
			output = append(output, r)
			break
		}
	}

	// try another read, there should be nothing so timeout after 1 second
	readCtx, readCancel := context.WithTimeout(ctx, d.ReadTimeout())
	defer readCancel()
	r, err := src.Read(readCtx)
	is.Equal(opencdc.Record{}, r) // record should be empty
	is.True(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBackoffRetry))

	return output
}

func (d ConfigurableAcceptanceTestDriver) ReadTimeout() time.Duration {
	if d.Config.ReadTimeout == 0 {
		return time.Second * 5
	}
	return d.Config.ReadTimeout
}

func (d ConfigurableAcceptanceTestDriver) WriteTimeout() time.Duration {
	if d.Config.WriteTimeout == 0 {
		return time.Second * 5
	}
	return d.Config.WriteTimeout
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
			a.driver.BeforeTest(t)
			defer a.driver.AfterTest(t)

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
	defer a.verifyGoleaks(t)

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

	// allow (devel) as the version to match default behavior from runtime/debug
	if spec.Version != "(devel)" {
		semverRegex := regexp.MustCompile(`v([0-9]+)(\.[0-9]+)?(\.[0-9]+)?` +
			`(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?` +
			`(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?`)
		is.True(semverRegex.MatchString(spec.Version)) // Specification.Version is not a valid semantic version "vX.Y.Z" or "(devel)"
	}
}

func (a acceptanceTest) TestSource_Parameters_Success(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	defer a.verifyGoleaks(t)

	source := a.driver.Connector().NewSource()
	params := source.Parameters()

	// we enforce that there is at least 1 parameter, any real source will
	// require some configuration
	is.True(params != nil)   // Source.Parameters() shouldn't return nil
	is.True(len(params) > 0) // Source.Parameters() shouldn't return empty map

	paramNameRegex := regexp.MustCompile(`^[a-zA-Z0-9.]+$`)
	for name, p := range params {
		is.True(paramNameRegex.MatchString(name)) // parameter contains invalid characters
		is.True(p.Description != "")              // parameter description is empty
	}
}

func (a acceptanceTest) TestSource_Configure_Success(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := a.context(t)
	defer a.verifyGoleaks(t)

	source := a.driver.Connector().NewSource()
	err := source.Configure(ctx, a.driver.SourceConfig(t))
	is.NoErr(err)

	// calling Teardown after Configure is valid and happens when connector is created
	err = source.Teardown(ctx)
	is.NoErr(err)
}

func (a acceptanceTest) TestSource_Configure_RequiredParams(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := a.context(t)

	srcSpec := a.driver.Connector().NewSource()
	origCfg := a.driver.SourceConfig(t)

	for name, p := range srcSpec.Parameters() {
		isRequired := false
		for _, v := range p.Validations {
			if _, ok := v.(config.ValidationRequired); ok {
				isRequired = true
				break
			}
		}
		if isRequired {
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

func (a acceptanceTest) TestSource_Open_ResumeAtPositionSnapshot(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := a.context(t)
	defer a.verifyGoleaks(t)

	// Write expectations before source is started, this means the source will
	// have to first read the existing data (i.e. snapshot), but we will
	// interrupt it and try to resume.
	want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, 10))

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	// read all records, but ack only half of them
	got, err := a.readMany(ctx, t, source, len(want))
	is.NoErr(err)
	for i := 0; i < len(got)/2; i++ {
		err = source.Ack(ctx, got[i].Position)
		is.NoErr(err)
	}
	sourceCleanup()

	// we only acked half of the records, let's "forget" we saw the second half
	got = got[:len(got)/2]

	// At this point we have read all records but acked only half and stopped
	// the connector. We should be able to resume from the last record that was
	// not acked.

	lastPosition := got[len(got)-1].Position
	source, sourceCleanup = a.openSource(ctx, t, lastPosition) // listen from position
	defer sourceCleanup()

	// Some connectors can't correctly resume if the snapshot is interrupted,
	// those should restart the snapshot and start from scratch, meaning that
	// some records might be encountered twice, but at least we don't miss any
	// records. That's why we try to read all records, even though the connector
	// might return only half of them.
	gotRemaining, err := a.readMany(ctx, t, source, len(want))
	if errors.Is(err, ErrBackoffRetry) || errors.Is(err, context.DeadlineExceeded) {
		// connector stopped returning records, apparently it was able to restart
		err = nil
		got = append(got, gotRemaining...)
	} else if err == nil {
		// connector did not return an error, seems like it restarted the snapshot
		got = gotRemaining
	}
	is.NoErr(err)
	a.isEqualRecords(is, want, got)
}

func (a acceptanceTest) TestSource_Open_ResumeAtPositionCDC(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := a.context(t)
	defer a.verifyGoleaks(t)

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	// try to read something to make sure the source is initialized
	readCtx, cancel := context.WithTimeout(ctx, a.driver.ReadTimeout())
	defer cancel()
	_, err := source.Read(readCtx)
	is.True(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBackoffRetry))

	// Write expectations after source is open, this means the source is already
	// listening to ongoing changes (i.e. CDC), we will interrupt it and try to
	// resume.
	want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationCreate, 10))

	// read all records, but ack only half of them
	got, err := a.readMany(ctx, t, source, len(want))
	is.NoErr(err)
	for i := 0; i < len(got)/2; i++ {
		err = source.Ack(ctx, got[i].Position)
		is.NoErr(err)
	}
	sourceCleanup()

	// we only acked half of the records, let's "forget" we saw the second half
	got = got[:len(got)/2]

	// At this point we have read all records but acked only half and stopped
	// the connector. We should be able to resume from the last record that was
	// not acked.

	lastPosition := got[len(got)-1].Position
	source, sourceCleanup = a.openSource(ctx, t, lastPosition) // listen from position
	defer sourceCleanup()

	// all connectors should know how to resume at the position
	gotRemaining, err := a.readMany(ctx, t, source, len(want)-len(got))
	is.NoErr(err)

	got = append(got, gotRemaining...)
	a.isEqualRecords(is, want, got)
}

func (a acceptanceTest) TestSource_Read_Success(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := a.context(t)
	defer a.verifyGoleaks(t)

	positions := make(map[string]bool)
	isUniquePositions := func(t *testing.T, records []opencdc.Record) {
		is := is.New(t)
		for _, r := range records {
			is.True(!positions[string(r.Position)])
			positions[string(r.Position)] = true
		}
	}

	// write expectation before source exists
	want := a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationSnapshot, 10))

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	t.Run("snapshot", func(t *testing.T) {
		is := is.New(t)

		// now try to read from the source
		got, err := a.readMany(ctx, t, source, len(want))
		is.NoErr(err) // could not read records using the source

		isUniquePositions(t, got)
		a.isEqualRecords(is, want, got)
	})

	// while connector is running write more data and make sure the connector
	// detects it
	want = a.driver.WriteToSource(t, a.generateRecords(t, opencdc.OperationCreate, 20))

	t.Run("cdc", func(t *testing.T) {
		is := is.New(t)

		// now try to read from the source
		got, err := a.readMany(ctx, t, source, len(want))
		is.NoErr(err) // could not read records using the source

		isUniquePositions(t, got)
		a.isEqualRecords(is, want, got)
	})
}

func (a acceptanceTest) TestSource_Read_Timeout(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := a.context(t)
	defer a.verifyGoleaks(t)

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	readCtx, cancel := context.WithTimeout(ctx, a.driver.ReadTimeout())
	defer cancel()
	r, err := a.readWithBackoffRetry(readCtx, t, source)
	is.Equal(opencdc.Record{}, r) // record should be empty
	is.True(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBackoffRetry))
}

func (a acceptanceTest) TestDestination_Parameters_Success(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	defer a.verifyGoleaks(t)

	dest := a.driver.Connector().NewDestination()
	params := dest.Parameters()

	// we enforce that there is at least 1 parameter, any real destination will
	// require some configuration
	is.True(params != nil)   // Destination.Parameters() shouldn't return nil
	is.True(len(params) > 0) // Destination.Parameters() shouldn't return empty map

	paramNameRegex := regexp.MustCompile(`^[a-zA-Z0-9.]+$`)
	for name, p := range params {
		is.True(paramNameRegex.MatchString(name)) // parameter contains invalid characters
		is.True(p.Description != "")              // parameter description is empty
	}
}

func (a acceptanceTest) TestDestination_Configure_Success(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := a.context(t)
	defer a.verifyGoleaks(t)

	dest := a.driver.Connector().NewDestination()
	err := dest.Configure(ctx, a.driver.DestinationConfig(t))
	is.NoErr(err)

	// calling Teardown after Configure is valid and happens when connector is created
	err = dest.Teardown(ctx)
	is.NoErr(err)
}

func (a acceptanceTest) TestDestination_Configure_RequiredParams(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := a.context(t)

	destSpec := a.driver.Connector().NewDestination()
	origCfg := a.driver.DestinationConfig(t)

	for name, p := range destSpec.Parameters() {
		isRequired := false
		for _, v := range p.Validations {
			if _, ok := v.(config.ValidationRequired); ok {
				isRequired = true
				break
			}
		}
		if isRequired {
			// removing the required parameter from the config should provoke an error
			t.Run(name, func(t *testing.T) {
				haveCfg := a.cloneConfig(origCfg)
				delete(haveCfg, name)

				is.Equal(len(origCfg), len(haveCfg)+1) // destination config does not contain required parameter, please check the test setup

				dest := a.driver.Connector().NewDestination()
				err := dest.Configure(ctx, haveCfg)
				is.True(err != nil) // expected error if required param is removed

				err = dest.Teardown(ctx)
				is.NoErr(err)
			})
		}
	}
}

func (a acceptanceTest) TestDestination_Write_Success(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := a.context(t)
	defer a.verifyGoleaks(t)

	dest, cleanup := a.openDestination(ctx, t)
	defer cleanup()

	want := a.generateRecords(t, opencdc.OperationSnapshot, 20)

	writeCtx, cancel := context.WithTimeout(ctx, a.driver.WriteTimeout())
	defer cancel()

	n, err := dest.Write(writeCtx, want)
	is.NoErr(err)
	is.Equal(n, len(want))

	got := a.driver.ReadFromDestination(t, want)
	a.isEqualRecords(is, want, got)
}

func (a acceptanceTest) verifyGoleaks(t *testing.T) {
	opts := a.driver.GoleakOptions(t)
	// always ignore goroutine spawned in go-cache, used in conduit-commons/schema
	opts = append(opts, goleak.IgnoreAnyFunction("github.com/twmb/go-cache/cache.New[...].func1"))
	goleak.VerifyNone(t, opts...)
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

func (a acceptanceTest) cloneConfig(orig config.Config) config.Config {
	cloned := make(config.Config, len(orig))
	for k, v := range orig {
		cloned[k] = v
	}
	return cloned
}

func (a acceptanceTest) openSource(ctx context.Context, t *testing.T, pos opencdc.Position) (source Source, cleanup func()) {
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

func (a acceptanceTest) generateRecords(t *testing.T, op opencdc.Operation, count int) []opencdc.Record {
	records := make([]opencdc.Record, count)
	for i := range records {
		records[i] = a.driver.GenerateRecord(t, op)
	}
	return records
}

func (a acceptanceTest) readMany(ctx context.Context, t *testing.T, source Source, limit int) ([]opencdc.Record, error) {
	var got []opencdc.Record
	for i := 0; i < limit; i++ {
		readCtx, cancel := context.WithTimeout(ctx, a.driver.ReadTimeout())
		defer cancel()

		rec, err := a.readWithBackoffRetry(readCtx, t, source)
		if err != nil {
			return got, err // return error and whatever we have so far
		}

		got = append(got, rec)
	}
	return got, nil
}

func (a acceptanceTest) readWithBackoffRetry(ctx context.Context, t *testing.T, source Source) (opencdc.Record, error) {
	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second,
	}
	for {
		got, err := source.Read(ctx)
		if errors.Is(err, ErrBackoffRetry) {
			// the plugin wants us to retry reading later
			t.Logf("source returned backoff retry error, backing off for %v", b.Duration())
			select {
			case <-ctx.Done():
				// return error
				return opencdc.Record{}, ctx.Err()
			case <-time.After(b.Duration()):
				continue
			}
		}
		return got, err
	}
}

// isEqualRecords compares two record slices and disregards their order.
func (a acceptanceTest) isEqualRecords(is *is.I, want, got []opencdc.Record) {
	is.Equal(len(want), len(got)) // record number did not match

	if len(want) == 0 {
		return
	}

	a.sortMatchingRecords(want, got)

	if bytes.Equal(want[0].Bytes(), got[0].Payload.After.Bytes()) {
		// the payload of the actual record matches the whole record in the
		// expectation, the destination apparently writes records as a whole and
		// retrieves them as a whole in the payload
		// this is valid behavior, we need to adjust the expectations
		for i, wantRec := range want {
			want[i] = opencdc.Record{
				Position:  nil,
				Operation: opencdc.OperationSnapshot, // we allow operations Snapshot or Create
				Metadata:  nil,                       // no expectation for metadata
				Key:       got[i].Key,                // no expectation for key
				Payload: opencdc.Change{
					Before: nil,
					After:  opencdc.RawData(wantRec.Bytes()), // the payload should contain the whole expected record
				},
			}
		}
	}

	for i := range want {
		a.isEqualRecord(is, want[i], got[i])
	}
}

// sortMatchingRecords will try to reorder records in both slices in a way that
// puts the equivalent records into the same position. It uses two strategies:
//   - It starts of by sorting records based on the output of
//     Record.Payload.After.Bytes(). If the first records contain the same
//     payload bytes after that it assumes the sort is fine and returns,
//     otherwise it falls back to the next strategy.
//   - It then sorts only the want slice using the output of Record.Bytes().
//     This assumes that the destination writes whole records and not only the
//     payload. It does not check if the first records match after this.
func (a acceptanceTest) sortMatchingRecords(want, got []opencdc.Record) {
	sort.Slice(want, func(i, j int) bool {
		return string(want[i].Payload.After.Bytes()) < string(want[j].Payload.After.Bytes())
	})
	sort.Slice(got, func(i, j int) bool {
		return string(got[i].Payload.After.Bytes()) < string(got[j].Payload.After.Bytes())
	})

	// check if first record payload matches to ensure we have the right order
	if bytes.Equal(want[0].Payload.After.Bytes(), got[0].Payload.After.Bytes()) {
		return // all good
	}

	// record payloads didn't match, we assume the destination writes whole
	// records, we should sort the want records based on their bytes then
	sort.Slice(want, func(i, j int) bool {
		return string(want[i].Bytes()) < string(want[j].Bytes())
	})
}

func (a acceptanceTest) isEqualRecord(is *is.I, want, got opencdc.Record) {
	if want.Operation == opencdc.OperationSnapshot &&
		got.Operation == opencdc.OperationCreate {
		// This is a special case and we accept it. Not all connectors will
		// create records with operation "snapshot", but they will still able to
		// produce records that were written before the source was open in
		// normal CDC mode (e.g. Kafka connector).
	} else {
		is.Equal(want.Operation, got.Operation) // record operation did not match (want != got)
	}

	a.isEqualData(is, want.Key, got.Key)
	a.isEqualChange(is, want.Payload, got.Payload)

	// check metadata fields, if they are there they should match
	if len(got.Metadata) == 0 {
		is.Equal(got.Metadata, nil) // if there is no metadata the map should be nil
	} else {
		for k, wantMetadata := range want.Metadata {
			if gotMetadata, ok := got.Metadata[k]; ok {
				// only compare fields if they actually exist
				is.Equal(wantMetadata, gotMetadata) // record metadata did not match (want != got)
			}
		}
	}
}

func (a acceptanceTest) isEqualChange(is *is.I, want, got opencdc.Change) {
	a.isEqualData(is, want.Before, got.Before)
	a.isEqualData(is, want.After, got.After)
}

// isEqualData will match the two data objects in their entirety if the types
// match or only the byte slice content if types differ.
func (a acceptanceTest) isEqualData(is *is.I, want, got opencdc.Data) {
	_, wantIsRaw := want.(opencdc.RawData)
	_, gotIsRaw := got.(opencdc.RawData)
	_, wantIsStructured := want.(opencdc.StructuredData)
	_, gotIsStructured := got.(opencdc.StructuredData)

	if (wantIsRaw && gotIsRaw) ||
		(wantIsStructured && gotIsStructured) ||
		(want == nil || got == nil) {
		// types match or data is nil, compare them directly
		is.Equal(want, got) // data did not match (want != got)
	} else {
		// we have different types, compare content
		is.Equal(want.Bytes(), got.Bytes()) // data did not match (want != got)
	}
}

func (a acceptanceTest) context(t *testing.T) context.Context {
	ctx := a.driver.Context()
	if ctx == nil {
		// default is a context with a logger
		logger := zerolog.New(zerolog.NewTestWriter(t))
		ctx = logger.WithContext(context.Background())
	}
	return ctx
}


*/
