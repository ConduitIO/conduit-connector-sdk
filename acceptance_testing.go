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

	"github.com/jpillora/backoff"
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

	// GenerateRecord will generate a new Record. It's the responsibility
	// of the AcceptanceTestDriver implementation to provide records with
	// appropriate contents (e.g. appropriate type of payload).
	// The generated record will contain mixed data types in the field Key and
	// Payload (i.e. RawData and StructuredData), unless configured otherwise
	// (see ConfigurableAcceptanceTestDriverConfig.GenerateDataType).
	GenerateRecord(*testing.T) Record

	// WriteToSource receives a slice of records that should be prepared in the
	// 3rd party system so that the source will read them. The returned slice
	// will be used to verify the source connector can successfully execute
	// reads.
	// It is encouraged for the driver to return the same slice, unless there is
	// no way to write the records to the 3rd party system, then the returning
	// slice should contain the expected records a source should read.
	WriteToSource(*testing.T, []Record) []Record
	// ReadFromDestination should return a slice with the records that were
	// written to the destination. The slice will be used to verify the
	// destination has successfully executed writes.
	// The parameter contains records that were actually written to the
	// destination. These will be compared to the returned slice of records. It
	// is encouraged for the driver to only touch the input records to change
	// the order of records and to not change the records themselves.
	ReadFromDestination(*testing.T, []Record) []Record
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

	// GenerateDataType controls which Data type will be generated in test
	// records. The default is GenerateMixedData which will produce both RawData
	// and StructuredData. To generate only one type of data set this field to
	// GenerateRawData or GenerateStructuredData.
	GenerateDataType GenerateDataType
}

// GenerateDataType is used in acceptance tests to control what data type will
// be generated.
type GenerateDataType int

const (
	GenerateMixedData GenerateDataType = iota
	GenerateRawData
	GenerateStructuredData
)

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

func (d ConfigurableAcceptanceTestDriver) GenerateRecord(t *testing.T) Record {
	return Record{
		Position:  Position(d.randString(32)), // position doesn't matter, as long as it's unique
		Metadata:  map[string]string{d.randString(32): d.randString(32)},
		CreatedAt: time.Now().UTC(),
		Key:       d.GenerateData(t),
		Payload:   d.GenerateData(t),
	}
}

// GenerateData generates either RawData or StructuredData depending on the
// configured data type (see
// ConfigurableAcceptanceTestDriverConfig.GenerateDataType).
func (d ConfigurableAcceptanceTestDriver) GenerateData(t *testing.T) Data {
	rand := d.getRand()

	gen := d.Config.GenerateDataType
	if gen == GenerateMixedData {
		gen = GenerateDataType(1 + (rand.Int63() % 2))
	}

	switch gen {
	case GenerateRawData:
		return RawData(d.randString(rand.Intn(1024) + 32))
	case GenerateStructuredData:
		data := StructuredData{}
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
		typeInt8
		typeInt16
		typeInt32
		typeInt64
		typeUint
		typeUint8
		typeUint16
		typeUint32
		typeUint64
		typeFloat32
		typeFloat64
		typeString

		typeCount // typeCount needs to be last and contains the number of constants
	)

	// helper, we need a random bool in lots of cases
	randBool := func() bool {
		return rand.Int63()%2 == 0
	}

	switch rand.Int() % typeCount {
	case typeBool:
		return randBool()
	case typeInt:
		i := rand.Int()
		if randBool() {
			return -i - 1 // negative
		}
		return i
	case typeInt8:
		i := int8(rand.Int63() >> (64 - 8))
		if randBool() {
			return -i - 1 // negative
		}
		return i
	case typeInt16:
		i := int16(rand.Int63() >> (64 - 16))
		if randBool() {
			return -i - 1 // negative
		}
		return i
	case typeInt32:
		i := rand.Int31()
		if randBool() {
			return -i - 1 // negative
		}
		return i
	case typeInt64:
		i := rand.Int63()
		if randBool() {
			return -i - 1 // negative
		}
		return i
	case typeUint:
		return uint(rand.Int())
	case typeUint8:
		return uint8(rand.Int63() >> (63 - 8))
	case typeUint16:
		return uint16(rand.Int63() >> (63 - 16))
	case typeUint32:
		return rand.Uint32()
	case typeUint64:
		return rand.Uint64()
	case typeFloat32:
		return rand.Float32()
	case typeFloat64:
		return rand.Float64()
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
func (d ConfigurableAcceptanceTestDriver) WriteToSource(t *testing.T, records []Record) []Record {
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

	defer func() {
		cancel() // cancel context to simulate stop
		err = dest.Teardown(context.Background())
		is.NoErr(err)
	}()

	// try to write using WriteAsync and fallback to Write if it's not supported
	err = d.writeAsync(ctx, dest, records)
	if errors.Is(err, ErrUnimplemented) {
		err = d.write(ctx, dest, records)
	}
	is.NoErr(err)

	return records
}

// ReadFromDestination by default opens the source and reads all records from
// the source. It is expected that the destination is writing to the same
// location the source is reading from. If the connector does not implement a
// source the function will fail the test.
func (d ConfigurableAcceptanceTestDriver) ReadFromDestination(t *testing.T, records []Record) []Record {
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

	defer func() {
		cancel() // first cancel context to simulate stop
		err = src.Teardown(context.Background())
		is.NoErr(err)
	}()

	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second, // 8 tries
	}

	output := make([]Record, 0, len(records))
	for i := 0; i < cap(output); i++ {
		// now try to read from the source
		readCtx, readCancel := context.WithTimeout(ctx, time.Second*5)
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
	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	defer readCancel()
	r, err := src.Read(readCtx)
	is.Equal(Record{}, r) // record should be empty
	is.True(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBackoffRetry))

	return output
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

func (a acceptanceTest) TestSource_Open_ResumeAtPositionSnapshot(t *testing.T) {
	a.skipIfNoSource(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	// Write expectations before source is started, this means the source will
	// have to first read the existing data (i.e. snapshot), but we will
	// interrupt it and try to resume.
	want := a.driver.WriteToSource(t, a.generateRecords(t, 10))

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
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	// try to read something to make sure the source is initialized
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err := source.Read(readCtx)
	is.True(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBackoffRetry))

	// Write expectations after source is open, this means the source is already
	// listening to ongoing changes (i.e. CDC), we will interrupt it and try to
	// resume.
	want := a.driver.WriteToSource(t, a.generateRecords(t, 10))

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
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	positions := make(map[string]bool)
	isUniquePositions := func(t *testing.T, records []Record) {
		is := is.New(t)
		for _, r := range records {
			is.True(!positions[string(r.Position)])
			positions[string(r.Position)] = true
		}
	}

	// write expectation before source exists
	want := a.driver.WriteToSource(t, a.generateRecords(t, 10))

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
	want = a.driver.WriteToSource(t, a.generateRecords(t, 20))

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
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	source, sourceCleanup := a.openSource(ctx, t, nil) // listen from beginning
	defer sourceCleanup()

	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	r, err := a.readWithBackoffRetry(readCtx, t, source)
	is.Equal(Record{}, r) // record should be empty
	is.True(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrBackoffRetry))
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

func (a acceptanceTest) TestDestination_WriteOrWriteAsync(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	dest, cleanup := a.openDestination(ctx, t)
	defer cleanup()

	writeCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	errWrite := dest.Write(writeCtx, a.driver.GenerateRecord(t))

	writeCtx, cancel = context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	errWriteAsync := dest.WriteAsync(writeCtx, a.driver.GenerateRecord(t), func(err error) error { return nil })

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

	want := a.generateRecords(t, 20)

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

	got := a.driver.ReadFromDestination(t, want)
	a.isEqualRecords(is, want, got)
}

func (a acceptanceTest) TestDestination_WriteAsync_Success(t *testing.T) {
	a.skipIfNoDestination(t)
	is := is.New(t)
	ctx := context.Background()
	defer goleak.VerifyNone(t, a.driver.GoleakOptions(t)...)

	dest, cleanup := a.openDestination(ctx, t)
	defer cleanup()

	want := a.generateRecords(t, 20)

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
	ackWg.Wait()

	got := a.driver.ReadFromDestination(t, want)
	a.isEqualRecords(is, want, got)
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

func (a acceptanceTest) generateRecords(t *testing.T, count int) []Record {
	records := make([]Record, count)
	for i := range records {
		records[i] = a.driver.GenerateRecord(t)
	}
	return records
}

func (a acceptanceTest) readMany(ctx context.Context, t *testing.T, source Source, limit int) ([]Record, error) {
	var got []Record
	for i := 0; i < limit; i++ {
		readCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		rec, err := a.readWithBackoffRetry(readCtx, t, source)
		if err != nil {
			return got, err // return error and whatever we have so far
		}

		got = append(got, rec)
	}
	return got, nil
}

func (a acceptanceTest) readWithBackoffRetry(ctx context.Context, t *testing.T, source Source) (Record, error) {
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
				return Record{}, ctx.Err()
			case <-time.After(b.Duration()):
				continue
			}
		}
		return got, err
	}
}

// isEqualRecords compares two record slices and disregards their order.
func (a acceptanceTest) isEqualRecords(is *is.I, want, got []Record) {
	is.Equal(len(want), len(got)) // record number did not match

	if len(want) == 0 {
		return
	}

	// transform slices into maps so we can disregard the order
	wantMap := make(map[string]Record, len(want))
	gotMap := make(map[string]Record, len(got))
	for i := 0; i < len(want); i++ {
		wantMap[string(want[i].Payload.Bytes())] = want[i]
		gotMap[string(got[i].Payload.Bytes())] = got[i]
	}

	is.Equal(len(got), len(gotMap)) // record payloads are not unique

	for key, wantRec := range wantMap {
		gotRec, ok := gotMap[key]
		is.True(ok) // expected record not found

		a.isEqualRecord(is, wantRec, gotRec)
	}
}

func (a acceptanceTest) isEqualRecord(is *is.I, want, got Record) {
	a.isEqualData(is, want.Key, got.Key)
	a.isEqualData(is, want.Payload, got.Payload)

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

	is.True(!want.CreatedAt.After(got.CreatedAt)) // the expected CreatedAt timestamp shouldn't be after the actual CreatedAt timestamp
}

// isEqualData will match the two data objects in their entirety if the types
// match or only the byte slice content if types differ.
func (a acceptanceTest) isEqualData(is *is.I, want, got Data) {
	_, wantIsRaw := want.(RawData)
	_, gotIsRaw := got.(RawData)
	_, wantIsStructured := want.(StructuredData)
	_, gotIsStructured := got.(StructuredData)

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
