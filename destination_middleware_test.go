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
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
)

// -- DestinationWithBatch -----------------------------------------------------

func TestDestinationWithBatch_Open(t *testing.T) {
	is := is.New(t)

	dst := NewMockDestination(gomock.NewController(t))
	dst.EXPECT().Open(gomock.Any()).Return(nil)

	mw := DestinationWithBatch{
		BatchSize:  10,
		BatchDelay: 123 * time.Second,
	}
	d := mw.Wrap(dst)

	ctx := (&destinationWithBatch{}).setBatchConfig(context.Background(), DestinationWithBatch{})
	err := d.Open(ctx)
	is.NoErr(err)

	is.NoErr(err)
	is.Equal(mw, (&destinationWithBatch{}).getBatchConfig(ctx))
}

// -- DestinationWithRateLimit -------------------------------------------------

func TestDestinationWithRateLimit_Open(t *testing.T) {
	testCases := []struct {
		name        string
		middleware  DestinationWithRateLimit
		wantLimiter bool
		wantLimit   rate.Limit
		wantBurst   int
	}{{
		name:        "empty config",
		middleware:  DestinationWithRateLimit{},
		wantLimiter: false,
	}, {
		name: "custom defaults",
		middleware: DestinationWithRateLimit{
			RatePerSecond: 1.23,
			Burst:         4,
		},
		wantLimiter: true,
		wantLimit:   rate.Limit(1.23),
		wantBurst:   4,
	}, {
		name: "negative burst default",
		middleware: DestinationWithRateLimit{
			RatePerSecond: 1.23,
			Burst:         -2,
		},
		wantLimiter: true,
		wantLimit:   rate.Limit(1.23),
		wantBurst:   2, // burst will be set to ceil of rate per second
	}, {
		name: "config with values",
		middleware: DestinationWithRateLimit{
			RatePerSecond: 1.23,
			Burst:         4,
		},
		wantLimiter: true,
		wantLimit:   rate.Limit(1.23),
		wantBurst:   4,
	}, {
		name: "config with zero burst",
		middleware: DestinationWithRateLimit{
			RatePerSecond: 1.23,
			Burst:         0,
		},
		wantLimiter: true,
		wantLimit:   rate.Limit(1.23),
		wantBurst:   2, // burst will be set to ceil of rate per second
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			dst := NewMockDestination(gomock.NewController(t))
			dst.EXPECT().Open(gomock.Any()).Return(nil)

			d := tt.middleware.Wrap(dst).(*destinationWithRateLimit)

			err := d.Open(context.Background())
			is.NoErr(err)

			if !tt.wantLimiter {
				is.Equal(d.limiter, nil)
			} else {
				is.Equal(d.limiter.Limit(), tt.wantLimit)
				is.Equal(d.limiter.Burst(), tt.wantBurst)
			}
		})
	}
}

func TestDestinationWithRateLimit_Write(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	dst := NewMockDestination(gomock.NewController(t))
	dst.EXPECT().Open(gomock.Any()).Return(nil)

	d := (&DestinationWithRateLimit{
		RatePerSecond: 8,
		Burst:         2,
	}).Wrap(dst)
	err := d.Open(ctx)
	is.NoErr(err)

	recs := []opencdc.Record{{}, {}, {}, {}}

	const tolerance = time.Millisecond * 10

	expectWriteAfter := func(delay time.Duration) {
		start := time.Now()
		// expect writes of 2 records at a time because of the burst
		dst.EXPECT().Write(ctx, []opencdc.Record{{}, {}}).DoAndReturn(func(context.Context, []opencdc.Record) (int, error) {
			dur := time.Since(start)
			diff := dur - delay
			if diff < 0 {
				diff = -diff
			}
			if diff > tolerance {
				t.Fatalf("expected delay: %s, actual delay: %s, tolerance: %s", delay, dur, tolerance)
			}
			return len(recs), nil
		})
	}

	// first write of 4 records is split in two bursts of 2 records, first
	// happens instantly, the second after 250ms (2/8 seconds)
	expectWriteAfter(0)
	expectWriteAfter(250 * time.Millisecond)
	_, err = d.Write(ctx, recs)
	is.NoErr(err)

	// third and fourth writes are again delayed by 250ms each
	// the result is 8 records written in a second with bursts of at most 2 records
	expectWriteAfter(250 * time.Millisecond)
	expectWriteAfter(500 * time.Millisecond)
	_, err = d.Write(ctx, recs)
	is.NoErr(err)
}

func TestDestinationWithRateLimit_Write_CancelledContext(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	dst := NewMockDestination(gomock.NewController(t))
	dst.EXPECT().Open(gomock.Any()).Return(nil)

	underTest := (&DestinationWithRateLimit{
		RatePerSecond: 10,
	}).Wrap(dst)

	err := underTest.Open(ctx)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(ctx)
	cancel()

	_, err = underTest.Write(ctx, []opencdc.Record{{}})
	is.True(errors.Is(err, ctx.Err()))
}

// -- DestinationWithRecordFormat ----------------------------------------------

func TestDestinationWithRecordFormat_Open(t *testing.T) {
	testCases := []struct {
		name           string
		middleware     DestinationWithRecordFormat
		wantSerializer RecordSerializer
	}{{
		name:           "empty config",
		middleware:     DestinationWithRecordFormat{},
		wantSerializer: defaultSerializer,
	}, {
		name: "valid config",
		middleware: DestinationWithRecordFormat{
			RecordFormat: lang.Ptr("debezium/json"),
		},
		wantSerializer: GenericRecordSerializer{
			Converter: DebeziumConverter{
				RawDataKey: debeziumDefaultRawDataKey,
			},
			Encoder: JSONEncoder{},
		},
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			dst := NewMockDestination(gomock.NewController(t))
			dst.EXPECT().Open(gomock.Any()).Return(nil)

			d := tt.middleware.Wrap(dst).(*destinationWithRecordFormat)
			err := d.Open(context.Background())
			is.NoErr(err)

			is.Equal(d.serializer, tt.wantSerializer)
		})
	}
}

// -- DestinationWithSchemaExtraction ------------------------------------------

func TestDestinationWithSchemaExtraction_Open(t *testing.T) {
	testCases := []struct {
		name       string
		middleware DestinationWithSchemaExtraction
		have       config.Config

		wantErr            error
		wantPayloadEnabled bool
		wantKeyEnabled     bool
	}{
		{
			name: "both disabled",
			middleware: DestinationWithSchemaExtraction{
				PayloadEnabled: lang.Ptr(false),
				KeyEnabled:     lang.Ptr(false),
			},
			wantPayloadEnabled: false,
			wantKeyEnabled:     false,
		},
		{
			name: "payload enabled, key disabled",
			middleware: DestinationWithSchemaExtraction{
				PayloadEnabled: lang.Ptr(true),
				KeyEnabled:     lang.Ptr(false),
			},
			wantPayloadEnabled: true,
			wantKeyEnabled:     false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			dst := NewMockDestination(gomock.NewController(t))
			dst.EXPECT().Open(gomock.Any()).Return(nil)

			s := tt.middleware.Wrap(dst).(*destinationWithSchemaExtraction)
			err := s.Open(context.Background())
			is.NoErr(err)

			is.Equal(*s.config.PayloadEnabled, tt.wantPayloadEnabled)
			is.Equal(*s.config.KeyEnabled, tt.wantKeyEnabled)
		})
	}
}

func TestDestinationWithSchemaExtraction_Write(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)
	ctx := context.Background()

	d := (&DestinationWithSchemaExtraction{
		PayloadEnabled: lang.Ptr(true),
		KeyEnabled:     lang.Ptr(true),
	}).Wrap(dst)

	testStructuredData := opencdc.StructuredData{
		"foo":   "bar",
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}

	srd, err := avro.SerdeForType(testStructuredData)
	is.NoErr(err)
	sch, err := schema.Create(ctx, schema.TypeAvro, "TestDestinationWithSchemaExtraction_Write", []byte(srd.String()))
	is.NoErr(err)

	b, err := sch.Marshal(testStructuredData)
	is.NoErr(err)
	testRawData := opencdc.RawData(b)

	testCases := []struct {
		name   string
		record opencdc.Record
	}{{
		name: "no metadata, no key, no payload",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "metadata attached, structured key",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataKeySchemaSubject: sch.Subject,
				opencdc.MetadataKeySchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "metadata attached, raw key",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataKeySchemaSubject: sch.Subject,
				opencdc.MetadataKeySchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: testRawData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "no metadata, structured key",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be decoded"),
				After:  nil,
			},
		},
	}, {
		name: "no metadata, raw key",
		record: opencdc.Record{
			Key: testRawData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  opencdc.RawData("this should not be decoded"),
			},
		},
	}, {
		name: "metadata attached, structured payload",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: opencdc.RawData("this should not be decoded"),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
	}, {
		name: "metadata attached, raw payload (both)",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  testRawData.Clone(),
			},
		},
	}, {
		name: "metadata attached, raw payload.before, structured payload.after",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
	}, {
		name: "metadata attached, structured payload.before, raw payload.after",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testRawData.Clone(),
			},
		},
	}, {
		name: "metadata attached, raw payload.before, no payload.after",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  nil,
			},
		},
	}, {
		name: "metadata attached, no payload.before, raw payload.after",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  testRawData.Clone(),
			},
		},
	}, {
		name: "no metadata, structured payload",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be decoded"),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
	}, {
		name: "no metadata, raw payload",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  testRawData.Clone(),
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			subject, _ := tc.record.Metadata.GetKeySchemaSubject()
			version, _ := tc.record.Metadata.GetKeySchemaVersion()
			wantDecodedKey := tc.record.Key != nil &&
				bytes.Equal(tc.record.Key.Bytes(), testRawData.Bytes()) &&
				subject != "" && version != 0

			subject, _ = tc.record.Metadata.GetPayloadSchemaSubject()
			version, _ = tc.record.Metadata.GetPayloadSchemaVersion()
			wantDecodedPayloadBefore := tc.record.Payload.Before != nil &&
				bytes.Equal(tc.record.Payload.Before.Bytes(), testRawData.Bytes()) &&
				subject != "" && version != 0
			wantDecodedPayloadAfter := tc.record.Payload.After != nil &&
				bytes.Equal(tc.record.Payload.After.Bytes(), testRawData.Bytes()) &&
				subject != "" && version != 0

			wantRecord := tc.record.Clone()
			if wantDecodedKey {
				t.Logf("expect decoded key")
				wantRecord.Key = testStructuredData
			}
			if wantDecodedPayloadBefore {
				t.Logf("expect decoded payload.before")
				wantRecord.Payload.Before = testStructuredData
			}
			if wantDecodedPayloadAfter {
				t.Logf("expect decoded payload.after")
				wantRecord.Payload.After = testStructuredData
			}

			dst.EXPECT().Write(ctx, []opencdc.Record{wantRecord}).Return(1, nil)

			_, err := d.Write(ctx, []opencdc.Record{tc.record})
			is.NoErr(err)
		})
	}
}
