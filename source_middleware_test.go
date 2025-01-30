// Copyright © 2024 Meroxa, Inc.
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
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// -- SourceWithSchemaExtraction -----------------------------------------------

func TestSourceWithSchemaExtraction_SchemaType(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name          string
		schemaTypeStr string
		want          schema.Type
		wantPanicErr  error
	}{
		{
			name:          "valid avro",
			schemaTypeStr: "avro",
			want:          schema.TypeAvro,
		},
		{
			name:          "invalid: unsupported schema type",
			schemaTypeStr: "foo",
			want:          schema.TypeAvro,
			wantPanicErr:  schema.ErrUnsupportedType,
		},
		{
			name:          "invalid: uppercase",
			schemaTypeStr: "AVRO",
			want:          schema.TypeAvro,
			wantPanicErr:  schema.ErrUnsupportedType,
		},
		{
			name:          "valid: empty string defaults to avro",
			schemaTypeStr: "",
			want:          schema.TypeAvro,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			underTest := &SourceWithSchemaExtraction{SchemaTypeStr: tc.schemaTypeStr}

			if tc.wantPanicErr != nil {
				defer func() {
					r := recover()
					is.True(r != nil) // expected a panic, but none occurred

					panicErr, ok := r.(error)
					is.True(ok) // expected panic value to be an error

					if tc.wantPanicErr != nil {
						is.True(errors.Is(panicErr, tc.wantPanicErr)) // Unexpected panic error
					}
				}()
			}

			result := underTest.SchemaType()

			if tc.wantPanicErr == nil {
				is.Equal(tc.want, result)
			}
		})
	}
}

func TestSourceWithSchemaExtraction_Read(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	testStructuredData := opencdc.StructuredData{
		"foo":   "bar",
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}
	wantSchema := `{"name":"record","type":"record","fields":[{"name":"float","type":"double"},{"name":"foo","type":"string"},{"name":"long","type":"long"},{"name":"time","type":{"type":"long","logicalType":"timestamp-micros"}}]}`

	customTestSchema, err := schema.Create(ctx, schema.TypeAvro, "custom-test-schema", []byte(wantSchema))
	is.NoErr(err)

	testCases := []struct {
		name               string
		record             opencdc.Record
		wantKeySubject     string
		wantPayloadSubject string
	}{{
		name: "no key, no payload",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw key",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured key",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw payload before",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured payload before",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw payload after",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured payload after",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all structured",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all raw",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "key raw payload structured",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: nil,
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "key structured payload raw",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all structured with collection",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "foo",
			},
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "foo.key",
		wantPayloadSubject: "foo.payload",
	}, {
		name: "all structured with collection and predefined schema",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataCollection:           "foo",
				opencdc.MetadataKeySchemaSubject:     customTestSchema.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(customTestSchema.Version),
				opencdc.MetadataPayloadSchemaSubject: customTestSchema.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(customTestSchema.Version),
			},
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     customTestSchema.Subject,
		wantPayloadSubject: customTestSchema.Subject,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			src := NewMockSource(ctrl)

			s := (&SourceWithSchemaExtraction{
				SchemaTypeStr:  schema.TypeAvro.String(),
				PayloadEnabled: lang.Ptr(true),
				PayloadSubject: lang.Ptr("payload"),
				KeyEnabled:     lang.Ptr(true),
				KeySubject:     lang.Ptr("key"),
			}).Wrap(src)

			src.EXPECT().ReadN(ctx, 1).Return([]opencdc.Record{tc.record}, nil)

			var wantKey, wantPayloadBefore, wantPayloadAfter opencdc.Data
			if tc.record.Key != nil {
				wantKey = tc.record.Key.Clone()
			}
			if tc.record.Payload.Before != nil {
				wantPayloadBefore = tc.record.Payload.Before.Clone()
			}
			if tc.record.Payload.After != nil {
				wantPayloadAfter = tc.record.Payload.After.Clone()
			}

			gotN, err := s.ReadN(ctx, 1)
			is.NoErr(err)
			is.Equal(1, len(gotN))

			got := gotN[0]
			gotKey := got.Key
			gotPayloadBefore := got.Payload.Before
			gotPayloadAfter := got.Payload.After

			if _, ok := wantKey.(opencdc.StructuredData); ok {
				subject, err := got.Metadata.GetKeySchemaSubject()
				is.NoErr(err)
				version, err := got.Metadata.GetKeySchemaVersion()
				is.NoErr(err)

				is.Equal(subject, tc.wantKeySubject)
				is.Equal(version, 1)

				sch, err := schema.Get(ctx, subject, version)
				is.NoErr(err)

				is.Equal("", cmp.Diff(wantSchema, string(sch.Bytes)))
			} else {
				_, err := got.Metadata.GetKeySchemaSubject()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
				_, err = got.Metadata.GetKeySchemaVersion()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
			}

			_, isPayloadBeforeStructured := wantPayloadBefore.(opencdc.StructuredData)
			_, isPayloadAfterStructured := wantPayloadAfter.(opencdc.StructuredData)
			if isPayloadBeforeStructured || isPayloadAfterStructured {
				subject, err := got.Metadata.GetPayloadSchemaSubject()
				is.NoErr(err)
				version, err := got.Metadata.GetPayloadSchemaVersion()
				is.NoErr(err)

				is.Equal(subject, tc.wantPayloadSubject)
				is.Equal(version, 1)

				sch, err := schema.Get(ctx, subject, version)
				is.NoErr(err)

				is.Equal("", cmp.Diff(wantSchema, string(sch.Bytes)))
			} else {
				_, err := got.Metadata.GetPayloadSchemaSubject()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
				_, err = got.Metadata.GetPayloadSchemaVersion()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
			}

			is.Equal("", cmp.Diff(wantKey, gotKey))
			is.Equal("", cmp.Diff(wantPayloadBefore, gotPayloadBefore))
			is.Equal("", cmp.Diff(wantPayloadAfter, gotPayloadAfter))
		})
	}
}

// -- SourceWithSchemaContext --------------------------------------------------

// TestSourceWithSchemaContext_Configure tests that the configured schema
// context name is added to relevant Go contexts in the tested methods.
func TestSourceWithSchemaContext_Configure(t *testing.T) {
	connID := "test-connector-id"

	testCases := []struct {
		name            string
		middlewareCfg   SourceWithSchemaContext
		wantContextName string
	}{
		{
			name: "enabled, with custom name",
			middlewareCfg: SourceWithSchemaContext{
				Enabled: lang.Ptr(true),
				Name:    lang.Ptr("foobar"),
			},
			wantContextName: "foobar",
		},
		{
			name: "disabled, with custom name",
			middlewareCfg: SourceWithSchemaContext{
				Enabled: lang.Ptr(false),
				Name:    lang.Ptr("foobar"),
			},
			wantContextName: "",
		},
		{
			name: "enabled, no custom name",
			middlewareCfg: SourceWithSchemaContext{
				Enabled: lang.Ptr(true),
			},
			wantContextName: connID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := internal.ContextWithConnectorID(context.Background(), connID)

			src := NewMockSource(gomock.NewController(t))
			underTest := tc.middlewareCfg.Wrap(src)

			src.EXPECT().
				Open(gomock.Any(), opencdc.Position{}).
				DoAndReturn(func(ctx context.Context, _ opencdc.Position) error {
					is.Equal(tc.wantContextName, schema.GetSchemaContextName(ctx))
					return nil
				})
			src.EXPECT().Read(gomock.Any()).
				DoAndReturn(func(ctx context.Context) (opencdc.Record, error) {
					is.Equal(tc.wantContextName, schema.GetSchemaContextName(ctx))
					return opencdc.Record{}, ErrBackoffRetry
				})
			src.EXPECT().ReadN(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, i int) ([]opencdc.Record, error) {
					is.Equal(tc.wantContextName, schema.GetSchemaContextName(ctx))
					return nil, ErrBackoffRetry
				})

			err := underTest.Open(ctx, opencdc.Position{})
			is.NoErr(err)

			rec, err := underTest.Read(ctx)
			is.True(errors.Is(err, ErrBackoffRetry))
			is.Equal(opencdc.Record{}, rec)

			recs, err := underTest.ReadN(ctx, 123)
			is.True(errors.Is(err, ErrBackoffRetry))
			is.True(recs == nil)
		})
	}
}

// -- SourceWithEncoding ------------------------------------------------------

func TestSourceWithEncoding_Read(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	testDataStruct := opencdc.StructuredData{
		"foo":   "bar",
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}
	wantSchema := `{"name":"record","type":"record","fields":[{"name":"float","type":"double"},{"name":"foo","type":"string"},{"name":"long","type":"long"},{"name":"time","type":{"type":"long","logicalType":"timestamp-micros"}}]}`

	customTestSchema, err := schema.Create(ctx, schema.TypeAvro, "custom-test-schema", []byte(wantSchema))
	is.NoErr(err)

	bytes, err := customTestSchema.Marshal(testDataStruct)
	is.NoErr(err)
	testDataRaw := opencdc.RawData(bytes)

	testCases := []struct {
		name     string
		inputRec opencdc.Record
		wantRec  opencdc.Record
	}{{
		name:     "no key, no payload",
		inputRec: opencdc.Record{},
		wantRec:  opencdc.Record{},
	}, {
		name: "raw key, no payload",
		inputRec: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
		},
		wantRec: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
		},
	}, {
		name: "structured key, no payload",
		inputRec: opencdc.Record{
			Key: testDataStruct.Clone(),
		},
		wantRec: opencdc.Record{
			Key: testDataRaw,
		},
	}, {
		name: "raw payload.before, no key, no payload.after",
		inputRec: opencdc.Record{
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
			},
		},
		wantRec: opencdc.Record{
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
			},
		},
	}, {
		name: "structured payload.before, no key, no payload.after",
		inputRec: opencdc.Record{
			Payload: opencdc.Change{
				Before: testDataStruct.Clone(),
			},
		},
		wantRec: opencdc.Record{
			Payload: opencdc.Change{
				Before: testDataRaw,
			},
		},
	}, {
		name: "raw payload.after, no key, no payload.before",
		inputRec: opencdc.Record{
			Payload: opencdc.Change{
				After: opencdc.RawData("this should not be encoded"),
			},
		},
		wantRec: opencdc.Record{
			Payload: opencdc.Change{
				After: opencdc.RawData("this should not be encoded"),
			},
		},
	}, {
		name: "structured payload after, no key, no payload.before",
		inputRec: opencdc.Record{
			Payload: opencdc.Change{
				After: testDataStruct.Clone(),
			},
		},
		wantRec: opencdc.Record{
			Payload: opencdc.Change{
				After: testDataRaw,
			},
		},
	}, {
		name: "all structured",
		inputRec: opencdc.Record{
			Key: testDataStruct.Clone(),
			Payload: opencdc.Change{
				Before: testDataStruct.Clone(),
				After:  testDataStruct.Clone(),
			},
		},
		wantRec: opencdc.Record{
			Key: testDataRaw,
			Payload: opencdc.Change{
				Before: testDataRaw,
				After:  testDataRaw,
			},
		},
	}, {
		name: "all raw",
		inputRec: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantRec: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
	}, {
		name: "raw key, payload.after structured, no payload.before",
		inputRec: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				After: testDataStruct.Clone(),
			},
		},
		wantRec: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				After: testDataRaw,
			},
		},
	}, {
		name: "structured key, raw payload.before, no payload.after",
		inputRec: opencdc.Record{
			Key: testDataStruct.Clone(),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
			},
		},
		wantRec: opencdc.Record{
			Key: testDataRaw,
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
			},
		},
	}, {
		name: "all structured with collection",
		inputRec: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "foo",
			},
			Key: testDataStruct.Clone(),
			Payload: opencdc.Change{
				Before: testDataStruct.Clone(),
				After:  testDataStruct.Clone(),
			},
		},
		wantRec: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "foo",
			},
			Key: testDataRaw,
			Payload: opencdc.Change{
				Before: testDataRaw,
				After:  testDataRaw,
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			src := NewMockSource(gomock.NewController(t))

			underTest := (&SourceWithEncoding{}).Wrap(src)

			tc.inputRec.Metadata = map[string]string{
				opencdc.MetadataCollection:           "foo",
				opencdc.MetadataKeySchemaSubject:     customTestSchema.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(customTestSchema.Version),
				opencdc.MetadataPayloadSchemaSubject: customTestSchema.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(customTestSchema.Version),
			}

			src.EXPECT().Read(ctx).Return(tc.inputRec, nil)

			got, err := underTest.Read(ctx)
			is.NoErr(err)

			gotKey := got.Key
			gotPayloadBefore := got.Payload.Before
			gotPayloadAfter := got.Payload.After

			is.Equal("", cmp.Diff(tc.wantRec.Key, gotKey))
			is.Equal("", cmp.Diff(tc.wantRec.Payload.Before, gotPayloadBefore))
			is.Equal("", cmp.Diff(tc.wantRec.Payload.After, gotPayloadAfter))
		})
	}
}

func TestSourceWithEncoding_ReadN(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	testDataStruct := opencdc.StructuredData{
		"foo":   "bar",
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}
	wantSchema := `{"name":"record","type":"record","fields":[{"name":"float","type":"double"},{"name":"foo","type":"string"},{"name":"long","type":"long"},{"name":"time","type":{"type":"long","logicalType":"timestamp-micros"}}]}`

	customTestSchema, err := schema.Create(ctx, schema.TypeAvro, "custom-test-schema", []byte(wantSchema))
	is.NoErr(err)

	bytes, err := customTestSchema.Marshal(testDataStruct)
	is.NoErr(err)
	testDataRaw := opencdc.RawData(bytes)

	testCases := []struct {
		name      string
		inputRecs []opencdc.Record
		wantRecs  []opencdc.Record
	}{{
		name: "no records returned",
	}, {
		name: "single record returned",
		inputRecs: []opencdc.Record{{
			Key: testDataStruct.Clone(),
			Payload: opencdc.Change{
				Before: testDataStruct.Clone(),
				After:  testDataStruct.Clone(),
			}},
		},
		wantRecs: []opencdc.Record{{
			Key: testDataRaw,
			Payload: opencdc.Change{
				Before: testDataRaw,
				After:  testDataRaw,
			}},
		},
	}, {
		name: "multiple records returned",
		inputRecs: []opencdc.Record{{
			Key: testDataStruct.Clone(),
			Payload: opencdc.Change{
				Before: testDataStruct.Clone(),
				After:  testDataStruct.Clone(),
			},
		}, {
			Key: testDataStruct.Clone(),
			Payload: opencdc.Change{
				Before: testDataStruct.Clone(),
				After:  testDataStruct.Clone(),
			},
		}, {
			Key: testDataStruct.Clone(),
			Payload: opencdc.Change{
				Before: testDataStruct.Clone(),
				After:  testDataStruct.Clone(),
			},
		}},
		wantRecs: []opencdc.Record{{
			Key: testDataRaw,
			Payload: opencdc.Change{
				Before: testDataRaw,
				After:  testDataRaw,
			},
		}, {
			Key: testDataRaw,
			Payload: opencdc.Change{
				Before: testDataRaw,
				After:  testDataRaw,
			},
		}, {
			Key: testDataRaw,
			Payload: opencdc.Change{
				Before: testDataRaw,
				After:  testDataRaw,
			},
		}},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			src := NewMockSource(gomock.NewController(t))

			underTest := (&SourceWithEncoding{}).Wrap(src)

			for i := range tc.inputRecs {
				tc.inputRecs[i].Metadata = map[string]string{
					opencdc.MetadataCollection:           "foo",
					opencdc.MetadataKeySchemaSubject:     customTestSchema.Subject,
					opencdc.MetadataKeySchemaVersion:     strconv.Itoa(customTestSchema.Version),
					opencdc.MetadataPayloadSchemaSubject: customTestSchema.Subject,
					opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(customTestSchema.Version),
				}
			}

			src.EXPECT().ReadN(ctx, 100).Return(tc.inputRecs, nil)

			got, err := underTest.ReadN(ctx, 100)
			is.NoErr(err)

			is.Equal(len(got), len(tc.wantRecs))

			for i := range got {
				gotKey := got[i].Key
				gotPayloadBefore := got[i].Payload.Before
				gotPayloadAfter := got[i].Payload.After

				is.Equal("", cmp.Diff(tc.wantRecs[i].Key, gotKey))
				is.Equal("", cmp.Diff(tc.wantRecs[i].Payload.Before, gotPayloadBefore))
				is.Equal("", cmp.Diff(tc.wantRecs[i].Payload.After, gotPayloadAfter))
			}
		})
	}
}

// -- SourceWithBatch --------------------------------------------------

func TestSourceWithBatch_ReadN(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	s := NewMockSource(gomock.NewController(t))
	underTest := (&SourceWithBatch{
		BatchSize: lang.Ptr(5),
	}).Wrap(s)

	want := []opencdc.Record{
		{Position: []byte("1")},
		{Position: []byte("2")},
		{Position: []byte("3")},
		{Position: []byte("4")},
		{Position: []byte("5")},
	}

	s.EXPECT().Open(gomock.Any(), opencdc.Position{}).Return(nil)

	// First batch returns 5 records
	call := s.EXPECT().ReadN(gomock.Any(), 5).Return(want, nil)
	// Second batch blocks until the context is done
	var done csync.WaitGroup
	done.Add(1)
	s.EXPECT().ReadN(gomock.Any(), 5).DoAndReturn(func(ctx context.Context, _ int) ([]opencdc.Record, error) {
		defer done.Done()
		<-ctx.Done()
		return nil, ctx.Err()
	}).After(call.Call)

	openCtx, openCancel := context.WithCancel(ctx)
	err := underTest.Open(openCtx, opencdc.Position{})
	is.NoErr(err)

	got, err := underTest.ReadN(ctx, 1) // 1 record per batch is the default, but the middleware should overwrite it
	is.NoErr(err)
	is.Equal(want, got)

	// Give the second batch a chance to start, it's called in the background
	time.Sleep(time.Millisecond * 10)

	// On teardown the SDK cancels the open ctx, which should cause the second
	// batch to return an error
	openCancel()
	is.NoErr(done.WaitTimeout(ctx, time.Second))

	// Calling the middleware again should return the context error
	_, err = underTest.ReadN(ctx, 1)
	is.True(errors.Is(err, context.Canceled))
}
