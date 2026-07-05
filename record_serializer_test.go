// Copyright © 2023 Meroxa, Inc.
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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
	"github.com/matryer/is"
)

var (
	encBytesSink []byte
	encErrSink   error
)

func BenchmarkJSONEncoder(b *testing.B) {
	rec := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{opencdc.MetadataConduitSourcePluginName: "example"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: nil,
			After: opencdc.StructuredData{
				"foo": "bar",
				"baz": "qux",
			},
		},
	}

	enc := JSONEncoder{}
	for i := 0; i < b.N; i++ {
		encBytesSink, encErrSink = enc.Encode(rec)
	}
}

// benchmarkRawRecord returns a record with raw key and payload, resembling a
// record produced by a connector that doesn't attach structured data (e.g. a
// file or generic byte-stream source). The payload is valid JSON so that
// converters attempting to parse raw data as JSON (e.g. DebeziumConverter)
// exercise their real parsing path instead of falling back to an error.
func benchmarkRawRecord() opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position("benchmark-position-00000001"),
		Operation: opencdc.OperationCreate,
		Metadata: opencdc.Metadata{
			opencdc.MetadataConduitSourcePluginName: "benchmark-source",
			opencdc.MetadataCollection:              "users",
		},
		Key: opencdc.RawData(`{"id":"a1b2c3d4"}`),
		Payload: opencdc.Change{
			Before: nil,
			After:  opencdc.RawData(`{"id":"a1b2c3d4","name":"Jane Doe","email":"jane@example.com","active":true,"created_at":"2026-01-01T00:00:00Z","balance":1234.56}`),
		},
	}
}

// benchmarkStructuredRecord returns a record with structured key and payload,
// resembling a record produced by a connector with native structured data
// support (e.g. a database CDC source).
func benchmarkStructuredRecord() opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position("benchmark-position-00000001"),
		Operation: opencdc.OperationUpdate,
		Metadata: opencdc.Metadata{
			opencdc.MetadataConduitSourcePluginName: "benchmark-source",
			opencdc.MetadataCollection:              "users",
		},
		Key: opencdc.StructuredData{"id": "a1b2c3d4"},
		Payload: opencdc.Change{
			Before: opencdc.StructuredData{
				"id":         "a1b2c3d4",
				"name":       "Jane Doe",
				"email":      "jane@old-example.com",
				"active":     true,
				"created_at": "2026-01-01T00:00:00Z",
				"balance":    1200.00,
			},
			After: opencdc.StructuredData{
				"id":         "a1b2c3d4",
				"name":       "Jane Doe",
				"email":      "jane@example.com",
				"active":     true,
				"created_at": "2026-01-01T00:00:00Z",
				"balance":    1234.56,
			},
		},
	}
}

// BenchmarkGenericRecordSerializer benchmarks the full destination
// serialization hot path (Converter.Convert + Encoder.Encode combined through
// GenericRecordSerializer.Serialize), for every combination of converter and
// record shape shipped as a default record serializer.
func BenchmarkGenericRecordSerializer(b *testing.B) {
	records := map[string]opencdc.Record{
		"raw":        benchmarkRawRecord(),
		"structured": benchmarkStructuredRecord(),
	}
	serializers := map[string]RecordSerializer{
		"opencdc/json":  GenericRecordSerializer{Converter: OpenCDCConverter{}, Encoder: JSONEncoder{}},
		"debezium/json": GenericRecordSerializer{Converter: DebeziumConverter{}, Encoder: JSONEncoder{}},
	}

	for serName, ser := range serializers {
		for recName, rec := range records {
			b.Run(serName+"/"+recName, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					encBytesSink, encErrSink = ser.Serialize(rec)
				}
				if encErrSink != nil {
					b.Fatal(encErrSink)
				}
			})
		}
	}
}

// BenchmarkTemplateRecordSerializer benchmarks TemplateRecordSerializer,
// which destinations can select via sdk.record.format=template to fully
// customize the output. The "toJson" template mirrors the opencdc/json output
// and is the most common non-trivial template in use.
func BenchmarkTemplateRecordSerializer(b *testing.B) {
	var serializer RecordSerializer = TemplateRecordSerializer{}
	serializer, err := serializer.Configure(`{{ toJson . }}`)
	if err != nil {
		b.Fatal(err)
	}

	records := map[string]opencdc.Record{
		"raw":        benchmarkRawRecord(),
		"structured": benchmarkStructuredRecord(),
	}

	for recName, rec := range records {
		b.Run(recName, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encBytesSink, encErrSink = serializer.Serialize(rec)
			}
			if encErrSink != nil {
				b.Fatal(encErrSink)
			}
		})
	}
}

func TestOpenCDCConverter(t *testing.T) {
	is := is.New(t)
	var converter OpenCDCConverter

	want := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{opencdc.MetadataConduitSourcePluginName: "example"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: nil,
			After: opencdc.StructuredData{
				"foo": "bar",
				"baz": "qux",
			},
		},
	}

	got, err := converter.Convert(want)
	is.NoErr(err)
	is.Equal(got, want)
}

func TestDebeziumConverter_Structured(t *testing.T) {
	is := is.New(t)
	converter, err := DebeziumConverter{}.Configure(map[string]string{
		"debezium.schema.name": "custom.name",
	})
	is.NoErr(err)

	r := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationUpdate,
		Metadata:  opencdc.Metadata{opencdc.MetadataConduitSourcePluginName: "example"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: opencdc.StructuredData{
				"bar": 123,
			},
			After: opencdc.StructuredData{
				"foo": "bar",
				"baz": "qux",
			},
		},
	}
	want := kafkaconnect.Envelope{
		Schema: kafkaconnect.Schema{
			Type: kafkaconnect.TypeStruct,
			Name: "custom.name",
			Fields: []kafkaconnect.Schema{{
				Field:    "before",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Field: "bar",
					Type:  kafkaconnect.TypeInt64,
				}},
			}, {
				Field:    "after",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Field: "foo",
					Type:  kafkaconnect.TypeString,
				}, {
					Field: "baz",
					Type:  kafkaconnect.TypeString,
				}},
			}, {
				Field:    "source",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Field: "conduit.source.plugin.name",
					Type:  kafkaconnect.TypeString,
				}},
			}, {
				Field: "op",
				Type:  kafkaconnect.TypeString,
			}, {
				Field:    "ts_ms",
				Type:     kafkaconnect.TypeInt64,
				Optional: true,
			}, {
				Field:    "transaction",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Field: "id",
					Type:  kafkaconnect.TypeString,
				}, {
					Field: "total_order",
					Type:  kafkaconnect.TypeInt64,
				}, {
					Field: "data_collection_order",
					Type:  kafkaconnect.TypeInt64,
				}},
			}},
		},
		Payload: kafkaconnect.DebeziumPayload{
			Before:          r.Payload.Before.(opencdc.StructuredData),
			After:           r.Payload.After.(opencdc.StructuredData),
			Source:          r.Metadata,
			Op:              kafkaconnect.DebeziumOpUpdate,
			TimestampMillis: 0,
			Transaction:     nil,
		},
	}

	got, err := converter.Convert(r)
	is.NoErr(err)

	gotEnvelope, ok := got.(kafkaconnect.Envelope)
	is.True(ok)
	// fields in maps don't have a deterministic order, let's sort all fields
	kafkaconnect.SortFields(&want.Schema)
	kafkaconnect.SortFields(&gotEnvelope.Schema)

	is.Equal(got, want)
}

func TestDebeziumConverter_RawData(t *testing.T) {
	is := is.New(t)
	converter, err := DebeziumConverter{}.Configure(map[string]string{})
	is.NoErr(err)

	r := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{opencdc.MetadataConduitSourcePluginName: "example"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: opencdc.RawData("foo"),
			After:  nil,
		},
	}
	want := kafkaconnect.Envelope{
		Schema: kafkaconnect.Schema{
			Type: kafkaconnect.TypeStruct,
			Fields: []kafkaconnect.Schema{{
				Field:    "before",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Optional: true,
					Field:    "opencdc.rawData",
					Type:     kafkaconnect.TypeBytes,
				}},
			}, {
				Field:    "after",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Optional: true,
					Field:    "opencdc.rawData",
					Type:     kafkaconnect.TypeBytes,
				}},
			}, {
				Field:    "source",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Field: "conduit.source.plugin.name",
					Type:  kafkaconnect.TypeString,
				}},
			}, {
				Field: "op",
				Type:  kafkaconnect.TypeString,
			}, {
				Field:    "ts_ms",
				Type:     kafkaconnect.TypeInt64,
				Optional: true,
			}, {
				Field:    "transaction",
				Type:     kafkaconnect.TypeStruct,
				Optional: true,
				Fields: []kafkaconnect.Schema{{
					Field: "id",
					Type:  kafkaconnect.TypeString,
				}, {
					Field: "total_order",
					Type:  kafkaconnect.TypeInt64,
				}, {
					Field: "data_collection_order",
					Type:  kafkaconnect.TypeInt64,
				}},
			}},
		},
		Payload: kafkaconnect.DebeziumPayload{
			Before:          opencdc.StructuredData{"opencdc.rawData": []byte("foo")},
			After:           nil,
			Source:          r.Metadata,
			Op:              kafkaconnect.DebeziumOpCreate,
			TimestampMillis: 0,
			Transaction:     nil,
		},
	}

	got, err := converter.Convert(r)
	is.NoErr(err)

	gotEnvelope, ok := got.(kafkaconnect.Envelope)
	is.True(ok)
	// fields in maps don't have a deterministic order, let's sort all fields
	kafkaconnect.SortFields(&want.Schema)
	kafkaconnect.SortFields(&gotEnvelope.Schema)

	is.Equal(got, want)
}

func TestJSONEncoder(t *testing.T) {
	is := is.New(t)
	var encoder JSONEncoder

	r := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{opencdc.MetadataConduitSourcePluginName: "example"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: nil,
			After: opencdc.StructuredData{
				"foo": "bar",
				"baz": "qux",
			},
		},
	}
	want := `{"position":"Zm9v","operation":"create","metadata":{"conduit.source.plugin.name":"example"},"key":"YmFy","payload":{"before":null,"after":{"baz":"qux","foo":"bar"}}}`

	got, err := encoder.Encode(r)
	is.NoErr(err)
	is.Equal(string(got), want)
}

func TestTemplateRecordSerializer(t *testing.T) {
	r := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{opencdc.MetadataConduitSourcePluginName: "example"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: nil,
			After: opencdc.StructuredData{
				"foo": "bar",
				"baz": "qux",
			},
		},
	}

	testCases := map[string]struct {
		have     opencdc.Record
		template string
		want     string
	}{
		"go record": {
			// output prints the Go record (not very useful, this test case is here to explain the behavior)
			have:     r,
			template: `{{ . }}`,
			want:     `{foo create map[conduit.source.plugin.name:example] [98 97 114] {<nil> map[baz:qux foo:bar]} <nil>}`,
		},
		"json record": {
			// output should be the same as in format opencdc/json
			have:     r,
			template: `{{ toJson . }}`,
			want:     `{"position":"Zm9v","operation":"create","metadata":{"conduit.source.plugin.name":"example"},"key":"YmFy","payload":{"before":null,"after":{"baz":"qux","foo":"bar"}}}`,
		},
		"json structured payload": {
			have:     r,
			template: `{{ if typeIs "opencdc.RawData" .Payload.After }}{{ printf "%s" .Payload.After }}{{ else }}{{ toJson .Payload.After }}{{ end }}`,
			want:     `{"baz":"qux","foo":"bar"}`,
		},
		"json raw payload": {
			have: opencdc.Record{
				Payload: opencdc.Change{
					After: opencdc.RawData("my raw data"),
				},
			},
			template: `{{ if typeIs "opencdc.RawData" .Payload.After }}{{ printf "%s" .Payload.After }}{{ else }}{{ toJson .Payload.After }}{{ end }}`,
			want:     `my raw data`,
		},
		"json nil payload": {
			have: opencdc.Record{
				Payload: opencdc.Change{
					After: nil,
				},
			},
			template: `{{ if typeIs "opencdc.RawData" .Payload.After }}{{ printf "%s" .Payload.After }}{{ else }}{{ toJson .Payload.After }}{{ end }}`,
			want:     `null`,
		},
		"map metadata": {
			have:     r,
			template: `{{ .Metadata }}`,
			want:     `map[conduit.source.plugin.name:example]`,
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			is := is.New(t)
			var serializer RecordSerializer = TemplateRecordSerializer{}

			serializer, err := serializer.Configure(tc.template)
			is.NoErr(err)

			got, err := serializer.Serialize(tc.have)
			is.NoErr(err)
			is.Equal(string(got), tc.want)
		})
	}
}

func TestTemplateSerializer_MissingKey(t *testing.T) {
	is := is.New(t)
	tests := []struct {
		name        string
		template    string
		record      opencdc.Record
		expectError bool
	}{
		{
			name:     "valid template with existing key",
			template: "{{.Key}}",
			record: opencdc.Record{
				Key: opencdc.RawData("test-key"),
			},
			expectError: false,
		},
		{
			name:     "template with missing key",
			template: "{{.NonExistentKey}}",
			record: opencdc.Record{
				Key: opencdc.RawData("test-key"),
			},
			expectError: true,
		},
		{
			name:     "nested template with missing key",
			template: "{{.Metadata.nonexistent}}",
			record: opencdc.Record{
				Metadata: opencdc.Metadata{
					"existing": "value",
				},
			},
			expectError: true,
		},
		{
			name:     "multiple expressions with missing key",
			template: "{{.Key}} {{.NonExistentKey}}",
			record: opencdc.Record{
				Key: opencdc.RawData("test-key"),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			serializer := TemplateRecordSerializer{}
			configured, err := serializer.Configure(tt.template)
			is.NoErr(err)

			_, err = configured.Serialize(tt.record)
			if tt.expectError {
				is.True(err != nil)
			} else {
				is.NoErr(err)
			}
		})
	}
}
