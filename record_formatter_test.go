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

	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
	"github.com/matryer/is"
)

var (
	encBytesSink []byte
	encErrSink   error
)

func BenchmarkJSONEncoder(b *testing.B) {
	rec := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  Metadata{MetadataConduitSourcePluginName: "example"},
		Key:       RawData("bar"),
		Payload: Change{
			Before: nil,
			After: StructuredData{
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

func TestOpenCDCConverter(t *testing.T) {
	is := is.New(t)
	var converter OpenCDCConverter

	want := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  Metadata{MetadataConduitSourcePluginName: "example"},
		Key:       RawData("bar"),
		Payload: Change{
			Before: nil,
			After: StructuredData{
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

	r := Record{
		Position:  Position("foo"),
		Operation: OperationUpdate,
		Metadata:  Metadata{MetadataConduitSourcePluginName: "example"},
		Key:       RawData("bar"),
		Payload: Change{
			Before: StructuredData{
				"bar": 123,
			},
			After: StructuredData{
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
			Before:          r.Payload.Before.(StructuredData),
			After:           r.Payload.After.(StructuredData),
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

	r := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  Metadata{MetadataConduitSourcePluginName: "example"},
		Key:       RawData("bar"),
		Payload: Change{
			Before: RawData("foo"),
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
			Before:          StructuredData{"opencdc.rawData": []byte("foo")},
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

	r := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  Metadata{MetadataConduitSourcePluginName: "example"},
		Key:       RawData("bar"),
		Payload: Change{
			Before: nil,
			After: StructuredData{
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

func TestTemplateRecordFormatter(t *testing.T) {
	r := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  Metadata{MetadataConduitSourcePluginName: "example"},
		Key:       RawData("bar"),
		Payload: Change{
			Before: nil,
			After: StructuredData{
				"foo": "bar",
				"baz": "qux",
			},
		},
	}

	testCases := map[string]struct {
		have     Record
		template string
		want     string
		wantErr  bool
	}{
		"go record": {
			// output prints the Go record (not very useful, this test case is here to explain the behavior)
			have:     r,
			template: `{{ . }}`,
			want:     `{[102 111 111] create map[conduit.source.plugin.name:example] [98 97 114] {<nil> map[baz:qux foo:bar]} <nil>}`,
		},
		"json record": {
			// output should be the same as in format opencdc/json
			have:     r,
			template: `{{ toJson . }}`,
			want:     `{"position":"Zm9v","operation":"create","metadata":{"conduit.source.plugin.name":"example"},"key":"YmFy","payload":{"before":null,"after":{"baz":"qux","foo":"bar"}}}`,
		},
		"json structured payload": {
			have:     r,
			template: `{{ if typeIs "sdk.RawData" .Payload.After }}{{ printf "%s" .Payload.After }}{{ else }}{{ toJson .Payload.After }}{{ end }}`,
			want:     `{"baz":"qux","foo":"bar"}`,
		},
		"json raw payload": {
			have: Record{
				Payload: Change{
					After: RawData("my raw data"),
				},
			},
			template: `{{ if typeIs "sdk.RawData" .Payload.After }}{{ printf "%s" .Payload.After }}{{ else }}{{ toJson .Payload.After }}{{ end }}`,
			want:     `my raw data`,
		},
		"json nil payload": {
			have: Record{
				Payload: Change{
					After: nil,
				},
			},
			template: `{{ if typeIs "sdk.RawData" .Payload.After }}{{ printf "%s" .Payload.After }}{{ else }}{{ toJson .Payload.After }}{{ end }}`,
			want:     `null`,
		},
		"map metadata": {
			have:     r,
			template: `{{ .Metadata }}`,
			want:     `map[conduit.source.plugin.name:example]`,
		},
		// New test cases for missing key behavior
		"missing key in template": {
			have:     r,
			template: `{{ .Payload.NonExistent }}`,
			wantErr:  true,
		},
		"missing nested key": {
			have:     r,
			template: `{{ .Payload.After.NonExistent }}`,
			wantErr:  true,
		},
		"missing key with valid parts": {
			have:     r,
			template: `{{ .Payload.After.foo }} {{ .Payload.After.NonExistent }}`,
			wantErr:  true,
		},
		"typo in function name": {
			have:     r,
			template: `{{ printif "%s" .Payload.After }}`,
			wantErr:  true,
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			is := is.New(t)
			var formatter RecordFormatter = TemplateRecordFormatter{}

			formatter, err := formatter.Configure(tc.template)
			is.NoErr(err)

			got, err := formatter.Format(tc.have)
			if tc.wantErr {
				is.True(err != nil)
				return
			}
			is.NoErr(err)
			is.Equal(string(got), tc.want)
		})
	}
}
