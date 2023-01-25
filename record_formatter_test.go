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
	"github.com/conduitio/conduit-connector-sdk/kafkaconnect"
	"github.com/matryer/is"
	"testing"
)

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

func TestDebeziumConverter(t *testing.T) {
	is := is.New(t)
	var converter DebeziumConverter
	converter.SchemaName = "custom.name"

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
	want := kafkaconnect.Envelope{
		Schema: kafkaconnect.Schema{
			Type: kafkaconnect.TypeStruct,
			Name: "custom.name",
			Fields: []kafkaconnect.Schema{{
				Field:    "before",
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
			Before:          nil,
			After:           r.Payload.After.(StructuredData),
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
