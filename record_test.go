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
	"testing"

	"github.com/matryer/is"
)

func TestRecord_Bytes(t *testing.T) {
	is := is.New(t)

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

	got := string(r.Bytes())
	is.Equal(got, want)
}

func TestConverter_OpenCDC(t *testing.T) {
	is := is.New(t)

	converter, err := NewOpenCDCConverter(nil)
	is.NoErr(err)

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

func TestEncoder_JSON(t *testing.T) {
	is := is.New(t)

	encoder, err := NewJSONEncoder(nil)
	is.NoErr(err)

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
