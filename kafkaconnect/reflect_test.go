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

package kafkaconnect

import (
	"github.com/matryer/is"
	"sort"
	"testing"
)

func TestReflect_Types(t *testing.T) {
	is := is.New(t)

	type customBool bool
	have := struct {
		MyBool       bool
		MyBoolPtr    *bool
		MyCustomBool customBool

		MyInt   int
		MyInt64 int64
		MyInt32 int32
		MyInt16 int16
		MyInt8  int8

		MyUint   uint
		MyUint64 uint64
		MyUint32 uint32
		MyUint16 uint16
		MyUint8  uint8

		MyFloat32 float32
		MyFloat64 float64

		MyString string

		MyMap   map[int]float32
		MyArray [4]int32
		MySlice []bool
	}{}

	want := &Schema{
		Type: TypeStruct,
		Fields: []Schema{
			{Field: "MyBool", Type: TypeBoolean},
			{Field: "MyBoolPtr", Type: TypeBoolean, Optional: true},
			{Field: "MyCustomBool", Type: TypeBoolean},
			{Field: "MyInt", Type: TypeInt64},
			{Field: "MyInt64", Type: TypeInt64},
			{Field: "MyInt32", Type: TypeInt32},
			{Field: "MyInt16", Type: TypeInt16},
			{Field: "MyInt8", Type: TypeInt8},
			{Field: "MyUint", Type: TypeInt64},
			{Field: "MyUint64", Type: TypeInt64},
			{Field: "MyUint32", Type: TypeInt64},
			{Field: "MyUint16", Type: TypeInt32},
			{Field: "MyUint8", Type: TypeInt16},
			{Field: "MyFloat32", Type: TypeFloat},
			{Field: "MyFloat64", Type: TypeDouble},
			{Field: "MyString", Type: TypeString},
			{
				Field:  "MyMap",
				Type:   TypeMap,
				Keys:   &Schema{Type: TypeInt64},
				Values: &Schema{Type: TypeFloat},
			}, {
				Field:    "MyArray",
				Type:     TypeArray,
				Items:    &Schema{Type: TypeInt32},
				Optional: true,
			}, {
				Field:    "MySlice",
				Type:     TypeArray,
				Items:    &Schema{Type: TypeBoolean},
				Optional: true,
			},
		},
	}

	got := Reflect(have)
	is.Equal(want, got)
}

func TestReflect_MapAsStruct_Nil(t *testing.T) {
	is := is.New(t)

	have := struct {
		StructuredData map[string]any
	}{}

	want := &Schema{
		Type: TypeStruct,
		Fields: []Schema{
			{Type: TypeStruct, Field: "StructuredData", Optional: true},
		},
	}

	got := Reflect(have)
	is.Equal(want, got)
}

func TestReflect_MapAsStruct(t *testing.T) {
	is := is.New(t)

	type StructuredData map[string]any
	type customBool bool
	have := StructuredData{
		"MyBool":       false,
		"MyBoolPtr":    func() *bool { a := false; return &a }(),
		"MyCustomBool": customBool(true),

		"MyInt":   int(1),
		"MyInt64": int64(1),
		"MyInt32": int32(1),
		"MyInt16": int16(1),
		"MyInt8":  int8(1),

		"MyUint":   uint(1),
		"MyUint64": uint64(1),
		"MyUint32": uint32(1),
		"MyUint16": uint16(1),
		"MyUint8":  uint8(1),

		"MyFloat32": float32(1.2),
		"MyFloat64": float64(1.2),

		"MyString": "foo",
		"MyNil":    nil,

		"MyMap":   map[int]float32{},
		"MyArray": [4]int32{},
		"MySlice": []bool{},
	}

	want := &Schema{
		Type:     TypeStruct,
		Optional: true,
		Fields: []Schema{
			{Field: "MyBool", Type: TypeBoolean},
			{Field: "MyBoolPtr", Type: TypeBoolean, Optional: true},
			{Field: "MyCustomBool", Type: TypeBoolean},
			{Field: "MyInt", Type: TypeInt64},
			{Field: "MyInt64", Type: TypeInt64},
			{Field: "MyInt32", Type: TypeInt32},
			{Field: "MyInt16", Type: TypeInt16},
			{Field: "MyInt8", Type: TypeInt8},
			{Field: "MyUint", Type: TypeInt64},
			{Field: "MyUint64", Type: TypeInt64},
			{Field: "MyUint32", Type: TypeInt64},
			{Field: "MyUint16", Type: TypeInt32},
			{Field: "MyUint8", Type: TypeInt16},
			{Field: "MyFloat32", Type: TypeFloat},
			{Field: "MyFloat64", Type: TypeDouble},
			{Field: "MyString", Type: TypeString},
			{Field: "MyNil", Type: TypeString, Optional: true},
			{
				Field:  "MyMap",
				Type:   TypeMap,
				Keys:   &Schema{Type: TypeInt64},
				Values: &Schema{Type: TypeFloat},
			}, {
				Field:    "MyArray",
				Type:     TypeArray,
				Items:    &Schema{Type: TypeInt32},
				Optional: true,
			}, {
				Field:    "MySlice",
				Type:     TypeArray,
				Items:    &Schema{Type: TypeBoolean},
				Optional: true,
			},
		},
	}

	got := Reflect(have)

	sortFields(want)
	sortFields(got)
	is.Equal(want, got)
}

func TestReflect_NestedMap(t *testing.T) {
	is := is.New(t)

	have := map[string]any{
		"foo": "bar",
		"level1": map[string]any{
			"foo": "bar",
			"level2": map[string]any{
				"foo": "bar",
				"level3": map[string]any{
					"foo":        "bar",
					"regularMap": map[int]bool{},
				},
			},
		},
	}

	want := &Schema{
		Type:     TypeStruct,
		Optional: true,
		Fields: []Schema{
			{Field: "foo", Type: TypeString},
			{
				Field:    "level1",
				Type:     TypeStruct,
				Optional: true,
				Fields: []Schema{
					{Field: "foo", Type: TypeString},
					{
						Field:    "level2",
						Type:     TypeStruct,
						Optional: true,
						Fields: []Schema{
							{Field: "foo", Type: TypeString},
							{
								Field:    "level3",
								Type:     TypeStruct,
								Optional: true,
								Fields: []Schema{
									{Field: "foo", Type: TypeString},
									{
										Field:  "regularMap",
										Type:   TypeMap,
										Keys:   &Schema{Type: TypeInt64},
										Values: &Schema{Type: TypeBoolean},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	got := Reflect(have)

	sortFields(want)
	sortFields(got)
	is.Equal(want, got)
}

func sortFields(s *Schema) {
	if s.Type != TypeStruct {
		return
	}
	sort.Slice(s.Fields, func(i, j int) bool {
		return s.Fields[i].Field < s.Fields[j].Field
	})
	for i := range s.Fields {
		sortFields(&s.Fields[i])
	}
}
