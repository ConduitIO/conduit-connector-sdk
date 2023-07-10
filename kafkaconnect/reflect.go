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
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Reflect uses reflection to extract a kafka connect compatible schema from v.
func Reflect(v any) *Schema {
	return reflectInternal(reflect.ValueOf(v), reflect.TypeOf(v))
}

// SortFields can be used in tests to make sure the order of fields in a map is
// deterministic.
func SortFields(s *Schema) {
	if s.Type != TypeStruct {
		return
	}
	sort.Slice(s.Fields, func(i, j int) bool {
		return s.Fields[i].Field < s.Fields[j].Field
	})
	for i := range s.Fields {
		SortFields(&s.Fields[i])
	}
}

//nolint:gocyclo // reflection requires a huge switch, it's fine
func reflectInternal(v reflect.Value, t reflect.Type) *Schema {
	if t == nil {
		return nil // untyped nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return &Schema{Type: TypeBoolean}
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Uint32: // uints might overflow int64, but we take the risk
		return &Schema{Type: TypeInt64}
	case reflect.Int32, reflect.Uint16:
		return &Schema{Type: TypeInt32}
	case reflect.Int16, reflect.Uint8:
		return &Schema{Type: TypeInt16}
	case reflect.Int8:
		return &Schema{Type: TypeInt8}
	case reflect.Float32:
		return &Schema{Type: TypeFloat}
	case reflect.Float64:
		return &Schema{Type: TypeDouble}
	case reflect.String:
		return &Schema{Type: TypeString}
	case reflect.Pointer:
		s := reflectInternal(v.Elem(), t.Elem())
		if s != nil {
			s.Optional = true // pointers can be nil
		}
		return s
	case reflect.Interface:
		if !v.IsValid() || v.IsNil() {
			return nil // can't get a schema for this
		}
		return reflectInternal(v.Elem(), v.Elem().Type())
	case reflect.Array, reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			// byte array/slice
			return &Schema{
				Type:     TypeBytes,
				Optional: true,
			}
		}

		var elemValue reflect.Value
		if v.Len() > 0 {
			elemValue = v.Index(0)
		}
		items := reflectInternal(elemValue, t.Elem())
		if items == nil {
			// we fall back to a simple string schema, it doesn't matter what it
			// is since elements are apparently nil or of any type
			items = &Schema{
				Type:     TypeString, // by default items are strings if we don't know them
				Optional: true,       // in case there's a nil
			}
		}
		return &Schema{
			Type:     TypeArray,
			Items:    items,
			Optional: true, // slices can be nil
		}
	case reflect.Map:
		if t.Key().Kind() == reflect.String {
			// special case - we treat the map like a struct
			s := &Schema{
				Type:     TypeStruct,
				Optional: true, // maps can be nil
			}
			valType := t.Elem()
			for _, keyValue := range v.MapKeys() {
				fs := reflectInternal(v.MapIndex(keyValue), valType)
				if fs == nil {
					// we fall back to a simple string schema, it doesn't matter
					// what it is since the value is nil
					fs = &Schema{
						Type:     TypeString,
						Optional: true,
					}
				}
				fs.Field = keyValue.String()
				s.Fields = append(s.Fields, *fs)
			}
			return s
		}

		var keyValue reflect.Value
		var valValue reflect.Value
		for _, kv := range v.MapKeys() {
			keyValue = kv
			valValue = v.MapIndex(kv)
			break
		}
		return &Schema{
			Type:   TypeMap,
			Keys:   reflectInternal(keyValue, t.Key()),
			Values: reflectInternal(valValue, t.Elem()),
		}
	case reflect.Struct:
		s := &Schema{
			Type: TypeStruct,
		}
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			name, ok := getStructFieldJSONName(sf)
			if !ok {
				continue // skip this field
			}
			fs := reflectInternal(v.Field(i), t.Field(i).Type)
			fs.Field = name
			s.Fields = append(s.Fields, *fs)
		}
		return s
	}
	// Invalid, Uintptr, UnsafePointer, Complex64, Complex128, Chan, Func
	panic(fmt.Errorf("unsupported type: %v", t))
}

func getStructFieldJSONName(sf reflect.StructField) (string, bool) {
	jsonTag := strings.Split(sf.Tag.Get("json"), ",")[0] // ignore tag options (omitempty)
	if jsonTag == "-" {
		return "", false
	}
	if jsonTag != "" {
		return jsonTag, true
	}
	return sf.Name, true
}
