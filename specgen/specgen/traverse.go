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

package specgen

import (
	"reflect"
	"strings"
	"unicode"
)

// fieldHook is a function type that gets called for each field during traversal.
// path is the JSON path to the field (using dot notation).
// field is the reflect.StructField information.
// value is the actual value of the field.
type fieldHook func(path string, field reflect.StructField, value reflect.Value)

// TraverseFields traverses all fields in a struct, including nested structs,
// calling the provided hook function for each field encountered.
func traverseFields(v any, hook fieldHook) {
	traverseFieldsInternal(reflect.ValueOf(v), "", hook)
}

func traverseFieldsInternal(v reflect.Value, parentPath string, hook fieldHook) {
	// Get the underlying value if it's a pointer
	v = reflect.Indirect(v)

	// Handle different kinds of values
	//nolint:exhaustive // we only need to traverse fields structs and map key-value pairs
	switch v.Kind() {
	case reflect.Map:
		traverseMapFields(v, parentPath, hook)
	case reflect.Struct:
		traverseStructFields(v, parentPath, hook)
	}
}

func traverseStructFields(v reflect.Value, parentPath string, hook fieldHook) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)
		if !field.IsExported() {
			// skip non-exported fields
			continue
		}

		// Get JSON tag if it exists
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue // Skip this field if json tag is "-"
		}

		// Parse the JSON tag to get the name
		tagParts := strings.Split(jsonTag, ",")

		// Determine if this is an embedded field without an explicit JSON tag
		isEmbeddedWithoutTag := field.Anonymous && jsonTag == ""

		// Handle field naming
		var fieldName string
		if tagParts[0] != "" {
			// Explicit JSON tag takes precedence
			fieldName = tagParts[0]
		} else if !isEmbeddedWithoutTag {
			// If not embedded or if embedded but has explicit name in struct
			fieldName = field.Name
		}
		fieldName = formatFieldName(fieldName)

		// Build the full path
		var fullPath string
		switch {
		case parentPath == "":
			fullPath = fieldName
		case fieldName == "":
			fullPath = parentPath
		default:
			fullPath = parentPath + "." + fieldName
		}

		// Get the final type after dereferencing pointers
		finalType := field.Type
		for finalType.Kind() == reflect.Ptr {
			finalType = finalType.Elem()
		}

		// Handle maps specially
		if finalType.Kind() == reflect.Map {
			traverseMapFields(fieldValue, fullPath, hook)
			continue
		}

		// For non-embedded structs or embedded structs with explicit naming,
		// call the hook with the current field if it's not a struct or if it's a map
		// with non-struct values
		if !isEmbeddedWithoutTag || finalType.Kind() != reflect.Struct {
			hook(fullPath, field, fieldValue)
		}

		// If it's a pointer, recurse deeper
		for fieldValue.Kind() == reflect.Ptr {
			fieldValue = fieldValue.Elem()
		}

		if fieldValue.Kind() == reflect.Struct {
			traverseFieldsInternal(fieldValue, fullPath, hook)
		}
	}
}

func traverseMapFields(v reflect.Value, parentPath string, hook fieldHook) {
	// For maps, we're interested in the type of the values
	valueType := v.Type().Elem()

	// If the value type is a pointer, get the element type
	if valueType.Kind() == reflect.Ptr {
		valueType = valueType.Elem()
	}

	// Only proceed if the map values are structs
	if valueType.Kind() == reflect.Struct {
		// Create a new value of the map's value type to traverse its fields
		// We use a zero value since we're only interested in the structure
		dummy := reflect.New(valueType).Elem()

		// Build the path with wildcard
		mapPath := parentPath + ".*"

		// Traverse the fields of the map value type
		traverseFieldsInternal(dummy, mapPath, hook)
	}
}

func formatFieldName(name string) string {
	if name == "" {
		return ""
	}
	nameRunes := []rune(name)
	foundLowercase := false
	i := 0
	newName := strings.Map(func(r rune) rune {
		if foundLowercase {
			return r
		}
		if unicode.IsLower(r) {
			// short circuit
			foundLowercase = true
			return r
		}
		if i == 0 ||
			(len(nameRunes) > i+1 && unicode.IsUpper(nameRunes[i+1])) {
			r = unicode.ToLower(r)
		}
		i++
		return r
	}, name)
	return newName
}
