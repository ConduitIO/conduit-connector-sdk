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

const (
	TypeBoolean Type = "boolean"
	TypeInt8    Type = "int8"
	TypeInt16   Type = "int16"
	TypeInt32   Type = "int32"
	TypeInt64   Type = "int64"
	TypeFloat   Type = "float"
	TypeDouble  Type = "double"
	TypeBytes   Type = "bytes"
	TypeString  Type = "string"
	TypeArray   Type = "array"
	TypeMap     Type = "map"
	TypeStruct  Type = "struct"
)

type Type string

// Envelope represents a kafka connect message that includes a schema.
type Envelope struct {
	Schema  Schema `json:"schema"`
	Payload any    `json:"payload"`
}

// Schema represents a kafka connect JSON schema, the one that can be included
// in a JSON message directly.
type Schema struct {
	Type     Type   `json:"type"`
	Optional bool   `json:"optional,omitempty"`
	Name     string `json:"name,omitempty"`
	Version  int    `json:"version,omitempty"`
	Doc      string `json:"doc,omitempty"`
	Default  string `json:"default,omitempty"`

	Parameters map[string]string `json:"parameters,omitempty"`

	// Type: Array
	Items *Schema `json:"items,omitempty"`
	// Type: Map
	Keys   *Schema `json:"keys,omitempty"`
	Values *Schema `json:"values,omitempty"`
	// Type: Struct
	Fields []Schema `json:"fields,omitempty"`
	// Struct fields
	Field string `json:"field,omitempty"`
}
