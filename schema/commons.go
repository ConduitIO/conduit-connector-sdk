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

package schema

import (
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
)

// This file contains type aliases for the conduit-commons/schema package. This
// makes it easier to use the schema package in other packages without having to
// import conduit-commons/schema directly.

// Type aliases for schema package.
type (
	// Type represents the type of a schema (avro, protobuf, etc).
	Type = schema.Type
	// Schema represents a schema object.
	Schema = schema.Schema
	// Serde represents a serializer/deserializer.
	Serde = schema.Serde
)

const (
	TypeAvro = schema.TypeAvro
)

var KnownSerdeFactories = schema.KnownSerdeFactories

func AttachKeySchemaToRecord(r opencdc.Record, s Schema) {
	schema.AttachKeySchemaToRecord(r, s)
}

func AttachPayloadSchemaToRecord(r opencdc.Record, s Schema) {
	schema.AttachPayloadSchemaToRecord(r, s)
}
