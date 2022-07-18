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
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
)

const (
	OperationCreate Operation = iota + 1
	OperationUpdate
	OperationDelete
	OperationSnapshot
)

// Operation defines what triggered the creation of a record.
type Operation int

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(OperationCreate)-int(cpluginv1.OperationCreate)]
	_ = cTypes[int(OperationUpdate)-int(cpluginv1.OperationUpdate)]
	_ = cTypes[int(OperationDelete)-int(cpluginv1.OperationDelete)]
	_ = cTypes[int(OperationSnapshot)-int(cpluginv1.OperationSnapshot)]
}

// Record represents a single data record produced by a source and/or consumed
// by a destination connector.
type Record struct {
	// Position uniquely represents the record.
	Position Position
	// Operation defines what triggered the creation of a record. There are four
	// possibilities: create, update, delete or snapshot. The first three
	// operations are encountered during normal CDC operation, while "snapshot"
	// is meant to represent records during an initial load. Depending on the
	// operation, the record will contain either the state before the change,
	// after the change, or both (see fields Before and After).
	Operation Operation
	// Metadata contains additional information regarding the record.
	Metadata map[string]string

	// Before contains the state of the entity before the operation occurred.
	// This field should only be populated for operations update and delete.
	Before Entity
	// After contains the state of the entity after the operation occurred. This
	// field should only be populated for operations create, update, snapshot.
	After Entity
}

// Bytes returns the JSON encoding of the Record.
// TODO in the future the behavior of this function will be configurable through
//  the SDK.
func (r Record) Bytes() []byte {
	b, err := json.Marshal(r)
	if err != nil {
		// Unlikely to happen, we receive content from a plugin through GRPC.
		// If the content could be marshaled as protobuf it can be as JSON.
		panic(fmt.Errorf("error while marshaling Entity as JSON: %w", err))
	}
	return b
}

type Entity struct {
	// Key represents a value that should identify the entity (e.g. database
	// row). In a destination Key will never be null.
	Key Data
	// Payload holds the actual data of the entity.
	// If the key and payload both contain structured data it is preferable not
	// to include the key data in the payload data.
	// In a destination Payload will never be null.
	Payload Data
}

// Position is a unique identifier for a record. It is the responsibility of the
// Source to choose and assign record positions, it can freely choose a format
// that makes sense and contains everything needed to restart a pipeline at a
// certain position.
type Position []byte

// Data is a structure that contains some bytes. The only structs implementing
// Data are RawData and StructuredData.
type Data interface {
	isData()
	Bytes() []byte
}

// RawData contains unstructured data in form of a byte slice.
type RawData []byte

func (RawData) isData() {}

// Bytes simply casts RawData to a byte slice.
func (d RawData) Bytes() []byte {
	return d
}

// StructuredData contains data in form of a map with string keys and arbitrary
// values.
type StructuredData map[string]interface{}

func (StructuredData) isData() {}

// Bytes returns the JSON encoding of the map.
func (d StructuredData) Bytes() []byte {
	b, err := json.Marshal(d)
	if err != nil {
		// Unlikely to happen, we receive content from a plugin through GRPC.
		// If the content could be marshaled as protobuf it can be as JSON.
		panic(fmt.Errorf("error while marshaling StructuredData as JSON: %w", err))
	}
	return b
}
