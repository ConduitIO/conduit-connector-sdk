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

//go:generate stringer -type=Operation -linecomment

package sdk

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
)

const (
	OperationCreate   Operation = iota + 1 // create
	OperationUpdate                        // update
	OperationDelete                        // delete
	OperationSnapshot                      // snapshot
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

func (i Operation) MarshalText() ([]byte, error) {
	return []byte(i.String()), nil
}

func (i *Operation) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		return nil // empty string, do nothing
	}

	switch string(b) {
	case OperationCreate.String():
		*i = OperationCreate
	case OperationUpdate.String():
		*i = OperationUpdate
	case OperationDelete.String():
		*i = OperationDelete
	case OperationSnapshot.String():
		*i = OperationSnapshot
	default:
		// it's not a known operation, but we also allow Operation(int)
		valIntRaw := strings.TrimSuffix(strings.TrimPrefix(string(b), "Operation("), ")")
		valInt, err := strconv.Atoi(valIntRaw)
		if err != nil {
			return fmt.Errorf("unknown operation %q", b)
		}
		*i = Operation(valInt)
	}

	return nil
}

// Record represents a single data record produced by a source and/or consumed
// by a destination connector.
type Record struct {
	// Position uniquely represents the record.
	Position Position `json:"position"`
	// Operation defines what triggered the creation of a record. There are four
	// possibilities: create, update, delete or snapshot. The first three
	// operations are encountered during normal CDC operation, while "snapshot"
	// is meant to represent records during an initial load. Depending on the
	// operation, the record will contain either the payload before the change,
	// after the change, or both (see field Payload).
	Operation Operation `json:"operation"`
	// Metadata contains additional information regarding the record.
	Metadata Metadata `json:"metadata"`

	// Key represents a value that should identify the entity (e.g. database
	// row).
	Key Data `json:"key"`
	// Payload holds the payload change (data before and after the operation
	// occurred).
	Payload Change `json:"payload"`
}

type Metadata map[string]string

// Bytes returns the JSON encoding of the Record.
// TODO in the future the behavior of this function will be configurable through
//
//	the SDK.
func (r Record) Bytes() []byte {
	if r.Metadata == nil {
		// since we are dealing with a Record value this will not be seen
		// outside this function
		r.Metadata = make(map[string]string)
	}

	// before encoding the record set the opencdc version metadata field
	r.Metadata.SetOpenCDCVersion()
	// we don't want to mutate the metadata permanently, so we revert it
	// when we are done
	defer func() {
		delete(r.Metadata, MetadataOpenCDCVersion)
	}()

	b, err := json.Marshal(r)
	if err != nil {
		// Unlikely to happen, we receive content from a plugin through GRPC.
		// If the content could be marshaled as protobuf it can be as JSON.
		panic(fmt.Errorf("error while marshaling Entity as JSON: %w", err))
	}
	return b
}

type Change struct {
	// Before contains the data before the operation occurred. This field is
	// optional and should only be populated for operations OperationUpdate
	// OperationDelete (if the system supports fetching the data before the
	// operation).
	Before Data `json:"before"`
	// After contains the data after the operation occurred. This field should
	// be populated for all operations except OperationDelete.
	After Data `json:"after"`
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
