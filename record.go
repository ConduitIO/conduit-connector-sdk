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
	"time"
)

// Record represents a single data record produced by a source and/or consumed
// by a destination connector.
type Record struct {
	// Position uniquely represents the record.
	Position Position
	// Metadata contains additional information regarding the record.
	Metadata map[string]string
	// CreatedAt represents the time when the change occurred in the source
	// system. If that's impossible to find out, then it should be the time the
	// change was detected by the connector.
	CreatedAt time.Time
	// Key represents a value that should be the same for records originating
	// from the same source entry (e.g. same DB row). In a destination Key will
	// never be null.
	Key Data
	// Payload holds the actual information that the record is transmitting. In
	// a destination Payload will never be null.
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
		panic(fmt.Errorf("StructuredData error while marshaling as JSON: %w", err))
	}
	return b
}
