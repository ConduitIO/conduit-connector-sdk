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

package example

import (
	"net/http"
	"time"
)

// GlobalConfig is a reusable config struct used in the source and destination
// config.
type GlobalConfig struct {
	// MyGlobalString is a required field in the global config with the name
	// "foo" and default value "bar".
	MyGlobalString string `json:"foo" default:"bar" validate:"required"`
}

// SourceConfig this comment will be ignored.
type SourceConfig struct {
	GlobalConfig

	// MyString my string description
	MyString string
	MyBool   bool

	MyInt    int `validate:"lt=100, gt=0"`
	MyUint   uint
	MyInt8   int8
	MyUint8  uint8
	MyInt16  int16
	MyUint16 uint16
	MyInt32  int32
	MyUint32 uint32
	MyInt64  int64
	MyUint64 uint64

	MyByte byte
	MyRune rune

	MyFloat32 float32
	MyFloat64 float64

	MyDuration time.Duration

	MyIntSlice   []int
	MyFloatSlice []float32
	MyDurSlice   []time.Duration

	// this field is ignored because it is not exported
	ignoreThis http.Client
}
