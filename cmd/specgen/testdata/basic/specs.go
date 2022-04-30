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
	MyGlobalString string `name:"foo" default:"bar" required:"true"`
}

// DestinationConfig can be used to parse the configuration for the destination
// connector.
type DestinationConfig struct {
	GlobalConfig

	// MyDestinationString is a field in the destination config.
	MyDestinationString string
}

// SourceConfig can be used to parse the configuration for the source connector.
type SourceConfig struct {
	GlobalConfig

	MyString string
	MyBool   bool

	MyInt    int
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

	// this field is ignored because it is not exported
	ignoreThis http.Client
}

// Specs contains the specifications. This comment describes the struct itself
// and won't be included in the generated specifications.
//
//spec:summary This is a basic summary.
//spec:description
// This is a basic description. It can start in a new line and be distributed
// across multiple lines. Standard Markdown syntax _can_ be used.
//spec:author Example Inc.
//spec:version v0.1.0
type Specs struct {
	//spec:sourceParams
	SourceConfig
	//spec:destinationParams
	DestinationConfig
}
