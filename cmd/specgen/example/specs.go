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

	internal "example.com/asdf/internal"
)

type GlobalConfig struct {
	// GlobalString is an example parameter. This comment will be converted into
	// the parameter description. Can be multiline too! Just write a godoc like
	// you normally would.
	GlobalString string `default:"foo" required:"true"`
	// GlobalDuration demonstrates that time.Duration is also allowed.
	GlobalDuration time.Duration `default:"1s"`
}

type SourceConfig struct {
	GlobalConfig
	MyInt  int
	Nested struct {
		MyFloat32 float32
		MyFloat64 float64
	} `name:"tagsWorkForNestedStructs"`
}

type DestinationConfig struct {
	G      internal.GlobalConfig
	MyBool bool `name:"myBoolino"`

	// this field is ignored because it is not exported
	ignoreThis http.Client
}

// Specs contains the specifications. This comment can describe the struct
// itself and won't be included in the generated specifications.
//
//spec:summary This is a test summary.
//spec:description
// This is a test description. It can start in a new line and be distributed
// across multiple lines. Standard Markdown syntax _can_ be used.
//spec:author Example Inc.
//spec:version v0.1.1
type Specs struct {
	//spec:sourceParams
	SourceConfig
	//spec:destinationParams
	DestinationConfig DestinationConfig
}
