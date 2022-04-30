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
)

type SourceConfig struct {
	// GlobalConfig parameters should be nested under "global". This comment
	// should be ignored.
	Global GlobalConfig
	// Nested structs can be used to create namespaces
	Nested struct {
		// FORMATThisName should become "formatThisName". Default is not a float
		// but that's not a problem, specgen does not validate correctness.
		FORMATThisName float32 `default:"this is not a float"`
		// unexported fields should be ignored.
		unexportedField string
	} `name:"nestMeHere"`
	// AnotherNested is also nested under nestMeHere.
	AnotherNested int `name:"nestMeHere.anotherNested"`
}

// Specs contains the specifications. This comment can describe the struct
// itself and won't be included in the generated specifications.
// All spec:X comments can parse multiline docs.
//
//spec:summary
// This is a complex summary. It can be
// multiline.
//spec:description
// This is a complex description. It can start in a new line and be distributed
// across multiple lines. Standard Markdown syntax _can_ be used.
//spec:author
// Example Inc.
//spec:version
// v0.1.1
type Specs struct {
	//spec:sourceParams
	SourceConfig
	// DestinationConfig can be defined as an anonymous struct in Specs.
	//spec:destinationParams
	DestinationConfig struct {
		GlobalConfig `name:"global"`
		MyBool       bool `name:""`

		// unexported fields should be ignored.
		ignoreThis http.Client
	}
}
