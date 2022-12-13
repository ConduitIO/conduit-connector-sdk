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

type SourceConfig struct {
	// GlobalConfig parameters should be nested under "global". This comment
	// should be ignored.
	Global GlobalConfig `json:"global"`
	// Nested structs can be used to create namespaces
	Nested struct {
		// FORMATThisName should become "formatThisName". Default is not a float
		// but that's not a problem, paramgen does not validate correctness.
		FORMATThisName float32 `default:"this is not a float"`
		// unexported fields should be ignored.
		unexportedField string
	} `json:"nestMeHere"`
	// AnotherNested is also nested under nestMeHere.
	AnotherNested int `json:"nestMeHere.anotherNested"`
}
