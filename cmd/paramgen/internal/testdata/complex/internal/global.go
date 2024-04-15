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

package internal

import "time"

// GlobalConfig is an internal struct that paramgen still parses.
type GlobalConfig struct {
	// Duration does not have a name so the type name is used.
	time.Duration `default:"1s"` // line comments on fields with doc comments are ignored

	WildcardStrings map[string]string        `default:"foo" validate:"required"`
	WildcardInts    map[string]time.Duration `json:"renamed" default:"1s"`
	WildcardStructs WildcardStruct
}

type WildcardStruct map[string]struct {
	Name string
}
