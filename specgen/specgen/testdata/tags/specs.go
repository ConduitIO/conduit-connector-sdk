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

package tags

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// SourceConfig is a configuration testing the usage of SDK parameter tags
type SourceConfig struct {
	sdk.UnimplementedSourceConfig

	InnerConfig
	// Param1 i am a parameter comment
	Param1 int `validate:"required, gt=0, lt=100" default:"3" json:"my-param"`

	Param2 bool   `validate:"inclusion=true|t, exclusion=false|f, required=false" default:"t"`
	Param3 string `validate:"required=true, regex=.*" default:"yes"`
}

type InnerConfig struct {
	Name string `validate:"required" json:"my-name"`
}

var Connector = sdk.Connector{
	NewSpecification: nil,
	NewSource: func() sdk.Source {
		return &Source{}
	},
	NewDestination: nil,
}

type Source struct {
	sdk.UnimplementedSource
	config SourceConfig
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}
