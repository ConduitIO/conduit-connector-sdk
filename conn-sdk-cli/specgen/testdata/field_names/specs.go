// Copyright © 2024 Meroxa, Inc.
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

package field_names

import (
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type EmbeddedConfig struct {
	// Name of this field can be:
	// (1) embeddedConfigField (no JSON tag, when this struct is embedded)
	// (2) embeddedConfigAsField.embeddedConfigField (no JSON tag, when this struct is used as a field)
	EmbeddedConfigField uint64
}

type SourceConfig struct {
	sdk.UnimplementedSourceConfig
	EmbeddedConfig

	EmbeddedConfigAsField EmbeddedConfig

	// Duration does not have a name so the type name is used.
	time.Duration `default:"1s"`
	// Name of this field should be field_with_json_tag_here, because of the JSON tag
	FieldWithJSONTag string `json:"field_with_json_tag_here"`
	// Name of this field should be yaml-tag-is-ignored, because the YAML tag should be ignored
	YAMLTagIsIgnored string `json:"yaml-tag-is-ignored" yaml:"yamlfieldname"`

	// Currently, paramgen generates uppercasefielD for this field name
	// see: https://github.com/ConduitIO/conduit-commons/issues/138
	// UPPERCASEFIELD             float32

	MixedCaseField             float32
	Snake_case_field           float32
	_FieldStartsWithUnderscore rune
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
