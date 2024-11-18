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

package defaults

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type SourceConfig struct {
	sdk.UnimplementedSourceConfig

	// WithDefaultTagValueNotOverwritten has a default tag that won't be overwritten.
	WithDefaultTagValueNotOverwritten string `default:"bar"`
	// WithDefaultTagValueOverwritten has a default tag that will be overwritten in NewSource().
	WithDefaultTagValueOverwritten int `default:"123"`
	// NoDefaultTagValueOverwritten has no default tag, and a default value will be assigned in NewSource().
	NoDefaultTagValueOverwritten float64
	// NoDefaultTagValueNotOverwritten has no default tag, and the default value won't be assigned in NewSource().
	NoDefaultTagValueNotOverwritten float64
}

var Connector = sdk.Connector{
	NewSpecification: nil,
	NewSource: func() sdk.Source {
		return &Source{
			config: SourceConfig{
				WithDefaultTagValueOverwritten: 456,
				NoDefaultTagValueOverwritten:   789,
			},
		}
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
