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

package custom_middleware_config

import (
	"github.com/conduitio/conduit-commons/lang"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// SourceConfig this comment will be ignored.
type SourceConfig struct {
	sdk.DefaultSourceMiddleware

	// MyString my string description
	MyString string
}

var Connector = sdk.Connector{
	NewSpecification: nil,
	NewSource: func() sdk.Source {
		return &Source{
			config: SourceConfig{
				DefaultSourceMiddleware: sdk.DefaultSourceMiddleware{
					SourceWithSchemaExtraction: sdk.SourceWithSchemaExtraction{
						KeyEnabled: lang.Ptr(false),
					},
					SourceWithBatch: sdk.SourceWithBatch{
						BatchSize: lang.Ptr(1234),
					},
				},
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
