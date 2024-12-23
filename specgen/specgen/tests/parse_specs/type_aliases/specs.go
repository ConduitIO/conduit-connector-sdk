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

package type_aliases

import (
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type SourceConfig struct {
	sdk.UnimplementedSourceConfig

	Global                 GlobalConfig `json:"global"`
	CustomDurationField    CustomDuration
	CustomDurationTwoField CustomDuration2
}

type (
	CustomDuration  CustomDuration2
	CustomDuration2 time.Duration
)

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
