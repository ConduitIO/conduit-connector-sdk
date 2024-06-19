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

package sdk

import (
	"context"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
)

// Specification contains general information regarding the plugin like its name
// and what it does.
type Specification pconnector.Specification

// NewSpecifierPlugin takes a Specification and wraps it into an adapter that
// converts it into a pconnector.SpecifierPlugin.
func NewSpecifierPlugin(specs Specification, source Source, dest Destination) pconnector.SpecifierPlugin {
	if source == nil {
		// prevent nil pointer
		source = UnimplementedSource{}
	}
	if dest == nil {
		// prevent nil pointer
		dest = UnimplementedDestination{}
	}

	return &specifierPluginAdapter{
		specs:             specs,
		sourceParams:      source.Parameters(),
		destinationParams: dest.Parameters(),
	}
}

type specifierPluginAdapter struct {
	specs             Specification
	sourceParams      config.Parameters
	destinationParams config.Parameters
}

func (s *specifierPluginAdapter) Specify(context.Context, pconnector.SpecifierSpecifyRequest) (pconnector.SpecifierSpecifyResponse, error) {
	return pconnector.SpecifierSpecifyResponse{
		Specification: pconnector.Specification(s.specs),
	}, nil
}
