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
	"github.com/conduitio/conduit-connector-protocol/cplugin"
)

// Specification contains general information regarding the plugin like its name
// and what it does.
type Specification struct {
	// Name is the name of the plugin.
	Name string
	// Summary is a brief description of the plugin and what it does. Try not to
	// exceed 200 characters.
	Summary string
	// Description is a more long form area appropriate for README-like text
	// that the author can provide for explaining the behavior of the connector
	// or specific parameters.
	Description string
	// Version string. Should be prepended with `v` like Go, e.g. `v1.54.3`.
	Version string
	// Author declares the entity that created or maintains this plugin.
	Author string
}

// NewSpecifierPlugin takes a Specification and wraps it into an adapter that
// converts it into a cplugin.SpecifierPlugin.
func NewSpecifierPlugin(specs Specification, source Source, dest Destination) cplugin.SpecifierPlugin {
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

func (s *specifierPluginAdapter) Specify(context.Context, cplugin.SpecifierSpecifyRequest) (cplugin.SpecifierSpecifyResponse, error) {
	return cplugin.SpecifierSpecifyResponse{
		Name:              s.specs.Name,
		Summary:           s.specs.Summary,
		Description:       s.specs.Description,
		Version:           s.specs.Version,
		Author:            s.specs.Author,
		SourceParams:      s.sourceParams,
		DestinationParams: s.destinationParams,
	}, nil
}
