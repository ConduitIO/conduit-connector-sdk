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

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
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

// Parameter defines a single connector parameter.
type Parameter struct {
	// Default is the default value of the parameter, if any.
	Default string
	// Required controls if the parameter will be shown as required or optional.
	// Deprecated: add ValidationRequired to Parameter.Validations instead.
	Required bool
	// Description holds a description of the field and how to configure it.
	Description string
	// Type defines the parameter data type.
	Type ParameterType
	// Validations slice of validations to be checked for the parameter.
	Validations []Validation
}

// NewSpecifierPlugin takes a Specification and wraps it into an adapter that
// converts it into a cpluginv1.SpecifierPlugin.
func NewSpecifierPlugin(specs Specification, source Source, dest Destination) cpluginv1.SpecifierPlugin {
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
	sourceParams      map[string]Parameter
	destinationParams map[string]Parameter
}

func (s *specifierPluginAdapter) Specify(context.Context, cpluginv1.SpecifierSpecifyRequest) (cpluginv1.SpecifierSpecifyResponse, error) {
	return cpluginv1.SpecifierSpecifyResponse{
		Name:              s.specs.Name,
		Summary:           s.specs.Summary,
		Description:       s.specs.Description,
		Version:           s.specs.Version,
		Author:            s.specs.Author,
		SourceParams:      s.convertParameters(s.sourceParams),
		DestinationParams: s.convertParameters(s.destinationParams),
	}, nil
}

func (s *specifierPluginAdapter) convertParameters(params map[string]Parameter) map[string]cpluginv1.SpecifierParameter {
	out := make(map[string]cpluginv1.SpecifierParameter)
	for k, v := range params {
		out[k] = s.convertParameter(v)
	}
	return out
}

func (s *specifierPluginAdapter) convertParameter(p Parameter) cpluginv1.SpecifierParameter {
	return cpluginv1.SpecifierParameter{
		Default:     p.Default,
		Required:    p.Required,
		Description: p.Description,
		Type:        cpluginv1.ParameterType(p.Type),
		Validations: convertValidations(p.Validations),
	}
}
