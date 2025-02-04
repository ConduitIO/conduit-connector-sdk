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

package v1

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/evolviconf"
)

const LatestVersion = "1.0"

// Changelog should be adjusted every time we change the specification and add
// a new config version. Based on the changelog the parser will output warnings.
var Changelog = evolviconf.Changelog{
	semver.MustParse("1.0"): {}, // initial version
}

type Specification struct {
	Version                string                 `json:"version" yaml:"version"`
	ConnectorSpecification ConnectorSpecification `json:"specification" yaml:"specification"`
}

type ConnectorSpecification struct {
	Name        string `json:"name" yaml:"name"`
	Summary     string `json:"summary" yaml:"summary"`
	Description string `json:"description" yaml:"description"`
	Version     string `json:"version" yaml:"version"`
	Author      string `json:"author" yaml:"author"`

	Source      Source      `json:"source,omitempty" yaml:"source,omitempty"`
	Destination Destination `json:"destination,omitempty" yaml:"destination,omitempty"`
}

type Source struct {
	Parameters Parameters `json:"parameters,omitempty" yaml:"parameters,omitempty"`
}

type Destination struct {
	Parameters Parameters `json:"parameters,omitempty" yaml:"parameters,omitempty"`
}

type Parameters []Parameter

type Parameter struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Type        ParameterType `json:"type"`
	Default     string        `json:"default"`
	Validations Validations   `json:"validations,omitempty"`
}

type ParameterType string

type Validations []Validation

type Validation struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// -- TO CONFIG ----------------------------------------------------------------

// ToConfig implements evolviconf.VersionedConfig.
func (s Specification) ToConfig() (pconnector.Specification, error) {
	return s.ConnectorSpecification.ToConfig()
}

func (c ConnectorSpecification) ToConfig() (pconnector.Specification, error) {
	sourceParams, err := c.Source.ToConfig()
	if err != nil {
		return pconnector.Specification{}, err
	}
	destinationParams, err := c.Destination.ToConfig()
	if err != nil {
		return pconnector.Specification{}, err
	}

	return pconnector.Specification{
		Name:        c.Name,
		Summary:     c.Summary,
		Description: c.Description,
		Version:     c.Version,
		Author:      c.Author,

		SourceParams:      sourceParams,
		DestinationParams: destinationParams,
	}, nil
}

func (s Source) ToConfig() (config.Parameters, error) {
	return s.Parameters.ToConfig()
}

func (d Destination) ToConfig() (config.Parameters, error) {
	return d.Parameters.ToConfig()
}

func (p Parameters) ToConfig() (config.Parameters, error) {
	var err error
	out := make(config.Parameters, len(p))
	for _, param := range p {
		out[param.Name], err = param.ToConfig()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (p Parameter) ToConfig() (config.Parameter, error) {
	t, err := p.Type.ToConfig()
	if err != nil {
		return config.Parameter{}, err
	}
	v, err := p.Validations.ToConfig()
	if err != nil {
		return config.Parameter{}, err
	}

	return config.Parameter{
		Description: p.Description,
		Type:        t,
		Default:     p.Default,
		Validations: v,
	}, nil
}

func (t ParameterType) ToConfig() (config.ParameterType, error) {
	switch string(t) {
	case config.ParameterTypeString.String():
		return config.ParameterTypeString, nil
	case config.ParameterTypeInt.String():
		return config.ParameterTypeInt, nil
	case config.ParameterTypeFloat.String():
		return config.ParameterTypeFloat, nil
	case config.ParameterTypeBool.String():
		return config.ParameterTypeBool, nil
	case config.ParameterTypeFile.String():
		return config.ParameterTypeFile, nil
	case config.ParameterTypeDuration.String():
		return config.ParameterTypeDuration, nil
	default:
		return 0, fmt.Errorf("unknown parameter type: %s", t)
	}
}

func (v Validations) ToConfig() ([]config.Validation, error) {
	out := make([]config.Validation, len(v))
	for i, validation := range v {
		var err error
		out[i], err = validation.ToConfig()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (v Validation) ToConfig() (config.Validation, error) {
	switch v.Type {
	case config.ValidationTypeRequired.String():
		return config.ValidationRequired{}, nil
	case config.ValidationTypeGreaterThan.String():
		val, err := strconv.ParseFloat(v.Value, 64)
		if err != nil {
			return nil, err
		}
		return config.ValidationGreaterThan{V: val}, nil
	case config.ValidationTypeLessThan.String():
		val, err := strconv.ParseFloat(v.Value, 64)
		if err != nil {
			return nil, err
		}
		return config.ValidationLessThan{V: val}, nil
	case config.ValidationTypeInclusion.String():
		list := strings.Split(v.Value, ",")
		for i, item := range list {
			list[i] = strings.TrimSpace(item)
		}
		return config.ValidationInclusion{List: list}, nil
	case config.ValidationTypeExclusion.String():
		list := strings.Split(v.Value, ",")
		for i, item := range list {
			list[i] = strings.TrimSpace(item)
		}
		return config.ValidationExclusion{List: list}, nil
	case config.ValidationTypeRegex.String():
		regex, err := regexp.Compile(v.Value)
		if err != nil {
			regex = nil
		}
		return config.ValidationRegex{Regex: regex}, nil
	default:
		return nil, fmt.Errorf("unknown validation type: %s", v.Type)
	}
}

// -- FROM CONFIG --------------------------------------------------------------

func (s Specification) FromConfig(spec pconnector.Specification) Specification {
	return Specification{
		Version:                LatestVersion,
		ConnectorSpecification: ConnectorSpecification{}.FromConfig(spec),
	}
}

func (c ConnectorSpecification) FromConfig(spec pconnector.Specification) ConnectorSpecification {
	c.Name = spec.Name
	c.Summary = spec.Summary
	c.Description = spec.Description
	c.Version = spec.Version
	c.Author = spec.Author

	c.Source.Parameters = Parameters{}.FromConfig(spec.SourceParams)
	c.Destination.Parameters = Parameters{}.FromConfig(spec.DestinationParams)
	return c
}

func (Parameters) FromConfig(params config.Parameters) Parameters {
	p := make(Parameters, 0, len(params))

	for name, param := range params {
		paramOut := Parameter{}.FromConfig(param)
		paramOut.Name = name
		p = append(p, paramOut)
	}

	// Parameters are sorted so that required params come first.
	// If two parameters are both required or both optional,
	// the one that's lexicographically smaller comes first.
	sort.Slice(p, func(i, j int) bool {
		pi := params[p[i].Name]
		pj := params[p[j].Name]

		if isConnectorParam(p[i].Name) != isConnectorParam(p[j].Name) {
			return isConnectorParam(p[i].Name)
		}

		if isRequired(pi) != isRequired(pj) {
			return isRequired(pi)
		}

		return p[i].Name < p[j].Name
	})
	return p
}

func isConnectorParam(name string) bool {
	return !strings.HasPrefix(name, "sdk.")
}

func isRequired(p config.Parameter) bool {
	for _, val := range p.Validations {
		if val.Type() == config.ValidationTypeRequired {
			return true
		}
	}

	return false
}

func (p Parameter) FromConfig(param config.Parameter) Parameter {
	return Parameter{
		Name:        "", // Name is the key of the map in Parameters
		Description: param.Description,
		Type:        ParameterType("").FromConfig(param.Type),
		Default:     param.Default,
		Validations: Validations{}.FromConfig(param.Validations),
	}
}

func (ParameterType) FromConfig(t config.ParameterType) ParameterType {
	switch t {
	case config.ParameterTypeString:
		return "string"
	case config.ParameterTypeInt:
		return "int"
	case config.ParameterTypeFloat:
		return "float"
	case config.ParameterTypeBool:
		return "bool"
	case config.ParameterTypeFile:
		return "file"
	case config.ParameterTypeDuration:
		return "duration"
	default:
		return "unknown"
	}
}

func (Validations) FromConfig(v []config.Validation) Validations {
	validations := make(Validations, len(v))
	for i, validation := range v {
		validations[i] = Validation{}.FromConfig(validation)
	}
	return validations
}

func (v Validation) FromConfig(validation config.Validation) Validation {
	switch val := validation.(type) {
	case config.ValidationRequired:
		return Validation{
			Type: config.ValidationTypeRequired.String(),
		}
	case config.ValidationGreaterThan:
		return Validation{
			Type:  config.ValidationTypeGreaterThan.String(),
			Value: fmt.Sprintf("%g", val.V),
		}
	case config.ValidationLessThan:
		return Validation{
			Type:  config.ValidationTypeLessThan.String(),
			Value: fmt.Sprintf("%g", val.V),
		}
	case config.ValidationInclusion:
		return Validation{
			Type:  config.ValidationTypeInclusion.String(),
			Value: strings.Join(val.List, ","),
		}
	case config.ValidationExclusion:
		return Validation{
			Type:  config.ValidationTypeExclusion.String(),
			Value: strings.Join(val.List, ","),
		}
	case config.ValidationRegex:
		value := ""
		if val.Regex != nil {
			value = val.Regex.String()
		}
		return Validation{
			Type:  config.ValidationTypeRegex.String(),
			Value: value,
		}
	default:
		return Validation{
			Type: "unknown",
		}
	}
}
