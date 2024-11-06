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
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestSpecifier_Connector(t *testing.T) {
	is := is.New(t)

	c := Connector{
		NewSpecification: testConnSpec,
	}

	s := NewSpecifierPlugin(c.NewSpecification())
	got, err := s.Specify(context.Background(), pconnector.SpecifierSpecifyRequest{})

	want := pconnector.SpecifierSpecifyResponse{
		Specification: pconnector.Specification{
			Name:        "testSpecConn",
			Summary:     "summary of spec conn",
			Description: "desc of spec conn",
			Version:     "(devel+1)",
			Author:      "sdk-max",
			SourceParams: map[string]config.Parameter{
				"srcParam1": {
					Default:     "",
					Description: "source param 1",
					Type:        config.ParameterTypeString,
					Validations: []config.Validation{
						config.ValidationRequired{},
					},
				},
				"srcParam2": {
					Default:     "set",
					Description: "source param 2",
					Type:        config.ParameterTypeString,
					Validations: []config.Validation{},
				},
			},
			DestinationParams: map[string]config.Parameter{
				"destParam1": {
					Default:     "",
					Description: "dest param 1",
					Type:        config.ParameterTypeString,
					Validations: []config.Validation{
						config.ValidationRequired{},
					},
				},
				"destParam2": {
					Default:     "unset",
					Description: "dest param 2",
					Type:        config.ParameterTypeString,
					Validations: []config.Validation{},
				},
			},
		},
	}

	is.NoErr(err)
	is.Equal("", cmp.Diff(want, got))
}

func testConnSpec() Specification {
	return Specification{
		Name:        "testSpecConn",
		Summary:     "summary of spec conn",
		Description: "desc of spec conn",
		Version:     "(devel+1)",
		Author:      "sdk-max",
		SourceParams: map[string]config.Parameter{
			"srcParam1": {
				Default:     "",
				Description: "source param 1",
				Type:        config.ParameterTypeString,
				Validations: []config.Validation{
					config.ValidationRequired{},
				},
			},
			"srcParam2": {
				Default:     "set",
				Description: "source param 2",
				Type:        config.ParameterTypeString,
				Validations: []config.Validation{},
			},
		},
		DestinationParams: map[string]config.Parameter{
			"destParam1": {
				Default:     "",
				Description: "dest param 1",
				Type:        config.ParameterTypeString,
				Validations: []config.Validation{
					config.ValidationRequired{},
				},
			},
			"destParam2": {
				Default:     "unset",
				Description: "dest param 2",
				Type:        config.ParameterTypeString,
				Validations: []config.Validation{},
			},
		},
	}
}
