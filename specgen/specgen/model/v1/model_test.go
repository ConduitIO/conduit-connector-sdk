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
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestParametersFromConfig_Sorted(t *testing.T) {
	testCases := []struct {
		name  string
		input config.Parameters
		want  Parameters
	}{
		{
			name: "one required parameter",
			input: config.Parameters{
				"requiredParam": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{config.ValidationRequired{}},
				},
			},
			want: Parameters([]Parameter{
				{
					Name:        "requiredParam",
					Type:        "int",
					Validations: []Validation{{Type: "required"}},
				},
			}),
		},
		{
			name: "one optional parameter",
			input: config.Parameters{
				"optionalParam": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{},
				},
			},
			want: Parameters([]Parameter{
				{
					Name:        "optionalParam",
					Type:        "int",
					Validations: []Validation{},
				},
			}),
		},
		{
			name: "two required, sort by name",
			input: config.Parameters{
				"requiredParamB": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{config.ValidationRequired{}},
				},
				"requiredParamA": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{config.ValidationRequired{}},
				},
			},
			want: Parameters([]Parameter{
				{
					Name:        "requiredParamA",
					Type:        "int",
					Validations: []Validation{{Type: "required"}},
				},
				{
					Name:        "requiredParamB",
					Type:        "int",
					Validations: []Validation{{Type: "required"}},
				},
			}),
		},
		{
			name: "two optional, sort by name",
			input: config.Parameters{
				"optionalParamB": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{},
				},
				"optionalParamA": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{},
				},
			},
			want: Parameters([]Parameter{
				{
					Name:        "optionalParamA",
					Type:        "int",
					Validations: []Validation{},
				},
				{
					Name:        "optionalParamB",
					Type:        "int",
					Validations: []Validation{},
				},
			}),
		},
		{
			name: "required and optional",
			input: config.Parameters{
				"paramB": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{config.ValidationRequired{}},
				},
				"paramA": config.Parameter{
					Type:        config.ParameterTypeInt,
					Validations: []config.Validation{},
				},
			},
			want: Parameters([]Parameter{
				{
					Name:        "paramB",
					Type:        "int",
					Validations: []Validation{{Type: "required"}},
				},
				{
					Name:        "paramA",
					Type:        "int",
					Validations: []Validation{},
				},
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got := Parameters{}.FromConfig(tc.input)
			is.Equal("", cmp.Diff(tc.want, got))
		})
	}
}
