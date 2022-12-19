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

package internal

import (
	"regexp"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestGenerateCodeWithRegex(t *testing.T) {
	is := is.New(t)
	got := GenerateCode(map[string]sdk.Parameter{
		"int.param": {
			Default:     "1",
			Description: "my int param with \"quotes\"",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
				sdk.ValidationExclusion{List: []string{"0", "-1"}},
				sdk.ValidationGreaterThan{Value: -3},
				sdk.ValidationLessThan{Value: 3},
			},
		},
		"bool.param": {
			Default:     "true",
			Description: "my bool param",
			Type:        sdk.ParameterTypeBool,
			Validations: []sdk.Validation{
				sdk.ValidationRegex{Regex: regexp.MustCompile(".*")},
			},
		},
		"string.param": {
			Description: "simple string param",
			Type:        sdk.ParameterTypeString,
		},
	}, "s3", "SourceConfig")

	want := `// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-connector-sdk/cmd/paramgen

package s3

import (
	"regexp"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (SourceConfig) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"bool.param": {
			Default:     "true",
			Description: "my bool param",
			Type:        sdk.ParameterTypeBool,
			Validations: []sdk.Validation{
				sdk.ValidationRegex{Regex: regexp.MustCompile(".*")},
			},
		},
		"int.param": {
			Default:     "1",
			Description: "my int param with \"quotes\"",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
				sdk.ValidationExclusion{List: []string{"0", "-1"}},
				sdk.ValidationGreaterThan{Value: -3},
				sdk.ValidationLessThan{Value: 3},
			},
		},
		"string.param": {
			Default:     "",
			Description: "simple string param",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
	}
}
`
	is.Equal(got, want)
}

func TestGenerateCodeWithoutRegex(t *testing.T) {
	is := is.New(t)
	got := GenerateCode(map[string]sdk.Parameter{
		"int.param": {
			Default:     "1",
			Description: "my int param",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{},
		},
		"duration.param": {
			Default:     "1s",
			Description: "my duration param",
			Type:        sdk.ParameterTypeDuration,
			Validations: []sdk.Validation{
				sdk.ValidationInclusion{List: []string{"1s", "2s", "3s"}},
			},
		},
	}, "file", "Config")

	want := `// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-connector-sdk/cmd/paramgen

package file

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (Config) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"duration.param": {
			Default:     "1s",
			Description: "my duration param",
			Type:        sdk.ParameterTypeDuration,
			Validations: []sdk.Validation{
				sdk.ValidationInclusion{List: []string{"1s", "2s", "3s"}},
			},
		},
		"int.param": {
			Default:     "1",
			Description: "my int param",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{},
		},
	}
}
`
	is.Equal(got, want)
}
