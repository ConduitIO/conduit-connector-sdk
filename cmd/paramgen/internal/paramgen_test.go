// Copyright © 2023 Meroxa, Inc.
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
	"errors"
	"regexp"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestParseSpecificationSuccess(t *testing.T) {
	testCases := []struct {
		path string
		name string
		pkg  string
		want map[string]sdk.Parameter
	}{{
		path: "./testdata/basic",
		name: "SourceConfig",
		pkg:  "example",
		want: map[string]sdk.Parameter{
			"globalConfig.foo": {
				Default:     "bar",
				Description: "foo is a required field in the global config with the name \"foo\" and default value \"bar\".",
				Type:        sdk.ParameterTypeString,
				Validations: []sdk.Validation{
					sdk.ValidationRequired{},
				},
			},
			"myString": {
				Description: "myString my string description",
				Type:        sdk.ParameterTypeString,
			},
			"myBool": {Type: sdk.ParameterTypeBool},
			"myInt": {
				Type: sdk.ParameterTypeInt,
				Validations: []sdk.Validation{
					sdk.ValidationLessThan{
						Value: 100,
					},
					sdk.ValidationGreaterThan{
						Value: 0,
					},
				},
			},
			"myUint":       {Type: sdk.ParameterTypeInt},
			"myInt8":       {Type: sdk.ParameterTypeInt},
			"myUint8":      {Type: sdk.ParameterTypeInt},
			"myInt16":      {Type: sdk.ParameterTypeInt},
			"myUint16":     {Type: sdk.ParameterTypeInt},
			"myInt32":      {Type: sdk.ParameterTypeInt},
			"myUint32":     {Type: sdk.ParameterTypeInt},
			"myInt64":      {Type: sdk.ParameterTypeInt},
			"myUint64":     {Type: sdk.ParameterTypeInt},
			"myByte":       {Type: sdk.ParameterTypeString},
			"myRune":       {Type: sdk.ParameterTypeInt},
			"myFloat32":    {Type: sdk.ParameterTypeFloat},
			"myFloat64":    {Type: sdk.ParameterTypeFloat},
			"myDuration":   {Type: sdk.ParameterTypeDuration},
			"myIntSlice":   {Type: sdk.ParameterTypeString},
			"myFloatSlice": {Type: sdk.ParameterTypeString},
			"myDurSlice":   {Type: sdk.ParameterTypeString},
		},
	},
		{
			path: "./testdata/complex",
			name: "SourceConfig",
			pkg:  "example",
			want: map[string]sdk.Parameter{
				"global.duration": {
					Default:     "1s",
					Description: "duration does not have a name so the type name is used.",
					Type:        sdk.ParameterTypeDuration,
				},
				"nestMeHere.anotherNested": {
					Type:        sdk.ParameterTypeInt,
					Description: "nestMeHere.anotherNested is also nested under nestMeHere. This is a block comment.",
				},
				"nestMeHere.formatThisName": {
					Type:        sdk.ParameterTypeFloat,
					Default:     "this is not a float",
					Description: "formatThisName should become \"formatThisName\". Default is not a float but that's not a problem, paramgen does not validate correctness.",
				},
				"customType": {
					Type:        sdk.ParameterTypeDuration,
					Description: "customType uses a custom type that is convertible to a supported type. Line comments are allowed.",
				},
			},
		},
		{
			path: "./testdata/tags",
			name: "Config",
			pkg:  "tags",
			want: map[string]sdk.Parameter{
				"innerConfig.my-name": {
					Type:        sdk.ParameterTypeString,
					Validations: []sdk.Validation{sdk.ValidationRequired{}},
				},
				"my-param": {
					Type:        sdk.ParameterTypeInt,
					Description: "my-param i am a parameter comment",
					Default:     "3",
					Validations: []sdk.Validation{
						sdk.ValidationRequired{},
						sdk.ValidationGreaterThan{Value: 0},
						sdk.ValidationLessThan{Value: 100},
					},
				},
				"param2": {
					Type:    sdk.ParameterTypeBool,
					Default: "t",
					Validations: []sdk.Validation{
						sdk.ValidationInclusion{List: []string{"true", "t"}},
						sdk.ValidationExclusion{List: []string{"false", "f"}},
					},
				},
				"param3": {
					Type:    sdk.ParameterTypeString,
					Default: "yes",
					Validations: []sdk.Validation{
						sdk.ValidationRequired{},
						sdk.ValidationRegex{Regex: regexp.MustCompile(".*")},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			is := is.New(t)
			got, pkg, err := ParseParameters(tc.path, tc.name)
			is.NoErr(err)
			is.Equal(pkg, tc.pkg)
			is.Equal(got, tc.want)
		})
	}
}

func TestParseSpecificationFail(t *testing.T) {
	testCases := []struct {
		path    string
		name    string
		wantErr error
	}{{
		path:    "./testdata/invalid1",
		name:    "SourceConfig",
		wantErr: errors.New("we do not support parameters from package net/http (please use builtin types or time.Duration)"),
	}, {
		path:    "./testdata/invalid2",
		name:    "SourceConfig",
		wantErr: errors.New("invalid value for tag validate: invalidValidation=hi"),
	}, {
		path:    "./testdata/basic",
		name:    "SomeConfig",
		wantErr: errors.New("struct \"SomeConfig\" was not found in the package \"example\""),
	}}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			is := is.New(t)
			_, pkg, err := ParseParameters(tc.path, tc.name)
			is.Equal(pkg, "")
			is.True(err != nil)
			for {
				unwrapped := errors.Unwrap(err)
				if unwrapped == nil {
					break
				}
				err = unwrapped
			}
			is.Equal(err, tc.wantErr)
		})
	}
}
