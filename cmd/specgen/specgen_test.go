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

package specgen

import (
	"errors"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestParseSpecificationSuccess(t *testing.T) {
	testCases := []struct {
		path string
		want sdk.Specification
	}{{
		path: "./testdata/basic",
		want: sdk.Specification{
			Name:        "example.com/test",
			Summary:     "This is a basic summary.",
			Description: "This is a basic description. It can start in a new line and be distributed\nacross multiple lines. Standard Markdown syntax _can_ be used.",
			Version:     "v0.1.0",
			Author:      "Example Inc.",
			DestinationParams: map[string]sdk.Parameter{
				"foo": {
					Default:     "bar",
					Description: "foo is a required field in the global config with the name\n\"foo\" and default value \"bar\".",
					Required:    true,
				},
				"myDestinationString": {
					Description: "myDestinationString is a field in the destination config.",
				},
			},
			SourceParams: map[string]sdk.Parameter{
				"foo": {
					Default:     "bar",
					Description: "foo is a required field in the global config with the name\n\"foo\" and default value \"bar\".",
					Required:    true,
				},
				"myString":   {},
				"myBool":     {},
				"myInt":      {},
				"myUint":     {},
				"myInt8":     {},
				"myUint8":    {},
				"myInt16":    {},
				"myUint16":   {},
				"myInt32":    {},
				"myUint32":   {},
				"myInt64":    {},
				"myUint64":   {},
				"myByte":     {},
				"myRune":     {},
				"myFloat32":  {},
				"myFloat64":  {},
				"myDuration": {},
			},
		},
	}, {
		path: "./testdata/complex",
		want: sdk.Specification{
			/*
				{example.com/test This is a complex summary. It can be
				multiline. This is a complex description. It can start in a new line and be distributed
				across multiple lines. Standard Markdown syntax _can_ be used. v0.1.1 Example Inc. map[global.duration:{1s true } myBoolino:{ false }] map[global.duration:{1s true } nestMeHere.anotherNested:{ false nestMeHere.anotherNested is also nested under nestMeHere.} nestMeHere.formatThisName:{this is not a float false formatThisName should become "formatThisName". Default is not a float
				but that's not a problem, specgen does not validate correctness.}]}
			*/
			Name:        "example.com/test",
			Summary:     "This is a complex summary. It can be\nmultiline.",
			Description: "This is a complex description. It can start in a new line and be distributed\nacross multiple lines. Standard Markdown syntax _can_ be used.",
			Version:     "v0.1.1",
			Author:      "Example Inc.",
			DestinationParams: map[string]sdk.Parameter{
				"global.duration": {
					Default:     "1s",
					Description: "duration does not have a name so the type name is used.",
					Required:    true,
				},
				"myBool": {},
			},
			SourceParams: map[string]sdk.Parameter{
				"global.duration": {
					Default:     "1s",
					Description: "duration does not have a name so the type name is used.",
					Required:    true,
				},
				"nestMeHere.anotherNested": {
					Description: "nestMeHere.anotherNested is also nested under nestMeHere.",
				},
				"nestMeHere.formatThisName": {
					Default:     "this is not a float",
					Description: "formatThisName should become \"formatThisName\". Default is not a float\nbut that's not a problem, specgen does not validate correctness.",
				},
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			is := is.New(t)
			got, err := ParseSpecification(tc.path)
			is.NoErr(err)
			is.Equal(got, tc.want)
		})
	}
}

func TestParseSpecificationFail(t *testing.T) {
	testCases := []struct {
		path    string
		wantErr error
	}{{
		path:    "./testdata/invalid1",
		wantErr: errors.New("we do not support parameters from package net/http (please use builtin types or time.Duration)"),
	}, {
		path:    "./testdata/invalid2",
		wantErr: errors.New("could not find any struct with a comment that starts with \"spec:summary\", please define such a struct"),
	}}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			is := is.New(t)
			_, err := ParseSpecification(tc.path)
			is.True(err != nil)
			// unwrap core error
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
