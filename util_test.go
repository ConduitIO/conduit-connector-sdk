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
	"testing"

	"github.com/matryer/is"
)

func TestParseConfig_Simple_Struct(t *testing.T) {
	is := is.New(t)

	type Person struct {
		Name   string `mapstructure:"person_name"`
		Age    int
		Emails []string
		Extra  map[string]string
	}

	input := map[string]interface{}{
		"person_name": "meroxa",
		"age":         91,
		"emails":      []string{"one", "two", "three"},
		"extra": map[string]string{
			"string": "value",
		},
	}
	want := Person{
		Name:   "meroxa",
		Age:    91,
		Emails: []string{"one", "two", "three"},
		Extra:  map[string]string{"string": "value"},
	}

	var got Person
	err := parseConfig(input, &got)
	is.NoErr(err)
	is.Equal(want, got)
}

func Test_ParseConfig_Embedded_Struct(t *testing.T) {
	is := is.New(t)
	type Family struct {
		LastName string
	}
	type Location struct {
		City string
	}
	type Person struct {
		Family    `mapstructure:",squash"`
		Location  `mapstructure:",squash"`
		FirstName string
	}

	input := map[string]interface{}{
		"FirstName": "conduit",
		"lastname":  "meroxa",
		"City":      "San Francisco",
	}
	want := Person{
		Family:    Family{LastName: "meroxa"},
		Location:  Location{City: "San Francisco"},
		FirstName: "conduit",
	}

	var got Person
	err := parseConfig(input, &got)
	is.NoErr(err)
	is.Equal(want, got)
}
