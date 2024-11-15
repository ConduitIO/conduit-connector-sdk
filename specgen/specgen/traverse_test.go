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

package specgen

import (
	"reflect"
	"testing"

	"github.com/matryer/is"
)

func TestTraverse(t *testing.T) {
	testCases := []struct {
		name       string
		testObject func() any
		wantPaths  []string
	}{
		{
			name: "struct with primitive fields",
			testObject: func() any {
				type simpleStruct struct {
					Name string
					Age  int
				}
				return simpleStruct{
					Name: "John Doe",
					Age:  30,
				}
			},
			wantPaths: []string{"name", "age"},
		},
		{
			name: "struct with embedded struct",
			testObject: func() any {
				type EmbeddedStruct struct {
					EmbeddedField string
				}
				type testStruct struct {
					EmbeddedStruct
				}

				return testStruct{
					EmbeddedStruct{
						EmbeddedField: "foo",
					},
				}
			},
			wantPaths: []string{"embeddedField"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			var gotPaths []string
			traverseFields(tc.testObject(), func(path string, _ reflect.StructField, _ reflect.Value) {
				gotPaths = append(gotPaths, path)
			})

			is.Equal(tc.wantPaths, gotPaths)
		})
	}
}
