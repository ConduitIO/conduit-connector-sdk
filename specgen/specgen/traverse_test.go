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
				type Foo struct {
					Bar string
					Baz int
				}
				return Foo{}
			},
			wantPaths: []string{"bar", "baz"},
		},
		{
			name: "struct with embedded struct",
			testObject: func() any {
				type Bar struct {
					Baz string
				}
				type Foo struct {
					Bar
				}

				return Foo{}
			},
			wantPaths: []string{"baz"},
		},
		{
			name: "struct with struct field",
			testObject: func() any {
				type Bar struct {
					Baz string
				}
				type Foo struct {
					Bar Bar
				}

				return Foo{}
			},
			wantPaths: []string{"bar", "bar.baz"},
		},
		{
			name: "struct with pointer-to-struct field",
			testObject: func() any {
				type Bar struct {
					Baz string
				}
				type Foo struct {
					Bar *Bar
				}

				return Foo{
					Bar: &Bar{},
				}
			},
			wantPaths: []string{"bar", "bar.baz"},
		},
		{
			name: "struct with map field",
			testObject: func() any {
				type Foo struct {
					Bar map[string]string
				}

				return Foo{
					Bar: map[string]string{
						"baz": "baz-val",
						"qux": "qux-val",
					},
				}
			},
			wantPaths: []string{"barMap"},
		},
		{
			name: "struct with map field, value is a struct",
			testObject: func() any {
				type Baz struct {
					Qux string
				}
				type Foo struct {
					BarMap map[string]Baz
				}

				return Foo{
					BarMap: map[string]Baz{
						"bar-map-key": {Qux: "qux-val"},
					},
				}
			},
			wantPaths: []string{"barMap.*.qux"},
		},
		{
			name: "struct with nested struct field",
			testObject: func() any {
				type Foo struct {
					Bar struct {
						Baz string
						Qux string
					}
				}

				return Foo{}
			},
			wantPaths: []string{"bar", "bar.baz", "bar.qux"},
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
