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

package specgen_test

import (
	"context"
	"os"
	"testing"

	"example.com/comments"
	"example.com/custom_embedded_struct"
	"example.com/custom_middleware_config"
	"example.com/defaults"
	"example.com/field_names"
	"example.com/nesting"
	"example.com/primitive_field_types"
	"example.com/simple"
	"example.com/tags"
	"example.com/type_aliases"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/specgen/specgen"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestParseSpecification(t *testing.T) {
	testCases := []struct {
		haveConnector sdk.Connector
		wantPath      string
	}{
		{
			haveConnector: custom_embedded_struct.Connector,
			wantPath:      "./custom_embedded_struct/want.yaml",
		},
		{
			haveConnector: custom_middleware_config.Connector,
			wantPath:      "./custom_middleware_config/want.yaml",
		},
		{
			haveConnector: primitive_field_types.Connector,
			wantPath:      "./primitive_field_types/want.yaml",
		},
		{
			haveConnector: comments.Connector,
			wantPath:      "./comments/want.yaml",
		},
		{
			haveConnector: nesting.Connector,
			wantPath:      "./nesting/want.yaml",
		},
		{
			haveConnector: tags.Connector,
			wantPath:      "./tags/want.yaml",
		},
		{
			haveConnector: type_aliases.Connector,
			wantPath:      "./type_aliases/want.yaml",
		},
		{
			haveConnector: field_names.Connector,
			wantPath:      "./field_names/want.yaml",
		},
		{
			haveConnector: defaults.Connector,
			wantPath:      "./defaults/want.yaml",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wantPath, func(t *testing.T) {
			is := is.New(t)
			specs, err := specgen.ExtractSpecification(context.Background(), tc.haveConnector)
			is.NoErr(err)
			got, err := specgen.SpecificationToYaml(specs)
			is.NoErr(err)

			want, err := os.ReadFile(tc.wantPath)
			is.NoErr(err)

			is.Equal("", cmp.Diff(string(want), string(got)))
		})
	}
}

func TestWriteAndCombine(t *testing.T) {
	is := is.New(t)

	specs, err := specgen.ExtractSpecification(context.Background(), simple.Connector)
	is.NoErr(err)
	specBytes, err := specgen.SpecificationToYaml(specs)
	is.NoErr(err)

	err = specgen.WriteAndCombine(specBytes, "./simple/existing_specs.yaml")
	is.NoErr(err)
}
