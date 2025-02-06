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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/comments"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/custom_embedded_struct"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/custom_middleware_config"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/defaults"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/field_names"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/nesting"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/overwrite_source_destination"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/param_sorting"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/partial_specification"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/primitive_field_types"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/simple"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/tags"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/testdata/type_aliases"
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
			wantPath:      "./testdata/custom_embedded_struct/want.yaml",
		},
		{
			haveConnector: custom_middleware_config.Connector,
			wantPath:      "./testdata/custom_middleware_config/want.yaml",
		},
		{
			haveConnector: primitive_field_types.Connector,
			wantPath:      "./testdata/primitive_field_types/want.yaml",
		},
		{
			haveConnector: comments.Connector,
			wantPath:      "./testdata/comments/want.yaml",
		},
		{
			haveConnector: nesting.Connector,
			wantPath:      "./testdata/nesting/want.yaml",
		},
		{
			haveConnector: tags.Connector,
			wantPath:      "./testdata/tags/want.yaml",
		},
		{
			haveConnector: type_aliases.Connector,
			wantPath:      "./testdata/type_aliases/want.yaml",
		},
		{
			haveConnector: field_names.Connector,
			wantPath:      "./testdata/field_names/want.yaml",
		},
		{
			haveConnector: defaults.Connector,
			wantPath:      "./testdata/defaults/want.yaml",
		},
		{
			haveConnector: param_sorting.Connector,
			wantPath:      "./testdata/param_sorting/want.yaml",
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
	testCases := []struct {
		haveConnector     sdk.Connector
		existingSpecsPath string
		wantPath          string
	}{
		{
			haveConnector:     simple.Connector,
			existingSpecsPath: "./testdata/simple/existing_specs.yaml",
			wantPath:          "./testdata/simple/want.yaml",
		},
		{
			haveConnector:     overwrite_source_destination.Connector,
			existingSpecsPath: "./testdata/overwrite_source_destination/existing_specs.yaml",
			wantPath:          "./testdata/overwrite_source_destination/want.yaml",
		},
		{
			haveConnector:     partial_specification.Connector,
			existingSpecsPath: "./testdata/partial_specification/existing_specs.yaml",
			wantPath:          "./testdata/partial_specification/want.yaml",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.wantPath, func(t *testing.T) {
			is := is.New(t)
			oldExisting, err := os.ReadFile(tc.existingSpecsPath)
			is.NoErr(err)
			t.Cleanup(func() {
				err := os.WriteFile(tc.existingSpecsPath, oldExisting, 0o644)
				if err != nil {
					t.Logf("failed reverting changes to existing specs file: %v", err)
				}
			})

			specs, err := specgen.ExtractSpecification(context.Background(), tc.haveConnector)
			is.NoErr(err)
			specBytes, err := specgen.SpecificationToYaml(specs)
			is.NoErr(err)

			err = specgen.WriteAndCombine(specBytes, tc.existingSpecsPath)
			is.NoErr(err)

			got, err := os.ReadFile(tc.existingSpecsPath)
			is.NoErr(err)

			want, err := os.ReadFile(tc.wantPath)
			is.NoErr(err)

			is.Equal("", cmp.Diff(got, want))
		})
	}
}
