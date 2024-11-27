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

package write_and_combine_test

import (
	"context"
	"os"
	"testing"

	"example.com/overwrite_source_destination"
	"example.com/partial_specification"
	"example.com/simple"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/specgen/specgen"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestWriteAndCombine(t *testing.T) {
	testCases := []struct {
		haveConnector     sdk.Connector
		existingSpecsPath string
		wantPath          string
	}{
		{
			haveConnector:     simple.Connector,
			existingSpecsPath: "./simple/existing_specs.yaml",
			wantPath:          "./simple/want.yaml",
		},
		{
			haveConnector:     overwrite_source_destination.Connector,
			existingSpecsPath: "./overwrite_source_destination/existing_specs.yaml",
			wantPath:          "./overwrite_source_destination/want.yaml",
		},
		{
			haveConnector:     partial_specification.Connector,
			existingSpecsPath: "./partial_specification/existing_specs.yaml",
			wantPath:          "./partial_specification/want.yaml",
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
