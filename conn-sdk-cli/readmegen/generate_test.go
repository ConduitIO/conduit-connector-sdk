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

package readmegen

import (
	"bytes"
	"os"
	"testing"

	"github.com/conduitio/yaml/v3"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestGenerate(t *testing.T) {
	is := is.New(t)

	specsRaw, err := os.ReadFile("./testdata/connector.yaml")
	is.NoErr(err)

	var data map[string]any
	err = yaml.Unmarshal(specsRaw, &data)
	is.NoErr(err)

	var buf bytes.Buffer
	opts := GenerateOptions{
		Data:       data,
		ReadmePath: "./testdata/test1.md",
		Out:        &buf,
	}

	err = Generate(opts)
	is.NoErr(err)

	wantBytes, err := os.ReadFile("./testdata/test1want.md")
	is.NoErr(err)
	want := string(wantBytes)
	is.Equal("", cmp.Diff(want, buf.String()))
}
