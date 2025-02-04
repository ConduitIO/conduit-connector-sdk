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

	want := `# Test

Name: <!-- readmegen:name -->Test-Connector<!-- /readmegen:name -->
Summary: <!-- readmegen:summary -->test summary<!-- /readmegen:summary -->
Description: <!-- readmegen:description -->
Test description
should be able to handle new lines as well!
<!-- /readmegen:description -->
Version: <!-- readmegen:version -->v0.1.0<!-- /readmegen:version -->
Author: <!-- readmegen:author -->test author<!-- /readmegen:author -->

## Source Parameters

<!-- readmegen:source.parameters.yaml -->
` + "```yaml" + `
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "test-connector"
        settings:
          # string param1 description
          # Type: string
          param1: "foo"
          # int param2 description
          # Type: int
          param2: "0"
` + "```" + `
<!-- /readmegen:source.parameters.yaml -->

<!-- readmegen:source.parameters.table -->
<table class="no-margin-table">
  <tr>
    <th>Name</th>
    <th>Type</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
<td>

` + "`param1`" + `

</td>
<td>

string

</td>
<td>

` + "`foo`" + `

</td>
<td>

string param1 description

</td>
  </tr>
  <tr>
<td>

` + "`param2`" + `

</td>
<td>

int

</td>
<td>

` + "`0`" + `

</td>
<td>

int param2 description

</td>
  </tr>
</table>
<!-- /readmegen:source.parameters.table -->

## Destination Parameters

<!-- readmegen:destination.parameters.yaml -->
` + "```yaml" + `
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "test-connector"
        settings:
          # boolean param1 description
          # Type: boolean
          param1: "true"
          # float param2 description
          # Type: float
          param2: "0.0"
` + "```" + `
<!-- /readmegen:destination.parameters.yaml -->

<!-- readmegen:destination.parameters.table -->
<table class="no-margin-table">
  <tr>
    <th>Name</th>
    <th>Type</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
<td>

` + "`param1`" + `

</td>
<td>

boolean

</td>
<td>

` + "`true`" + `

</td>
<td>

boolean param1 description

</td>
  </tr>
  <tr>
<td>

` + "`param2`" + `

</td>
<td>

float

</td>
<td>

` + "`0.0`" + `

</td>
<td>

float param2 description

</td>
  </tr>
</table>
<!-- /readmegen:destination.parameters.table -->

`
	is.Equal("", cmp.Diff(want, buf.String()))
}
