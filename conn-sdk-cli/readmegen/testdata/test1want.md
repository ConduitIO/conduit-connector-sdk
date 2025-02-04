# Test

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
```yaml
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
          # Required: yes
          param1: "foo"
          # int param2 description
          # Type: int
          # Required: no
          param2: "0"
```
<!-- /readmegen:source.parameters.yaml -->

<!-- readmegen:source.parameters.table -->
<table class="no-margin-table">
  <tr>
    <th>Name</th>
    <th>Type</th>
    <th>Required</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
<td>

`param1`

</td>
<td>

string

</td>
<td>
✔
</td>
<td>

`foo`

</td>
<td>

string param1 description

</td>
  </tr>
  <tr>
<td>

`param2`

</td>
<td>

int

</td>
<td>



</td>
<td>

`0`

</td>
<td>

int param2 description

</td>
  </tr>
</table>
<!-- /readmegen:source.parameters.table -->

## Destination Parameters

<!-- readmegen:destination.parameters.yaml -->
```yaml
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
          # Required: no
          param1: "true"
          # float param2 description
          # Type: float
          # Required: no
          param2: "0.0"
```
<!-- /readmegen:destination.parameters.yaml -->

<!-- readmegen:destination.parameters.table -->
<table class="no-margin-table">
  <tr>
    <th>Name</th>
    <th>Type</th>
    <th>Required</th>
    <th>Default</th>
    <th>Description</th>
  </tr>
  <tr>
<td>

`param1`

</td>
<td>

boolean

</td>
<td>

</td>
<td>

`true`

</td>
<td>

boolean param1 description

</td>
  </tr>
  <tr>
<td>

`param2`

</td>
<td>

float

</td>
<td>



</td>
<td>

`0.0`

</td>
<td>

float param2 description

</td>
  </tr>
</table>
<!-- /readmegen:destination.parameters.table -->

