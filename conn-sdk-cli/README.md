# conn-sdk-cli

`conn-sdk-cli` is a CLI tool that helps with development of connectors. To
install the tool, run:

```shell
go install github.com/conduitio/conduit-connector-sdk/conn-sdk-cli@latest
```

For more information about using the available commands, run:

```shell
conn-sdk-cli help
```

or the following for a specific command:

```shell
conn-sdk-cli <command> --help
```

The following sections describe the most important commands in more details.

## readmegen

The `readmegen` command is used to generate sections in a README file that
document the connector. `readmegen` searches for certain tags in the README file
and replaces them with appropriate values. For example, if
`<!-- readmegen:name -->` is found in a README, then `readmegen` will replace it
with the connector name. All tags must have a corresponding <!-- /readmegen:(tagname) -->
closing tag.

Here's the list of supported tags.

* `<!-- readmegen:name -->` - The name of the connector (e.g., "postgres", "mysql").

* `<!-- readmegen:summary -->` - A brief overview of what the connector does and
  what service/system it connects to.

* `<!-- readmegen:description -->` - Detailed information about the connector's
  functionality, use cases, and any special features.

* `<!-- readmegen:version -->` - The version of the connector.

* `<!-- readmegen:author -->` - The name(s) of the connector's developer(s) or
  maintaining organization.

* `<!-- readmegen:source.parameters.yaml -->` - Configuration parameters for the
  connector when acting as a source (reading data), rendered in YAML format. The
  YAML code can be used in a pipeline.

* `<!-- readmegen:source.parameters.table -->` - Source configuration parameters
  formatted as a table.

* `<!-- readmegen:destination.parameters.yaml -->` - Configuration parameters
  for the connector when acting as a destination (writing data), rendered in
  YAML format. The YAML code can be used in a pipeline.

* `<!-- readmegen:destination.parameters.table -->` - Destination configuration
  parameters formatted as a table.

## specgen

The `specgen` command updates the source and destination parameters in the
`connector.yaml` by using the configuration structs from the code. 

It's usually run as part of `go generate`. It also needs access to the
`sdk.Connector` variable that holds references to the constructor functions for
the source and the destination, so it's best to place it in the `connector.go`
file. The following is an example from the Kafka connector:

```go
//go:generate conn-sdk-cli specgen

// Package kafka contains implementations for Kafka source and destination
// connectors for Conduit.
package kafka

import (
	_ "embed"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

//go:embed connector.yaml
var specs string

var version = "(devel)"

var Connector = sdk.Connector{
  NewSpecification: sdk.YAMLSpecification(specs, version),
  NewSource:        NewSource,
  NewDestination:   NewDestination,
}
```

If you run `conn-sdk-cli specgen` (directly, through `go generate ./...`, with
`make generate` that's provided by the connector template), you'll see that the
`connector.yaml` is updated with the source and/or destination parameters.
