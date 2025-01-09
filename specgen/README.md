# specgen

## Overview

`specgen` is a tool that generates connector specifications and writes them to a
`connector.yaml`. The input to `specgen` are source and destination
configuration structs returned by the `Config()` methods in the connectors.

`specgen` is run as part of `go generate`. It also needs access to the
`sdk.Connector` variable that holds references to constructor functions for the
source and the destination, so it's best to place it in the `connector.go` file.
The following is an example from the Kafka connector:

```go
//go:generate specgen

// Package kafka contains implementations for Kafka source and destination
// connectors for Conduit.
package kafka

import (
	_ "embed"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

//go:embed connector.yaml
var specs string

var Connector = sdk.Connector{
	NewSpecification: sdk.YAMLSpecification(specs),
	NewSource:        NewSource,
	NewDestination:   NewDestination,
}
```

`specgen` generates the specification in the following phases:

1. Extract the specifications from the source and destination configuration
   struct.
2. Combine the extracted specification with the existing one in `connector.yaml`.

More detailed information about `specgen` and `connector.yaml` can be found in
the [Conduit documentation](https://conduit.io/docs/developing/connectors/connector-specification).
