# Conduit Connector SDK

[![License](https://img.shields.io/badge/license-Apache%202-blue)](https://github.com/ConduitIO/conduit-connector-sdk/blob/main/LICENSE.md)
[![Build](https://github.com/ConduitIO/conduit-connector-sdk/actions/workflows/build.yml/badge.svg)](https://github.com/ConduitIO/conduit-connector-sdk/actions/workflows/build.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/conduitio/conduit-connector-sdk)](https://goreportcard.com/report/github.com/conduitio/conduit-connector-sdk)
[![Go Reference](https://pkg.go.dev/badge/github.com/conduitio/conduit-connector-sdk.svg)](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk)

This repository contains the Go software development kit for implementing a connector for
[Conduit](https://github.com/conduitio/conduit). If you want to implement a connector in another language please
have a look at the [connector protocol](https://github.com/conduitio/conduit-connector-protocol).

## Quickstart

Create a new folder and initialize a fresh go module:
```
go mod init example.com/conduit-connector-demo
```

Add the connector SDK dependency:

```
go get github.com/conduitio/conduit-connector-sdk
```

With this you can start implementing the connector. To implement a source (a connector that reads from a 3rd party
resource and sends data to Conduit) create a struct that implements
[`sdk.Source`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Source). To implement a destination (a
connector that receives data from Conduit and writes it to a 3rd party resource) create a struct that implements
[`sdk.Destination`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Destination). You can implement both to
make a connector that can be used both as a source or a destination.

Apart from the source and/or destination you should create constructor functions that return a `sdk.Source`,
`sdk.Destination` and `sdk.Specification` respectively.

The last part is the entrypoint, it needs to call `sdk.Serve` and pass in the constructor functions mentioned before. If
the connector does not implement a source or destination you should pass in `nil` instead.

```go
package main

import (
	demo "example.com/conduit-connector-demo"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(
		demo.Specification,  // func Specification() sdk.Specification { ... }
		demo.NewSource,      // func NewSource() sdk.Source { ... }
		demo.NewDestination, // func NewDestination() sdk.Destination { ... }
	)
}
```

Now you can build the standalone connector:

```
go build path/to/main.go
```

You will get a compiled binary which Conduit can use as a connector. To run your connector as part of a Conduit pipeline you
can create it using the connectors API and specify the path to the compiled connector binary in the field `plugin`.

Here is an example request to `POST /v1/connectors` (find more about the [Conduit API](https://github.com/conduitio/conduit#api)):

```json
{
  "type": "TYPE_SOURCE",
  "plugin": "/path/to/compiled/connector/binary",
  "pipelineId": "...",
  "config": {
    "name": "my-connector",
    "settings": {
      "my-key": "my-value"
    }
  }
}
```

## Examples

For examples of simple connectors you can look at existing connectors like
[conduit-connector-generator](https://github.com/ConduitIO/conduit-connector-generator) or
[conduit-connector-file](https://github.com/ConduitIO/conduit-connector-file).
