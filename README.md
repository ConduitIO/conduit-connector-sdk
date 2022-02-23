# Conduit Connector Plugin SDK

[![License](https://img.shields.io/badge/license-Apache%202-blue)](https://github.com/ConduitIO/connector-plugin-sdk/blob/main/LICENSE.md)
[![Build](https://github.com/ConduitIO/connector-plugin-sdk/actions/workflows/build.yml/badge.svg)](https://github.com/ConduitIO/connector-plugin-sdk/actions/workflows/build.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/conduitio/connector-plugin-sdk)](https://goreportcard.com/report/github.com/conduitio/connector-plugin-sdk)
[![Go Reference](https://pkg.go.dev/badge/github.com/conduitio/connector-plugin-sdk.svg)](https://pkg.go.dev/github.com/conduitio/connector-plugin-sdk)

This repository contains the Go software development kit for implementing a connector plugin for
[Conduit](https://github.com/conduitio/conduit). If you want to implement a connector plugin in another language please
have a look at the [connector plugin protocol](https://github.com/conduitio/connector-plugin).

## Quickstart

Create a new folder and initialize a fresh go module:
```
go mod init example.com/conduit-plugin-demo
```

Add the connector plugin SDK dependency:

```
go get github.com/conduitio/connector-plugin-sdk
```

You need to create two structs, one that implements `sdk.Source` and another that implements `sdk.Destination`. Apart
from that, you need to create three constructor functions where each returns a `sdk.Source`, `sdk.Destination` and
`sdk.Specification` respectively.

The plugin also needs an entrypoint that only calls `sdk.Serve`.

```go
package main

import (
	demo "example.com/conduit-plugin-demo"
	sdk "github.com/conduitio/connector-plugin-sdk"
)

func main() {
	sdk.Serve(
		demo.Specification,  // func Specification() sdk.Specification { ... }
		demo.NewSource,      // func NewSource() sdk.Source { ... }
		demo.NewDestination, // func NewDestination() sdk.Destination { ... }
	)
}
```

## Examples

For examples of simple plugins you can look at existing plugins like
[conduit-plugin-generator](https://github.com/ConduitIO/conduit-plugin-generator) or
[conduit-plugin-file](https://github.com/ConduitIO/conduit-plugin-file).
