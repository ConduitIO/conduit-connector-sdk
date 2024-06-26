# Conduit Connector SDK

[![License](https://img.shields.io/badge/license-Apache%202-blue)](https://github.com/ConduitIO/conduit-connector-sdk/blob/main/LICENSE.md)
[![Test](https://github.com/ConduitIO/conduit-connector-sdk/actions/workflows/test.yml/badge.svg)](https://github.com/ConduitIO/conduit-connector-sdk/actions/workflows/test.yml)
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

Apart from the source and/or destination you should create a global variable of type
[`sdk.Connector`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Connector) that contains references to
constructors for [`sdk.Source`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Source),
[`sdk.Destination`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Destination) and
[`sdk.Specification`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Specification).

The last part is the entrypoint, it needs to call
[`sdk.Serve`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Serve) and pass in the connector
mentioned above.

```go
package main

import (
	demo "example.com/conduit-connector-demo"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(demo.Connector)
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

Find out more information on building a connector in the [Go doc reference](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk).

## FAQ

**Q: How to identify the source from which a record originated?**

A connector can use whatever means available to associate a record with the
originating source. However, to promote compatibility between connectors, we 
**highly recommend** that a record's metadata is used to indicate from which
_collection_[^1] a record originated.

The metadata key to be used is `opencdc.collection`, which can be accessed
through the `sdk.MetadataCollection` constant.

For example, if a record was read from a database table called `employees`, it
should have the following in its metadata:
```json5
{
  "opencdc.collection": "employees",
  // other metadata
}
```

Additionally, Conduit automatically adds the following metadata to each record:

* `conduit.source.plugin.name`: the source plugin that created a record
* `conduit.source.plugin.version`: version of the source plugin that created
  this record

More information about metadata in OpenCDC records can be found [here](https://conduit.io/docs/features/opencdc-record/#opencdc).

**Q: If a destination connector is able to write to multiple tables (topics,
collections, indexes, etc.), how should a record be routed to the correct
destination?**

Similarly to above, we recommend that the metadata key `"opencdc.collection"` is
used.

For example, if a record has the metadata field `"opencdc.collection"` set
to `employees`, then the PostgreSQL destination connector will write it to
the `employees` table.

**Q: Is there a standard format for errors?**

Conduit doesn't expect any specific error format. We still encourage developers to follow the conventional [error message
formatting](https://github.com/golang/go/wiki/CodeReviewComments#error-strings) and include enough contextual information to make
debugging as easy as possible (e.g. stack trace, information about the value that caused the error, internal state).

**Q: Is there a standard format for logging?**

Developers should use [`sdk.Logger`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Logger) to retrieve a
[`*zerolog.Logger`](https://pkg.go.dev/github.com/rs/zerolog#Logger) instance. It can be used to emit structured and leveled
log messages that will be included in Conduit logs.

Keep in mind that logging in the hot path (e.g. reading or writing a record) can have a negative impact on performance and should
be avoided. If you _really_ want to add a log message in the hot path please use the "trace" level.

**Q: How do I enable logging in my tests?**

By default, logging calls made using the `sdk.Logger` in your tests will not produce any output. To enable logging while running your
connector tests or debugging, you need to pass a custom context with a [zerolog](https://github.com/rs/zerolog) logger attached:

```go
func TestFoo(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	ctx := logger.WithContext(context.Background())

	// pass ctx to connector functions ...
}
```

**Q: Do I need to worry about ordering?**

In case of the destination connector you do not have to worry about ordering. Conduit will supply records one by one in the order
they were produced in the source.

On the other hand, the source connector is in charge of producing records and thus dictates the order. That said, you do not have
to worry about concurrent reads, the SDK will call [`Source.Read`](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk#Source)
repeatedly and only in one goroutine, all you have to do is return one record at a time.

## Examples

For examples of simple connectors you can look at existing connectors like
[conduit-connector-generator](https://github.com/ConduitIO/conduit-connector-generator) or
[conduit-connector-file](https://github.com/ConduitIO/conduit-connector-file).

[^1]: Collection is a generic term used in Conduit to describe an entity in a
3rd party system from which records are read from or to which records they are
written to. Examples are: topics (in Kafka), tables (in a database), indexes (in
a search engine), and collections (in NoSQL databases).
