// Copyright © 2022 Meroxa, Inc.
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

/*
Package sdk implements utilities for implementing a Conduit connector.

# Getting started

Conduit connectors can be thought of as the edges of a Conduit pipeline.
They are responsible for reading records from and writing records to third
party systems. Conduit uses connectors as plugins that hide the intricacies
of working with a particular third party system, so that Conduit itself can
focus on efficiently processing records and moving them safely from sources
to destinations.

To implement a connector, start by defining a global variable of type
[Connector], preferably in connector.go at the root of your project to make
it easy to discover.

	var Connector = sdk.Connector {
	    NewSpecification: Specification,  // Specification is my connector's specification
	    NewSource:        NewSource,      // NewSource is the constructor for my source
	    NewDestination:   NewDestination, // NewDestination is the constructor for my destination
	}

Connector will be used as the starting point for accessing three main
connector components that you need to provide:
  - [Specification] contains general information about the plugin like its
    name and what it does. Writing a specification is relatively simple and
    straightforward, for more info check the corresponding field docs of
    [Specification].
  - [Source] is the connector part that knows how to fetch data from the
    third party system and convert it to a [Record].
  - [Destination] is the connector part that knows how to write a [Record]
    to the third party system.

General advice for implementing connectors:
  - The SDK provides a structured logger that can be retrieved with
    [Logger]. It allows you to create structured and leveled output that
    will be included as part of the Conduit logs.
  - If you want to add logging to the hot path (i.e. code that is executed
    for every record that is read or written) you should use the log level
    "trace", otherwise it can greatly impact the performance of your
    connector.

# Source

A [Source] is responsible for continuously reading data from a third party
system and returning it in form of a [Record].

Every [Source] implementation needs to include an [UnimplementedSource] to
satisfy the interface. This allows us to potentially change the interface
in the future while remaining backwards compatible with existing [Source]
implementations.

	type Source struct {
	  sdk.UnimplementedSource
	}

You need to implement the functions required by [Source] and provide your
own implementations. Please look at the documentation of [Source] for
further information about individual functions.

You should also create a constructor function for your source struct.
Note that this is the same function that should be set as the value of
[Connector].NewSource. The constructor should be used to wrap your source in
the default middleware. You can add additional middleware, but unless you
have a very good reason, you should always include the default middleware.

	func NewSource() sdk.Source {
	  return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
	}

Additional tips for implementing a source:

  - The SDK provides utilities for certain operations like creating records
    in [SourceUtil]. You can access it through the global variable
    [Util].Source.
  - The function Source.Ack is optional and does not have to be implemented.
  - [Source] is responsible for creating record positions that should
    ideally uniquely identify a record. Think carefully about what you will
    store in the position, it should give the source enough information to
    resume reading records at that specific position.
  - The SDK provides acceptance tests, if your source doesn't pass it means
    your implementation has a bug¹.

# Destination

A [Destination] is responsible for writing [Record] to third party systems.

Every [Destination] implementation needs to include an
[UnimplementedDestination] to satisfy the interface. This allows us to
potentially change the interface in the future while remaining backwards
compatible with existing [Destination] implementations.

	type Destination struct {
	  sdk.UnimplementedSource
	}

You need to implement the functions required by [Destination] and provide
your own implementations. Please look at the documentation of [Destination]
for further information about individual functions.

You should also create a constructor function for your destination struct.
Note that this is the same function that should be set as the value of
[Connector].NewDestination. The constructor should be used to wrap your
destination in the default middleware. You can add additional middleware,
but unless you have a very good reason, you should always include the
default middleware.

	func NewDestination() sdk.Destination {
	  return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
	}

Additional tips for implementing a destination:

  - The SDK provides utilities for certain operations like routing records
    based on their operation in [DestinationUtil]. You can access it through
    the global variable [Util].Destination.
  - If your destination writes records as a whole to the destination you
    should use [Record].Bytes to get the raw record representation.
  - If possible, make your destination writes idempotent. It is possible
    that the destination will receive the same record twice after a pipeline
    restart.
  - Some sources won't be able to distinguish create and update operations.
    In case your destination is updating data in place, we recommend to
    upsert the record on a create or update.
  - The SDK provides acceptance tests, if your destination doesn't pass it
    means your implementation has a bug¹.

# Acceptance tests

The SDK provides acceptance tests that can be run in a simple Go test.¹

To run acceptance tests you should create a test file, preferably named
acceptance_test.go at the root of your project to make it easy to discover.
Inside create a Go test where you trigger the function [AcceptanceTest].

	func TestAcceptance(t *testing.T) {
	  // set up dependencies here
	  sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
	    Config: sdk.ConfigurableAcceptanceTestDriverConfig{
	      Connector: Connector, // Connector is the global variable from your connector
	      SourceConfig: map[string]string{ … },
	      DestinationConfig: map[string]string{ … },
	    },
	  }
	}

[AcceptanceTest] uses the [AcceptanceTestDriver] for certain operations. The
SDK already provides a default implementation for the driver with
[ConfigurableAcceptanceTestDriver], although you can supply your own
implementation if you need to adjust the behavior of acceptance tests for
your connector.

Some acceptance tests will try to write data using the destination and then
read the same data using the source. Because of that you need to make sure
that the configurations point both to the same exact data store (e.g. in
case of the file connector the source and destination need to read and write
to the same file).

If your connector does not implement both sides of the connector (a source
and a destination) you will need to write a custom driver that knows how to
read or write, depending on which side of the connector is not implemented.
Here is an example how to do that:

	type CustomAcceptanceTestDriver struct {
	  sdk.ConfigurableAcceptanceTestDriver
	}
	func (d *CustomAcceptanceTestDriver) ReadFromDestination(t *testing.T, records []opencdc.Record) []opencdc.Record {
	  // implement read
	}
	func (d *CustomAcceptanceTestDriver) WriteToSource(t *testing.T, records []opencdc.Record) []opencdc.Record {
	  // implement write
	}

For more information about what behavior can be customized please refer to
the [AcceptanceTestDriver] interface.

¹Acceptance tests are currently still experimental.
*/
package sdk
