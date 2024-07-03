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

package sdk

import (
	"errors"
	"fmt"
	"os"

	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-protocol/pconnector/server"
	"github.com/conduitio/conduit-connector-sdk/internal"
)

// Serve starts the plugin and takes care of its whole lifecycle by blocking
// until the plugin can safely stop running. Any fixable errors will be output
// to os.Stderr and the process will exit with a status code of 1. Serve will
// panic for unexpected conditions where a user's fix is unknown.
//
// It is essential that nothing gets written to stdout or stderr before this
// function is called, as the first output is used to perform the initial
// handshake.
//
// Plugins should call Serve in their main() functions.
func Serve(c Connector) {
	err := serve(c)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error running plugin: %+v", err)
		os.Exit(1)
	}
}

func serve(c Connector) error {
	err := initStandaloneModeLogger()
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	target := os.Getenv("CONDUIT_STANDALONE_GRPC_TARGET")
	if target == "" {
		target = internal.DefaultStandaloneGRPCTarget
	}

	if err = internal.InitStandaloneConnectorGRPCClient(target); err != nil {
		return fmt.Errorf("failed to initialize standalone connector utilities: %w", err)
	}

	if c.NewSpecification == nil {
		return errors.New("Connector.NewSpecification is a required field")
	}
	if c.NewSource == nil {
		c.NewSource = func() Source { return nil }
	}
	if c.NewDestination == nil {
		c.NewDestination = func() Destination { return nil }
	}

	return server.Serve(
		func() pconnector.SpecifierPlugin {
			return NewSpecifierPlugin(c.NewSpecification(), c.NewSource(), c.NewDestination())
		},
		func() pconnector.SourcePlugin { return NewSourcePlugin(c.NewSource()) },
		func() pconnector.DestinationPlugin { return NewDestinationPlugin(c.NewDestination()) },
	)
}
