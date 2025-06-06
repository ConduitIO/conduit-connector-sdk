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
	"strconv"

	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-protocol/pconnector/server"
	"github.com/conduitio/conduit-connector-protocol/pconnutils"
	v1 "github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen/model/v1"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/yaml/v3"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
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
	handleCommand(c, os.Args[1:])

	err := serve(c)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error running plugin: %+v", err)
		os.Exit(1)
	}
}

func handleCommand(c Connector, args []string) {
	if len(args) == 0 {
		return
	}

	switch args[0] {
	case "spec", "specs", "specification", "specifications":
		spec := v1.Specification{}.FromConfig(c.NewSpecification())
		out, err := yaml.Marshal(spec)
		if err != nil {
			panic(fmt.Errorf("failed to marshal specification: %w", err))
		}
		fmt.Println(string(out))
		os.Exit(0)
	case "version":
		fmt.Println(c.NewSpecification().Version)
		os.Exit(0)
	default:
		fmt.Println("unknown command:", args[0])
		os.Exit(1)
	}
}

func serve(c Connector) error {
	cfg, err := getPluginConfig()
	if err != nil {
		return fmt.Errorf("failed to get plugin configuration: %w", err)
	}

	initStandaloneModeLogger(connectorLogLevel(cfg))

	grpcCfg, err := getGRPCConfig()
	if err != nil {
		return fmt.Errorf("failed to get gRPC configuration: %w", err)
	}

	if err := internal.InitStandaloneConnectorUtilities(grpcCfg.ConnectorUtilitiesTarget); err != nil {
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

	spec := c.NewSpecification()

	return server.Serve(
		func() pconnector.SpecifierPlugin { return NewSpecifierPlugin(spec) },
		func() pconnector.SourcePlugin { return NewSourcePlugin(c.NewSource(), cfg, spec.SourceParams) },
		func() pconnector.DestinationPlugin {
			return NewDestinationPlugin(c.NewDestination(), cfg, spec.DestinationParams)
		},
		server.WithGRPCServerOptions(
			grpc.MaxRecvMsgSize(grpcCfg.MaxReceiveRecordSize),
		),
	)
}

func getPluginConfig() (pconnector.PluginConfig, error) {
	token := os.Getenv(pconnutils.EnvConduitConnectorToken)
	if token == "" {
		return pconnector.PluginConfig{}, missingEnvError(pconnutils.EnvConduitConnectorToken, "v0.11.0")
	}

	connectorID := os.Getenv(pconnector.EnvConduitConnectorID)
	if connectorID == "" {
		return pconnector.PluginConfig{}, missingEnvError(pconnector.EnvConduitConnectorID, "v0.11.0")
	}

	logLevel := os.Getenv(pconnector.EnvConduitConnectorLogLevel)

	return pconnector.PluginConfig{
		Token:       token,
		ConnectorID: connectorID,
		LogLevel:    logLevel,
	}, nil
}

func getGRPCConfig() (pconnector.GRPCConfig, error) {
	target, err := connectorUtilitiesGRPCTarget()
	if err != nil {
		return pconnector.GRPCConfig{}, err
	}

	maxReceiveRecordSize := connectorGRPCMaxReceiveRecordSize()

	return pconnector.GRPCConfig{
		ConnectorUtilitiesTarget: target,
		MaxReceiveRecordSize:     maxReceiveRecordSize,
	}, nil
}

// connectorUtilitiesGRPCTarget returns the address and token to be used for the connector utilities service.
// The values are fetched from environment variables provided by conduit-connector-protocol.
// The function returns an error if the environment variables are not specified or empty.
func connectorUtilitiesGRPCTarget() (string, error) {
	target := os.Getenv(pconnutils.EnvConduitConnectorUtilitiesGRPCTarget)
	if target == "" {
		return "", missingEnvError(pconnutils.EnvConduitConnectorUtilitiesGRPCTarget, "v0.11.0")
	}

	return target, nil
}

// connectorLogLevel returns the log level to be used by the connector. The value
// is fetched from the environment variable provided by conduit-connector-protocol.
// The function returns the TRACE level if the environment variable is not
// specified or empty (that's the zerolog default).
func connectorLogLevel(cfg pconnector.PluginConfig) zerolog.Level {
	level := cfg.LogLevel

	l, err := zerolog.ParseLevel(level)
	if err != nil {
		// Default to TRACE, logs will get filtered by Conduit.
		return zerolog.TraceLevel
	}

	return l
}

// connectorGRPCMaxReceiveRecordSize returns the maximum size of a record that can be received
// by the connector. The value is fetched from the environment variable provided by conduit-connector-protocol.
func connectorGRPCMaxReceiveRecordSize() int {
	maxReceiveRecordSize := os.Getenv(pconnector.EnvConduitConnectorMaxReceiveRecordSize)

	size, err := strconv.Atoi(maxReceiveRecordSize)
	if err != nil {
		return server.DefaultMaxReceiveRecordSize
	}

	return size
}

func missingEnvError(envVar, conduitVersion string) error {
	return fmt.Errorf(
		"%q is not set. This indicates you are using an old version of Conduit."+
			"Please, consider upgrading to at least %v. https://github.com/ConduitIO/conduit/releases/latest",
		envVar,
		conduitVersion,
	)
}
