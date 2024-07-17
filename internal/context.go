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

package internal

import (
	"context"

	"github.com/conduitio/conduit-connector-protocol/pconduit"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
)

// -- Connector ID ----------------------------------------------------------

type connectorIDCtxKey struct{}

// ContextWithConnectorID wraps ctx and returns a context that contains the connector ID.
func ContextWithConnectorID(ctx context.Context, connectorID string) context.Context {
	return context.WithValue(ctx, connectorIDCtxKey{}, connectorID)
}

// ConnectorIDFromContext fetches the connector ID from the context. If the
// context does not contain a connector ID it returns an empty string.
func ConnectorIDFromContext(ctx context.Context) string {
	connectorID := ctx.Value(connectorIDCtxKey{})
	if connectorID != nil {
		return connectorID.(string) //nolint:forcetypeassert // only this package can set the value, it has to be a string
	}
	return ""
}

// -- Connector token ----------------------------------------------------------

type logLevelCtxKey struct{}

// ContextWithLogLevel wraps ctx and returns a context that contains the connector LogLevel.
func ContextWithLogLevel(ctx context.Context, logLevel string) context.Context {
	return context.WithValue(ctx, logLevelCtxKey{}, logLevel)
}

// LogLevelFromContext fetches the connector LogLevel from the context. If the
// context does not contain a connector LogLevel it returns an empty string.
func LogLevelFromContext(ctx context.Context) string {
	logLevel := ctx.Value(logLevelCtxKey{})
	if logLevel != nil {
		return logLevel.(string) //nolint:forcetypeassert // only this package can set the value, it has to be a string
	}

	return ""
}

func Enrich(ctx context.Context, cfg pconnector.PluginConfig) context.Context {
	// NB: The context is essentially a stack, so fetching a value from the context means
	// we have to pop values from the stack until we get to the one we are looking for.
	// We know that the token will be used in calls to connector utils, so it should be
	// on the top of the stack.
	// The connector ID is expected to be used more often than the log level.
	ctx = ContextWithLogLevel(ctx, cfg.LogLevel)
	ctx = ContextWithConnectorID(ctx, cfg.ConnectorID)
	ctx = pconduit.ContextWithConnectorToken(ctx, cfg.Token)

	return ctx
}
