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

package schema

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-protocol/pconnutils"
	"github.com/conduitio/conduit-connector-protocol/pconnutils/v1/client"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"google.golang.org/grpc"
)

func init() {
	internal.StandaloneConnectorUtilities = append(internal.StandaloneConnectorUtilities, standaloneInitializer{})
}

var (
	ErrSubjectNotFound = pconnutils.ErrSubjectNotFound
	ErrVersionNotFound = pconnutils.ErrVersionNotFound
	ErrInvalidSchema   = pconnutils.ErrInvalidSchema
)

// Service is the schema service client that can be used to interact with the schema service.
// It is initialized with an in-memory service by default.
var Service = newCachedSchemaService(newInMemoryService())

// Create creates a new schema with the given name and bytes. The schema type must be Avro.
func Create(ctx context.Context, typ schema.Type, subject string, bytes []byte) (schema.Schema, error) {
	resp, err := Service.CreateSchema(ctx, pconnutils.CreateSchemaRequest{
		Subject: qualifiedSubject(ctx, subject),
		Type:    typ,
		Bytes:   bytes,
	})
	if err != nil {
		return schema.Schema{}, err
	}

	return resp.Schema, nil
}

// qualifiedSubject returns a "fully qualified" schema subject name,
// i.e. prefixes the input subject name with the schema context name.
// We plan adding support for proper schema contexts: https://github.com/ConduitIO/conduit/issues/1721
func qualifiedSubject(ctx context.Context, subject string) string {
	schemaCtx := GetSchemaContextName(ctx)
	if schemaCtx == "" {
		return subject
	}

	return fmt.Sprintf("%s:%s", schemaCtx, subject)
}

// Get retrieves the schema with the given name and version. If the schema does not exist, an error is returned.
func Get(ctx context.Context, subject string, version int) (schema.Schema, error) {
	resp, err := Service.GetSchema(ctx, pconnutils.GetSchemaRequest{
		Subject: subject,
		Version: version,
	})
	if err != nil {
		return schema.Schema{}, err
	}

	return resp.Schema, nil
}

// -- Schema context utilities -------------------------------------------------

type schemaContextNameKey struct{}

// WithSchemaContextName wraps ctx and returns a context that contains the provided schema context name.
func WithSchemaContextName(ctx context.Context, schemaCtxName string) context.Context {
	if schemaCtxName == "" {
		return ctx
	}

	return context.WithValue(ctx, schemaContextNameKey{}, schemaCtxName)
}

// GetSchemaContextName fetches the schema context name from the context.
// If the context does not contain a schema context name it returns an empty string.
func GetSchemaContextName(ctx context.Context) string {
	name := ctx.Value(schemaContextNameKey{})
	if name != nil {
		return name.(string) //nolint:forcetypeassert // only this package can set the value, it has to be a string
	}

	return ""
}

// -- Schema service initializer -----------------------------------------------

type standaloneInitializer struct{}

// Init initializes the schema service client with the given gRPC connection.
func (standaloneInitializer) Init(conn *grpc.ClientConn) error {
	Service = newCachedSchemaService(client.NewSchemaServiceClient(conn))
	return nil
}
