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
	"github.com/conduitio/conduit-connector-protocol/pconduit"
	"github.com/conduitio/conduit-connector-protocol/pconduit/v1/client"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"google.golang.org/grpc"
)

func init() {
	internal.StandaloneConnectorUtilities = append(internal.StandaloneConnectorUtilities, standaloneInitializer{})
}

// Service is the schema service client that can be used to interact with the schema service.
// It is initialized with an in-memory service by default.
var Service = newInMemoryService()

// Create creates a new schema with the given name and bytes. The schema type must be Avro.
func Create(ctx context.Context, typ schema.Type, subject string, bytes []byte) (schema.Schema, error) {
	resp, err := Service.CreateSchema(ctx, pconduit.CreateSchemaRequest{
		Subject: qualifiedSubject(ctx, subject),
		Type:    typ,
		Bytes:   bytes,
	})
	if err != nil {
		return schema.Schema{}, err
	}

	return resp.Schema, nil
}

func qualifiedSubject(ctx context.Context, subject string) string {
	// todo better delimiter
	schemaCtx := GetSchemaContextName(ctx)
	if schemaCtx == "" {
		return subject
	}

	return fmt.Sprintf("%s_%s", schemaCtx, subject)
}

// Get retrieves the schema with the given name and version. If the schema does not exist, an error is returned.
func Get(ctx context.Context, subject string, version int) (schema.Schema, error) {
	resp, err := Service.GetSchema(ctx, pconduit.GetSchemaRequest{
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

func WithSchemaContextName(ctx context.Context, schemaCtxName string) context.Context {
	return context.WithValue(ctx, schemaContextNameKey{}, schemaCtxName)
}

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
	Service = client.NewSchemaServiceClient(conn)
	return nil
}
