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

package sdk

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-connector-protocol/conduit/schema"
	schemav1 "github.com/conduitio/conduit-connector-protocol/conduit/schema/v1"
	"google.golang.org/grpc"
)

type schemaServiceKey struct{}

func NewSchemaService(ctx context.Context) (schema.SchemaService, error) {
	service := ctx.Value(schemaServiceKey{})
	if service != nil {
		return service.(schema.SchemaService), nil
	}

	conn, err := grpc.NewClient("localhost:8085")
	if err != nil {
		return nil, fmt.Errorf("failed creating gRPC client: %w", err)
	}

	return schemav1.NewClient(conn)
}
