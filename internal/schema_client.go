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
	"fmt"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-protocol/conduit/v1/fromproto"
	"github.com/conduitio/conduit-connector-protocol/conduit/v1/toproto"
	conduitv1 "github.com/conduitio/conduit-connector-protocol/proto/conduit/v1"
	"google.golang.org/grpc"
)

type SchemaClient struct {
	grpcClient conduitv1.SchemaServiceClient
}

func NewSchemaClient() (*SchemaClient, error) {
	conn, err := grpc.NewClient("localhost:8085")
	if err != nil {
		return nil, fmt.Errorf("failed creating gRPC client: %w", err)
	}

	return &SchemaClient{
		grpcClient: conduitv1.NewSchemaServiceClient(conn),
	}, nil
}

func (c *SchemaClient) Create(ctx context.Context, instance schema.Instance) (schema.Instance, error) {
	resp, err := c.grpcClient.Create(ctx, toproto.CreateSchemaRequest(instance))
	if err != nil {
		return schema.Instance{}, fmt.Errorf("failed creating schema: %w", err)
	}

	return fromproto.SchemaInstanceFromResponse(resp), nil
}
