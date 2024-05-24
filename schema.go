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

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-protocol/conduit/v1/fromproto"
	conduitv1 "github.com/conduitio/conduit-connector-protocol/proto/conduit/v1"
	"google.golang.org/grpc"
)

type SchemaUtil struct {
	client conduitv1.SchemaServiceClient
}

func NewSchemaUtil() SchemaUtil {
	conn, err := grpc.NewClient("localhost:8085")
	if err != nil {
		panic(fmt.Errorf("failed connecting to schema service: %w", err))
	}

	return SchemaUtil{
		client: conduitv1.NewSchemaServiceClient(conn),
	}
}

func (u SchemaUtil) Create(ctx context.Context, name string, bytes []byte) (schema.Instance, error) {
	resp, err := u.client.Create(
		ctx,
		&conduitv1.CreateSchemaRequest{
			Name:  name,
			Type:  conduitv1.Schema_TYPE_AVRO,
			Bytes: bytes,
		},
	)
	if err != nil {
		return schema.Instance{}, err
	}

	return fromproto.SchemaInstance(resp.Schema), nil
}

func (u SchemaUtil) Get(ctx context.Context, id string) (schema.Instance, error) {
	resp, err := u.client.Get(ctx, &conduitv1.GetSchemaRequest{Id: id})
	if err != nil {
		return schema.Instance{}, err
	}

	return fromproto.SchemaInstance(resp.Schema), nil
}
