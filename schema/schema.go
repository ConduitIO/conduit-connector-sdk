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

var Service = NewInMemoryService()

func Create(ctx context.Context, typ schema.Type, name string, bytes []byte) (schema.Schema, error) {
	if typ != schema.TypeAvro {
		return schema.Schema{}, fmt.Errorf("type %v is not supported (only Avro is supported)", typ)
	}

	resp, err := Service.CreateSchema(ctx, pconduit.CreateSchemaRequest{
		Subject: name,
		Type:    typ,
		Bytes:   bytes,
	})
	if err != nil {
		return schema.Schema{}, err
	}

	return resp.Schema, nil
}

func Get(ctx context.Context, name string, version int) (schema.Schema, error) {
	resp, err := Service.GetSchema(ctx, pconduit.GetSchemaRequest{
		Subject: name,
		Version: version,
	})
	if err != nil {
		return schema.Schema{}, err
	}

	return resp.Schema, nil
}

type standaloneInitializer struct{}

func (standaloneInitializer) Init(conn *grpc.ClientConn) error {
	Service = client.NewSchemaServiceClient(conn)
	return nil
}
