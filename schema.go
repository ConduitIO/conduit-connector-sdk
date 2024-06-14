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

	cschema "github.com/conduitio/conduit-commons/schema"
	pschema "github.com/conduitio/conduit-connector-protocol/conduit/schema"
	"github.com/conduitio/conduit-connector-protocol/conduit/schema/client"
)

type SchemaService interface {
	Create(ctx context.Context, name string, bytes []byte) (cschema.Instance, error)
	Get(ctx context.Context, name string, version int) (cschema.Instance, error)
}

// NewSchemaService creates a new SchemaService.
// If the connector is running in standalone mode, the SchemaService
// communicates with Conduit via gRPC.
// If the connector is running in built-in mode, then the SchemaService
// communicates with Conduit via method calls.
func NewSchemaService(ctx context.Context) (SchemaService, error) {
	target, err := client.New(ctx)
	if err != nil {
		return nil, err
	}

	return newSchemaServiceAdapter(target), nil
}

// schemaServiceAdapter adapts Conduit/connector protocol's schema.Service
// to the SDK SchemaService interface.
type schemaServiceAdapter struct {
	target pschema.Service
}

func newSchemaServiceAdapter(target pschema.Service) *schemaServiceAdapter {
	return &schemaServiceAdapter{target: target}
}

func (s *schemaServiceAdapter) Create(ctx context.Context, name string, bytes []byte) (cschema.Instance, error) {
	resp, err := s.target.Create(ctx, pschema.CreateRequest{
		Name:  name,
		Type:  pschema.TypeAvro,
		Bytes: bytes,
	})
	if err != nil {
		return cschema.Instance{}, err
	}

	return resp.Instance, nil
}

func (s *schemaServiceAdapter) Get(ctx context.Context, name string, version int) (cschema.Instance, error) {
	resp, err := s.target.Get(ctx, pschema.GetRequest{
		Name:    name,
		Version: version,
	})
	if err != nil {
		return cschema.Instance{}, err
	}

	return resp.Instance, nil
}
