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
	"sync"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-protocol/pconduit"
)

type inMemoryService struct {
	// schemas is a map of schema subjects to all the versions of that schema
	// versioning starts at 1, newer versions are appended to the end of the versions slice.
	schemas map[string][]schema.Schema
	// m guards access to schemas
	m sync.Mutex
}

func newInMemoryService() pconduit.SchemaService {
	return &inMemoryService{
		schemas: make(map[string][]schema.Schema),
	}
}

func (s *inMemoryService) CreateSchema(_ context.Context, request pconduit.CreateSchemaRequest) (pconduit.CreateSchemaResponse, error) {
	if request.Type != schema.TypeAvro {
		return pconduit.CreateSchemaResponse{}, ErrInvalidSchemaType
	}

	s.m.Lock()
	defer s.m.Unlock()

	inst := schema.Schema{
		Subject: request.Subject,
		Version: len(s.schemas[request.Subject]) + 1,
		Type:    request.Type,
		Bytes:   request.Bytes,
	}
	s.schemas[request.Subject] = append(s.schemas[request.Subject], inst)

	return pconduit.CreateSchemaResponse{Schema: inst}, nil
}

func (s *inMemoryService) GetSchema(_ context.Context, request pconduit.GetSchemaRequest) (pconduit.GetSchemaResponse, error) {
	s.m.Lock()
	defer s.m.Unlock()

	versions, ok := s.schemas[request.Subject]
	if !ok {
		return pconduit.GetSchemaResponse{}, fmt.Errorf("subject %v: %w", request.Subject, ErrSchemaNotFound)
	}

	if len(versions) < request.Version {
		return pconduit.GetSchemaResponse{}, fmt.Errorf("version %v: %w", request.Version, ErrSchemaNotFound)
	}

	return pconduit.GetSchemaResponse{Schema: versions[request.Version-1]}, nil
}
