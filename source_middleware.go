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
	"context"
	"fmt"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"strconv"
)

// SourceMiddleware wraps a Source and adds functionality to it.
type SourceMiddleware interface {
	Wrap(Source) Source
}

// DefaultSourceMiddleware returns a slice of middleware that should be added to
// all sources unless there's a good reason not to.
func DefaultSourceMiddleware() []SourceMiddleware {
	return []SourceMiddleware{
		&SourceWithSchemaContext{},
	}
}

// SourceWithMiddleware wraps the source into the supplied middleware.
func SourceWithMiddleware(d Source, middleware ...SourceMiddleware) Source {
	for _, m := range middleware {
		d = m.Wrap(d)
	}
	return d
}

type SourceWithSchemaContext struct {
	Source

	UseContext  bool
	ContextName string
}

func (s *SourceWithSchemaContext) Wrap(impl Source) Source {
	s.Source = impl

	return s
}

func (s *SourceWithSchemaContext) Parameters() config.Parameters {
	return mergeParameters(
		s.Source.Parameters(),
		config.Parameters{
			"sdk.schema.context.use": config.Parameter{
				Default:     "true",
				Description: "", // todo
				Type:        config.ParameterTypeBool,
			},
			"sdk.schema.context.name": config.Parameter{
				Default:     "",
				Description: "", // todo
				Type:        config.ParameterTypeString,
			},
		},
	)
}

func (s *SourceWithSchemaContext) Configure(ctx context.Context, cfg config.Config) error {
	s.UseContext = true
	if useStr, ok := cfg["sdk.schema.context.use"]; ok {
		use, err := strconv.ParseBool(useStr)
		if err != nil {
			return fmt.Errorf("could not parse `sdk.schema.context.use`, input %v: %w", useStr, err)
		}
		s.UseContext = use
	}

	if s.UseContext {
		s.ContextName = internal.ConnectorIDFromContext(ctx)
		if ctxName, ok := cfg["sdk.schema.context.name"]; ok {
			s.ContextName = ctxName
		}
	}

	return s.Source.Configure(schema.WithSchemaContextName(ctx, s.ContextName), cfg)
}

func (s *SourceWithSchemaContext) Open(ctx context.Context, pos opencdc.Position) error {
	return s.Source.Open(schema.WithSchemaContextName(ctx, s.ContextName), pos)
}

func (s *SourceWithSchemaContext) Read(ctx context.Context) (opencdc.Record, error) {
	return s.Source.Read(schema.WithSchemaContextName(ctx, s.ContextName))
}

func (s *SourceWithSchemaContext) Teardown(ctx context.Context) error {
	return s.Source.Teardown(schema.WithSchemaContextName(ctx, s.ContextName))
}

func (s *SourceWithSchemaContext) LifecycleOnCreated(ctx context.Context, config config.Config) error {
	return s.Source.LifecycleOnCreated(schema.WithSchemaContextName(ctx, s.ContextName), config)
}

func (s *SourceWithSchemaContext) LifecycleOnUpdated(ctx context.Context, configBefore, configAfter config.Config) error {
	return s.Source.LifecycleOnUpdated(schema.WithSchemaContextName(ctx, s.ContextName), configBefore, configAfter)
}

func (s *SourceWithSchemaContext) LifecycleOnDeleted(ctx context.Context, config config.Config) error {
	return s.Source.LifecycleOnDeleted(schema.WithSchemaContextName(ctx, s.ContextName), config)
}
