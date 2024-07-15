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
	"strconv"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/rabin"
	"github.com/conduitio/conduit-commons/schema"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
)

// SourceMiddleware wraps a Source and adds functionality to it.
type SourceMiddleware interface {
	Wrap(Source) Source
}

// DefaultSourceMiddleware returns a slice of middleware that should be added to
// all sources unless there's a good reason not to.
func DefaultSourceMiddleware() []SourceMiddleware {
	return []SourceMiddleware{
		SourceWithSchema{
			DefaultSchemaFormat:  schema.TypeAvro.String(),
			DefaultEncodePayload: true,
			DefaultEncodeKey:     true,
		},
	}
}

// SourceWithMiddleware wraps the source into the supplied middleware.
func SourceWithMiddleware(d Source, middleware ...SourceMiddleware) Source {
	for _, m := range middleware {
		d = m.Wrap(d)
	}
	return d
}

// -- SourceWithSchema ----------------------------------------------

const (
	configSourceSchemaFormat        = "sdk.schema.format"
	configSourceSchemaEncodePayload = "sdk.schema.encodePayload"
	configSourceSchemaEncodeKey     = "sdk.schema.encodeKey"
)

// SourceWithSchema TODO
type SourceWithSchema struct {
	// DefaultSchemaFormat is the default schema format.
	DefaultSchemaFormat string
	// DefaultEncodePayload is the default value for the encode payload option.
	DefaultEncodePayload bool
	// DefaultEncodeKey is the default value for the encode key option.
	DefaultEncodeKey bool
}

func (s SourceWithSchema) SchemaFormatParameterName() string {
	return configSourceSchemaFormat
}

// Wrap a Source into the schema middleware.
func (s SourceWithSchema) Wrap(impl Source) Source {
	return &sourceWithSchema{
		Source:   impl,
		defaults: s,
	}
}

type sourceWithSchema struct {
	Source
	defaults SourceWithSchema

	payloadSchemaType *schema.Type
	keySchemaType     *schema.Type
	fingerprintCache  map[uint64]schema.Schema
}

func (s *sourceWithSchema) formats() []string {
	out := make([]string, 0, len(schema.KnownSerdeFactories))
	for t := range schema.KnownSerdeFactories {
		out = append(out, t.String())
	}
	return out
}

func (s *sourceWithSchema) Parameters() config.Parameters {
	return mergeParameters(s.Source.Parameters(), config.Parameters{
		configSourceSchemaFormat: {
			Default:     s.defaults.DefaultSchemaFormat,
			Description: "The format of the payload schema.",
			Validations: []config.Validation{
				config.ValidationInclusion{List: s.formats()},
			},
		},
		configSourceSchemaEncodePayload: {
			Default:     strconv.FormatBool(s.defaults.DefaultEncodePayload),
			Description: "Whether to extract and encode the record payload with a schema.",
		},
		configSourceSchemaEncodeKey: {
			Default:     strconv.FormatBool(s.defaults.DefaultEncodeKey),
			Description: "Whether to extract and encode the record key with a schema.",
		},
	})
}

func (s *sourceWithSchema) Configure(ctx context.Context, config config.Config) error {
	err := s.Source.Configure(ctx, config)
	if err != nil {
		return err
	}

	// TODO parse config
	return nil
}

func (s *sourceWithSchema) Read(ctx context.Context) (opencdc.Record, error) {
	rec, err := s.Source.Read(ctx)
	if err != nil || (s.keySchemaType == nil && s.payloadSchemaType == nil) {
		return rec, err
	}

	if err := s.encodeKey(ctx, &rec); err != nil {
		return rec, err
	}
	if err := s.encodePayload(ctx, &rec); err != nil {
		return rec, err
	}

	return rec, nil
}

func (s *sourceWithSchema) encodeKey(ctx context.Context, rec *opencdc.Record) error {
	if s.keySchemaType == nil {
		return nil // key schema encoding is disabled
	}
	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// TODO log warning
		return nil
	}

	sch, err := s.schemaForType(ctx, *s.keySchemaType, rec.Key)
	if err != nil {
		return fmt.Errorf("failed to extract schema for key: %w", err)
	}
	encoded, err := s.encodeWithSchema(sch, rec.Key)
	if err != nil {
		return fmt.Errorf("failed to encode key: %w", err)
	}

	rec.Key = opencdc.RawData(encoded)
	schema.AttachKeySchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithSchema) encodePayload(ctx context.Context, rec *opencdc.Record) error {
	if s.payloadSchemaType == nil {
		return nil // payload schema encoding is disabled
	}
	_, beforeIsStructured := rec.Payload.Before.(opencdc.StructuredData)
	_, afterIsStructured := rec.Payload.After.(opencdc.StructuredData)
	if !beforeIsStructured && !afterIsStructured {
		// TODO log warning
		return nil
	}

	val := rec.Payload.After
	if !afterIsStructured {
		// use before as a fallback
		val = rec.Payload.Before
	}

	sch, err := s.schemaForType(ctx, *s.payloadSchemaType, val)
	if err != nil {
		return fmt.Errorf("failed to extract schema for payload: %w", err)
	}

	// encode both before and after with the extracted schema
	if beforeIsStructured {
		encoded, err := s.encodeWithSchema(sch, rec.Payload.Before)
		if err != nil {
			return fmt.Errorf("failed to encode before payload: %w", err)
		}
		rec.Payload.Before = opencdc.RawData(encoded)
	}
	if afterIsStructured {
		encoded, err := s.encodeWithSchema(sch, rec.Payload.After)
		if err != nil {
			return fmt.Errorf("failed to encode after payload: %w", err)
		}
		rec.Payload.After = opencdc.RawData(encoded)
	}
	schema.AttachKeySchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithSchema) schemaForType(ctx context.Context, t schema.Type, data any) (schema.Schema, error) {
	srd, err := schema.KnownSerdeFactories[t].SerdeForType(data)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema for value: %w", err)
	}
	schemaBytes := []byte(srd.String())
	fp := rabin.Bytes(schemaBytes)

	sch, ok := s.fingerprintCache[fp]
	if !ok {
		sch, err = sdkschema.Create(ctx, t, "TODO", schemaBytes)
		if err != nil {
			return schema.Schema{}, fmt.Errorf("failed to create schema: %w", err)
		}
		s.fingerprintCache[sch.Fingerprint()] = sch
	}

	return sch, nil
}

func (s *sourceWithSchema) encodeWithSchema(sch schema.Schema, data any) ([]byte, error) {
	srd, err := sch.Serde()
	if err != nil {
		return nil, fmt.Errorf("failed to get serde for schema: %w", err)
	}

	encoded, err := srd.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data with schema: %w", err)
	}

	return encoded, nil
}
