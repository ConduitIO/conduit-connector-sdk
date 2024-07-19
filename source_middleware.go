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
	"sync"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/rabin"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-sdk/internal"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
)

// SourceMiddleware wraps a Source and adds functionality to it.
type SourceMiddleware interface {
	Wrap(Source) Source
}

// SourceMiddlewareOption can be used to change the behavior of the default source
// middleware created with DefaultSourceMiddleware.
type SourceMiddlewareOption func(SourceMiddleware)

// DefaultSourceMiddleware returns a slice of middleware that should be added to
// all sources unless there's a good reason not to.
func DefaultSourceMiddleware(opts ...SourceMiddlewareOption) []SourceMiddleware {
	middleware := []SourceMiddleware{
		&SourceWithSchema{},
	}
	// apply options to all middleware
	for _, m := range middleware {
		for _, opt := range opts {
			opt(m)
		}
	}
	return middleware
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
	configSourceSchemaType           = "sdk.schema.type"
	configSourceSchemaPayloadEncode  = "sdk.schema.payload.encode"
	configSourceSchemaPayloadSubject = "sdk.schema.payload.subject"
	configSourceSchemaKeyEncode      = "sdk.schema.key.encode"
	configSourceSchemaKeySubject     = "sdk.schema.key.subject"
)

// WithSourceWithSchemaConfig returns a SourceMiddlewareOption that sets the
// default configuration for the SourceWithSchema middleware.
func WithSourceWithSchemaConfig(cfg SourceWithSchemaConfig) SourceMiddlewareOption {
	return func(m SourceMiddleware) {
		if s, ok := m.(*SourceWithSchema); ok {
			s.Config = cfg
		}
	}
}

// SourceWithSchemaConfig is the configuration for the SourceWithSchema middleware.
type SourceWithSchemaConfig struct {
	// The type of the payload schema.
	SchemaType schema.Type
	// Whether to extract and encode the record payload with a schema.
	PayloadEncode *bool
	// The subject of the payload schema. Defaults to the connector ID with a "-payload" postfix.
	PayloadSubject *string
	// Whether to extract and encode the record key with a schema.
	KeyEncode *bool
	// The subject of the key schema. Defaults to the connector ID with a "-key" postfix.
	KeySubject *string
}

func (s SourceWithSchemaConfig) SchemaTypeParameterName() string {
	return configSourceSchemaType
}

func (s SourceWithSchemaConfig) SchemaPayloadEncodeParameterName() string {
	return configSourceSchemaPayloadEncode
}

func (s SourceWithSchemaConfig) SchemaPayloadSubjectParameterName() string {
	return configSourceSchemaPayloadSubject
}

func (s SourceWithSchemaConfig) SchemaKeyEncodeParameterName() string {
	return configSourceSchemaKeyEncode
}

func (s SourceWithSchemaConfig) SchemaKeySubjectParameterName() string {
	return configSourceSchemaKeySubject
}

func (s SourceWithSchemaConfig) parameters() config.Parameters {
	return config.Parameters{
		configSourceSchemaType: {
			Default:     s.SchemaType.String(),
			Type:        config.ParameterTypeString,
			Description: "The type of the payload schema.",
			Validations: []config.Validation{
				config.ValidationInclusion{List: s.types()},
			},
		},
		configSourceSchemaPayloadEncode: {
			Default:     strconv.FormatBool(*s.PayloadEncode),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and encode the record payload with a schema.",
		},
		configSourceSchemaPayloadSubject: {
			Default: func() string {
				if s.PayloadSubject != nil {
					return *s.PayloadSubject
				}
				return ""
			}(),
			Type: config.ParameterTypeString,
			Description: func() string {
				desc := "The subject of the payload schema."
				if s.PayloadSubject == nil {
					desc += ` Defaults to the connector ID with a "-payload" postfix.`
				}
				return desc
			}(),
		},
		configSourceSchemaKeyEncode: {
			Default:     strconv.FormatBool(*s.KeyEncode),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and encode the record key with a schema.",
		},
		configSourceSchemaKeySubject: {
			Default: func() string {
				if s.KeySubject != nil {
					return *s.KeySubject
				}
				return ""
			}(),
			Type: config.ParameterTypeString,
			Description: func() string {
				desc := "The subject of the payload schema."
				if s.KeySubject == nil {
					desc += ` Defaults to the connector ID with a "-key" postfix.`
				}
				return desc
			}(),
		},
	}
}

func (s SourceWithSchemaConfig) types() []string {
	out := make([]string, 0, len(schema.KnownSerdeFactories))
	for t := range schema.KnownSerdeFactories {
		out = append(out, t.String())
	}
	return out
}

// SourceWithSchema is a middleware that extracts and encodes the record payload
// and key with a schema. The schema is extracted from the record data for each
// record produced by the source. The schema is registered with the schema
// service and the schema subject is attached to the record metadata.
type SourceWithSchema struct {
	Config SourceWithSchemaConfig
}

// Wrap a Source into the schema middleware.
func (s *SourceWithSchema) Wrap(impl Source) Source {
	if s.Config.SchemaType == 0 {
		s.Config.SchemaType = schema.TypeAvro
	}

	if s.Config.KeyEncode == nil {
		t := true
		s.Config.KeyEncode = &t
	}
	if s.Config.PayloadEncode == nil {
		t := true
		s.Config.PayloadEncode = &t
	}

	return &sourceWithSchema{
		Source:           impl,
		config:           s.Config,
		fingerprintCache: make(map[uint64]schema.Schema),
	}
}

// sourceWithSchema is the actual middleware implementation.
type sourceWithSchema struct {
	Source
	config SourceWithSchemaConfig

	schemaType     schema.Type
	payloadSubject string
	keySubject     string

	fingerprintCache map[uint64]schema.Schema
	payloadWarnOnce  sync.Once
	keyWarnOnce      sync.Once
}

func (s *sourceWithSchema) Parameters() config.Parameters {
	return mergeParameters(s.Source.Parameters(), s.config.parameters())
}

func (s *sourceWithSchema) Configure(ctx context.Context, config config.Config) error {
	err := s.Source.Configure(ctx, config)
	if err != nil {
		return err
	}

	connectorID := internal.ConnectorIDFromContext(ctx)

	s.schemaType = s.config.SchemaType
	if val, ok := config[configSourceSchemaType]; ok {
		if err := s.schemaType.UnmarshalText([]byte(val)); err != nil {
			return fmt.Errorf("invalid %s: failed to parse schema type: %w", configSourceSchemaType, err)
		}
	}

	encodeKey := *s.config.KeyEncode
	if val, ok := config[configSourceSchemaKeyEncode]; ok {
		encodeKey, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configSourceSchemaKeyEncode, err)
		}
	}
	if encodeKey {
		// TODO: when adding schema context support, set DefaultKeySubject
		//  to "key" and let the schema service attach the prefix / context.
		s.keySubject = connectorID + "-key"
		if s.config.KeySubject != nil {
			s.keySubject = *s.config.KeySubject
		}
		if val, ok := config[configSourceSchemaKeySubject]; ok {
			s.keySubject = val
		}
	}

	encodePayload := *s.config.PayloadEncode
	if val, ok := config[configSourceSchemaPayloadEncode]; ok {
		encodePayload, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configSourceSchemaPayloadEncode, err)
		}
	}
	if encodePayload {
		// TODO: when adding schema context support, set DefaultPayloadSubject
		//  to "payload" and let the schema service attach the prefix / context.
		s.payloadSubject = connectorID + "-payload"
		if s.config.PayloadSubject != nil {
			s.payloadSubject = *s.config.PayloadSubject
		}
		if val, ok := config[configSourceSchemaPayloadSubject]; ok {
			s.payloadSubject = val
		}
	}

	return nil
}

func (s *sourceWithSchema) Read(ctx context.Context) (opencdc.Record, error) {
	rec, err := s.Source.Read(ctx)
	if err != nil || (s.keySubject == "" && s.payloadSubject == "") {
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
	if s.keySubject == "" {
		return nil // key schema encoding is disabled
	}
	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		s.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msg(`record key is not structured, consider disabling the source schema key encoding using "sdk.schema.key.encode: false"`)
		})
		return nil
	}

	sch, err := s.schemaForType(ctx, rec.Key, s.keySubject)
	if err != nil {
		return fmt.Errorf("failed to extract schema for key: %w", err)
	}
	encoded, err := s.encodeWithSchema(sch, rec.Key)
	if err != nil {
		return fmt.Errorf("failed to encode key: %w", err)
	}

	rec.Key = opencdc.RawData(encoded)
	if rec.Metadata == nil {
		rec.Metadata = opencdc.Metadata{}
	}
	schema.AttachKeySchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithSchema) encodePayload(ctx context.Context, rec *opencdc.Record) error {
	if s.payloadSubject == "" {
		return nil // payload schema encoding is disabled
	}
	_, beforeIsStructured := rec.Payload.Before.(opencdc.StructuredData)
	_, afterIsStructured := rec.Payload.After.(opencdc.StructuredData)
	if !beforeIsStructured && !afterIsStructured {
		// log warning once, to avoid spamming the logs
		s.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msg(`record payload is not structured, consider disabling the source schema payload encoding using "sdk.schema.payload.encode: false"`)
		})
		return nil
	}

	val := rec.Payload.After
	if !afterIsStructured {
		// use before as a fallback
		val = rec.Payload.Before
	}

	sch, err := s.schemaForType(ctx, val, s.payloadSubject)
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
	if rec.Metadata == nil {
		rec.Metadata = opencdc.Metadata{}
	}
	schema.AttachPayloadSchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithSchema) schemaForType(ctx context.Context, data any, subject string) (schema.Schema, error) {
	srd, err := schema.KnownSerdeFactories[s.schemaType].SerdeForType(data)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema for value: %w", err)
	}
	schemaBytes := []byte(srd.String())
	fp := rabin.Bytes(schemaBytes)

	sch, ok := s.fingerprintCache[fp]
	if !ok {
		sch, err = sdkschema.Create(ctx, s.schemaType, subject, schemaBytes)
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
