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
		&SourceWithSchemaExtraction{},
		&SourceWithSchemaContext{},
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
func SourceWithMiddleware(s Source, middleware ...SourceMiddleware) Source {
	for _, m := range middleware {
		s = m.Wrap(s)
	}
	return s
}

// -- SourceWithSchemaExtraction ----------------------------------------------

const (
	configSourceSchemaExtractionType           = "sdk.schemaExtraction.type"
	configSourceSchemaExtractionPayloadEncode  = "sdk.schemaExtraction.payload.encode"
	configSourceSchemaExtractionPayloadSubject = "sdk.schemaExtraction.payload.subject"
	configSourceSchemaExtractionKeyEncode      = "sdk.schemaExtraction.key.encode"
	configSourceSchemaExtractionKeySubject     = "sdk.schemaExtraction.key.subject"
)

// SourceWithSchemaExtractionConfig is the configuration for the
// SourceWithSchemaExtraction middleware. Fields set to their zero value are
// ignored and will be set to the default value.
type SourceWithSchemaExtractionConfig struct {
	// The type of the payload schema. Defaults to Avro.
	SchemaType schema.Type
	// Whether to extract and encode the record payload with a schema.
	// If unset, defaults to true.
	PayloadEncode *bool
	// The subject of the payload schema. Defaults to the connector ID with a ".payload" postfix.
	PayloadSubject *string
	// Whether to extract and encode the record key with a schema.
	// If unset, defaults to true.
	KeyEncode *bool
	// The subject of the key schema. Defaults to the connector ID with a ".key" postfix.
	KeySubject *string
}

// Apply sets the default configuration for the SourceWithSchemaExtraction middleware.
// Apply can be used as a SourceMiddlewareOption.
func (c SourceWithSchemaExtractionConfig) Apply(m SourceMiddleware) {
	if s, ok := m.(*SourceWithSchemaExtraction); ok {
		s.Config = c
	}
}

func (c SourceWithSchemaExtractionConfig) SchemaTypeParameterName() string {
	return configSourceSchemaExtractionType
}

func (c SourceWithSchemaExtractionConfig) SchemaPayloadEncodeParameterName() string {
	return configSourceSchemaExtractionPayloadEncode
}

func (c SourceWithSchemaExtractionConfig) SchemaPayloadSubjectParameterName() string {
	return configSourceSchemaExtractionPayloadSubject
}

func (c SourceWithSchemaExtractionConfig) SchemaKeyEncodeParameterName() string {
	return configSourceSchemaExtractionKeyEncode
}

func (c SourceWithSchemaExtractionConfig) SchemaKeySubjectParameterName() string {
	return configSourceSchemaExtractionKeySubject
}

func (c SourceWithSchemaExtractionConfig) parameters() config.Parameters {
	return config.Parameters{
		configSourceSchemaExtractionType: {
			Default:     c.SchemaType.String(),
			Type:        config.ParameterTypeString,
			Description: "The type of the payload schema.",
			Validations: []config.Validation{
				config.ValidationInclusion{List: c.types()},
			},
		},
		configSourceSchemaExtractionPayloadEncode: {
			Default:     strconv.FormatBool(*c.PayloadEncode),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and encode the record payload with a schema.",
		},
		configSourceSchemaExtractionPayloadSubject: {
			Default: func() string {
				if c.PayloadSubject != nil {
					return *c.PayloadSubject
				}
				return ""
			}(),
			Type: config.ParameterTypeString,
			Description: func() string {
				desc := "The subject of the payload schema."
				if c.PayloadSubject == nil {
					desc += ` Defaults to the connector ID with a ".payload" postfix.`
				}
				return desc
			}(),
		},
		configSourceSchemaExtractionKeyEncode: {
			Default:     strconv.FormatBool(*c.KeyEncode),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and encode the record key with a schema.",
		},
		configSourceSchemaExtractionKeySubject: {
			Default: func() string {
				if c.KeySubject != nil {
					return *c.KeySubject
				}
				return ""
			}(),
			Type: config.ParameterTypeString,
			Description: func() string {
				desc := "The subject of the payload schema."
				if c.KeySubject == nil {
					desc += ` Defaults to the connector ID with a ".key" postfix.`
				}
				return desc
			}(),
		},
	}
}

func (c SourceWithSchemaExtractionConfig) types() []string {
	out := make([]string, 0, len(schema.KnownSerdeFactories))
	for t := range schema.KnownSerdeFactories {
		out = append(out, t.String())
	}
	return out
}

// SourceWithSchemaExtraction is a middleware that extracts and encodes the record
// payload and key with a schema. The schema is extracted from the record data
// for each record produced by the source. The schema is registered with the
// schema service and the schema subject is attached to the record metadata.
type SourceWithSchemaExtraction struct {
	Config SourceWithSchemaExtractionConfig
}

// Wrap a Source into the schema middleware. It will apply default configuration
// values if they are not explicitly set.
func (s *SourceWithSchemaExtraction) Wrap(impl Source) Source {
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

	return &sourceWithSchemaExtraction{
		Source:           impl,
		config:           s.Config,
		fingerprintCache: make(map[uint64]schema.Schema),
	}
}

// sourceWithSchemaExtraction is the actual middleware implementation.
type sourceWithSchemaExtraction struct {
	Source
	config SourceWithSchemaExtractionConfig

	schemaType     schema.Type
	payloadSubject string
	keySubject     string

	fingerprintCache map[uint64]schema.Schema
	payloadWarnOnce  sync.Once
	keyWarnOnce      sync.Once
}

func (s *sourceWithSchemaExtraction) Parameters() config.Parameters {
	return mergeParameters(s.Source.Parameters(), s.config.parameters())
}

func (s *sourceWithSchemaExtraction) Configure(ctx context.Context, config config.Config) error {
	err := s.Source.Configure(ctx, config)
	if err != nil {
		return err
	}

	connectorID := internal.ConnectorIDFromContext(ctx)

	s.schemaType = s.config.SchemaType
	if val, ok := config[configSourceSchemaExtractionType]; ok {
		if err := s.schemaType.UnmarshalText([]byte(val)); err != nil {
			return fmt.Errorf("invalid %s: failed to parse schema type: %w", configSourceSchemaExtractionType, err)
		}
	}

	encodeKey := *s.config.KeyEncode
	if val, ok := config[configSourceSchemaExtractionKeyEncode]; ok {
		encodeKey, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configSourceSchemaExtractionKeyEncode, err)
		}
	}
	if encodeKey {
		s.keySubject = connectorID + ".key"
		if s.config.KeySubject != nil {
			s.keySubject = *s.config.KeySubject
		}
		if val, ok := config[configSourceSchemaExtractionKeySubject]; ok {
			s.keySubject = val
		}
	}

	encodePayload := *s.config.PayloadEncode
	if val, ok := config[configSourceSchemaExtractionPayloadEncode]; ok {
		encodePayload, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configSourceSchemaExtractionPayloadEncode, err)
		}
	}
	if encodePayload {
		// TODO: when adding schema context support, set DefaultPayloadSubject
		//  to "payload" and let the schema service attach the prefix / context.
		s.payloadSubject = connectorID + ".payload"
		if s.config.PayloadSubject != nil {
			s.payloadSubject = *s.config.PayloadSubject
		}
		if val, ok := config[configSourceSchemaExtractionPayloadSubject]; ok {
			s.payloadSubject = val
		}
	}

	return nil
}

func (s *sourceWithSchemaExtraction) Read(ctx context.Context) (opencdc.Record, error) {
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

func (s *sourceWithSchemaExtraction) encodeKey(ctx context.Context, rec *opencdc.Record) error {
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

func (s *sourceWithSchemaExtraction) encodePayload(ctx context.Context, rec *opencdc.Record) error {
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

func (s *sourceWithSchemaExtraction) schemaForType(ctx context.Context, data any, subject string) (schema.Schema, error) {
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

func (s *sourceWithSchemaExtraction) encodeWithSchema(sch schema.Schema, data any) ([]byte, error) {
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

// -- SourceWithSchemaContext --------------------------------------------------

type SourceWithSchemaContextConfig struct {
	UseContext  *bool
	ContextName *string
}

// Apply sets the default configuration for the SourceWithSchemaExtraction middleware.
// Apply can be used as a SourceMiddlewareOption.
func (c SourceWithSchemaContextConfig) Apply(m SourceMiddleware) {
	if s, ok := m.(*SourceWithSchemaContext); ok {
		s.Config = c
	}
}

func (c SourceWithSchemaContextConfig) parameters() config.Parameters {
	return config.Parameters{
		c.UseContextParameterName(): config.Parameter{
			Default: strconv.FormatBool(*c.UseContext),
			Description: "Specifies whether to use a schema context name. If set to false, no schema context name will " +
				"be used, and schemas will be saved with the subject name specified in the connector " +
				"(not safe because of name conflicts).",
			Type: config.ParameterTypeBool,
		},
		c.ContextNameParameterName(): config.Parameter{
			Default: func() string {
				if c.ContextName == nil {
					return ""
				}

				return *c.ContextName
			}(),
			Description: func() string {
				d := "Schema context name to be used. Used as a prefix for all schema subject names."
				if c.ContextName == nil {
					d += " Defaults to the connector ID."
				}
				return d
			}(),
			Type: config.ParameterTypeString,
		},
	}
}

func (c SourceWithSchemaContextConfig) UseContextParameterName() string {
	return "sdk.schema.context.use"
}

func (c SourceWithSchemaContextConfig) ContextNameParameterName() string {
	return "sdk.schema.context.name"
}

// SourceWithSchemaContext is a middleware that makes it possible to configure
// the schema context for records read by a source.
type SourceWithSchemaContext struct {
	Config SourceWithSchemaContextConfig
}

// Wrap a Source into the schema middleware. It will apply default configuration
// values if they are not explicitly set.
func (s *SourceWithSchemaContext) Wrap(impl Source) Source {
	if s.Config.UseContext == nil {
		t := true
		s.Config.UseContext = &t
	}

	return &sourceWithSchemaContext{
		Source: impl,
		mwCfg:  s.Config,
	}
}

type sourceWithSchemaContext struct {
	Source
	// mwCfg is the default middleware config
	mwCfg SourceWithSchemaContextConfig

	useContext  bool
	contextName string
}

func (s *sourceWithSchemaContext) Parameters() config.Parameters {
	return mergeParameters(s.Source.Parameters(), s.mwCfg.parameters())
}

func (s *sourceWithSchemaContext) Configure(ctx context.Context, cfg config.Config) error {
	s.useContext = *s.mwCfg.UseContext
	if useStr, ok := cfg[s.mwCfg.UseContextParameterName()]; ok {
		use, err := strconv.ParseBool(useStr)
		if err != nil {
			return fmt.Errorf("could not parse `sdk.schema.context.use`, input %v: %w", useStr, err)
		}
		s.useContext = use
	}

	if s.useContext {
		// the order of precedence (least to most) is:
		// 1. connector ID (if no context name is configured anywhere)
		// 2. default middleware config
		// 3. user config
		s.contextName = internal.ConnectorIDFromContext(ctx)
		if s.mwCfg.ContextName != nil {
			s.contextName = *s.mwCfg.ContextName
		}
		if ctxName, ok := cfg[s.mwCfg.ContextNameParameterName()]; ok {
			s.contextName = ctxName
		}
	}

	return s.Source.Configure(sdkschema.WithSchemaContextName(ctx, s.contextName), cfg)
}

func (s *sourceWithSchemaContext) Open(ctx context.Context, pos opencdc.Position) error {
	return s.Source.Open(sdkschema.WithSchemaContextName(ctx, s.contextName), pos)
}

func (s *sourceWithSchemaContext) Read(ctx context.Context) (opencdc.Record, error) {
	return s.Source.Read(sdkschema.WithSchemaContextName(ctx, s.contextName))
}

func (s *sourceWithSchemaContext) Teardown(ctx context.Context) error {
	return s.Source.Teardown(sdkschema.WithSchemaContextName(ctx, s.contextName))
}

func (s *sourceWithSchemaContext) LifecycleOnCreated(ctx context.Context, config config.Config) error {
	return s.Source.LifecycleOnCreated(sdkschema.WithSchemaContextName(ctx, s.contextName), config)
}

func (s *sourceWithSchemaContext) LifecycleOnUpdated(ctx context.Context, configBefore, configAfter config.Config) error {
	return s.Source.LifecycleOnUpdated(sdkschema.WithSchemaContextName(ctx, s.contextName), configBefore, configAfter)
}

func (s *sourceWithSchemaContext) LifecycleOnDeleted(ctx context.Context, config config.Config) error {
	return s.Source.LifecycleOnDeleted(sdkschema.WithSchemaContextName(ctx, s.contextName), config)
}
