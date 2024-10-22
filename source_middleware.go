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
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/conduit-connector-sdk/schema"
)

// SourceMiddleware wraps a Source and adds functionality to it.
type SourceMiddleware interface {
	Wrap(Source) Source
}

// SourceMiddlewareOption can be used to change the behavior of the default source
// middleware created with DefaultSourceMiddleware.
type SourceMiddlewareOption interface {
	Apply(SourceMiddleware)
}

// Available source middleware options.
var (
	_ SourceMiddlewareOption = SourceWithSchemaExtractionConfig{}
	_ SourceMiddlewareOption = SourceWithSchemaContextConfig{}
	_ SourceMiddlewareOption = SourceWithEncoding{}
)

// DefaultSourceMiddleware returns a slice of middleware that should be added to
// all sources unless there's a good reason not to.
func DefaultSourceMiddleware(opts ...SourceMiddlewareOption) []SourceMiddleware {
	middleware := []SourceMiddleware{
		&SourceWithSchemaContext{},
		&SourceWithSchemaExtraction{},
		&SourceWithEncoding{},
	}

	// apply options to all middleware
	for _, m := range middleware {
		for _, opt := range opts {
			opt.Apply(m)
		}
	}
	return middleware
}

// SourceWithMiddleware wraps the source into the supplied middleware.
func SourceWithMiddleware(s Source, middleware ...SourceMiddleware) Source {
	// apply middleware in reverse order to preserve the order as specified
	for i := len(middleware) - 1; i >= 0; i-- {
		s = middleware[i].Wrap(s)
	}
	return s
}

// -- SourceWithSchemaExtraction ----------------------------------------------

const (
	configSourceSchemaExtractionType           = "sdk.schema.extract.type"
	configSourceSchemaExtractionPayloadEnabled = "sdk.schema.extract.payload.enabled"
	configSourceSchemaExtractionPayloadSubject = "sdk.schema.extract.payload.subject"
	configSourceSchemaExtractionKeyEnabled     = "sdk.schema.extract.key.enabled"
	configSourceSchemaExtractionKeySubject     = "sdk.schema.extract.key.subject"
)

// SourceWithSchemaExtractionConfig is the configuration for the
// SourceWithSchemaExtraction middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// SourceWithSchemaExtractionConfig can be used as a SourceMiddlewareOption.
type SourceWithSchemaExtractionConfig struct {
	// The type of the payload schema. Defaults to Avro.
	SchemaType schema.Type
	// Whether to extract and encode the record payload with a schema.
	// If unset, defaults to true.
	PayloadEnabled *bool
	// The subject of the payload schema. If unset, defaults to "payload".
	PayloadSubject *string
	// Whether to extract and encode the record key with a schema.
	// If unset, defaults to true.
	KeyEnabled *bool
	// The subject of the key schema. If unset, defaults to "key".
	KeySubject *string
}

// Apply sets the default configuration for the SourceWithSchemaExtraction middleware.
func (c SourceWithSchemaExtractionConfig) Apply(m SourceMiddleware) {
	if s, ok := m.(*SourceWithSchemaExtraction); ok {
		s.Config = c
	}
}

func (c SourceWithSchemaExtractionConfig) SchemaTypeParameterName() string {
	return configSourceSchemaExtractionType
}

func (c SourceWithSchemaExtractionConfig) SchemaPayloadEnabledParameterName() string {
	return configSourceSchemaExtractionPayloadEnabled
}

func (c SourceWithSchemaExtractionConfig) SchemaPayloadSubjectParameterName() string {
	return configSourceSchemaExtractionPayloadSubject
}

func (c SourceWithSchemaExtractionConfig) SchemaKeyEnabledParameterName() string {
	return configSourceSchemaExtractionKeyEnabled
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
		configSourceSchemaExtractionPayloadEnabled: {
			Default:     strconv.FormatBool(*c.PayloadEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and encode the record payload with a schema.",
		},
		configSourceSchemaExtractionPayloadSubject: {
			Default:     *c.PayloadSubject,
			Type:        config.ParameterTypeString,
			Description: `The subject of the payload schema. If the record metadata contains the field "opencdc.collection" it is prepended to the subject name and separated with a dot.`,
		},
		configSourceSchemaExtractionKeyEnabled: {
			Default:     strconv.FormatBool(*c.KeyEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and encode the record key with a schema.",
		},
		configSourceSchemaExtractionKeySubject: {
			Default:     *c.KeySubject,
			Type:        config.ParameterTypeString,
			Description: `The subject of the key schema. If the record metadata contains the field "opencdc.collection" it is prepended to the subject name and separated with a dot.`,
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

// SourceWithSchemaExtraction is a middleware that extracts a record's
// payload and key schemas. The schema is extracted from the record data
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

	if s.Config.KeyEnabled == nil {
		s.Config.KeyEnabled = ptr(true)
	}
	if s.Config.KeySubject == nil {
		s.Config.KeySubject = ptr("key")
	}

	if s.Config.PayloadEnabled == nil {
		s.Config.PayloadEnabled = ptr(true)
	}
	if s.Config.PayloadSubject == nil {
		s.Config.PayloadSubject = ptr("payload")
	}

	return &sourceWithSchemaExtraction{
		Source:   impl,
		defaults: s.Config,
	}
}

// sourceWithSchemaExtraction is the actual middleware implementation.
type sourceWithSchemaExtraction struct {
	Source
	defaults SourceWithSchemaExtractionConfig

	schemaType     schema.Type
	payloadSubject string
	keySubject     string

	payloadWarnOnce sync.Once
	keyWarnOnce     sync.Once
}

func (s *sourceWithSchemaExtraction) Parameters() config.Parameters {
	return mergeParameters(s.Source.Parameters(), s.defaults.parameters())
}

func (s *sourceWithSchemaExtraction) Configure(ctx context.Context, config config.Config) error {
	err := s.Source.Configure(ctx, config)
	if err != nil {
		return err
	}

	s.schemaType = s.defaults.SchemaType
	if val, ok := config[configSourceSchemaExtractionType]; ok {
		if err := s.schemaType.UnmarshalText([]byte(val)); err != nil {
			return fmt.Errorf("invalid %s: failed to parse schema type: %w", configSourceSchemaExtractionType, err)
		}
	}

	encodeKey := *s.defaults.KeyEnabled
	if val, ok := config[configSourceSchemaExtractionKeyEnabled]; ok {
		encodeKey, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configSourceSchemaExtractionKeyEnabled, err)
		}
	}
	if encodeKey {
		s.keySubject = *s.defaults.KeySubject
		if val, ok := config[configSourceSchemaExtractionKeySubject]; ok {
			s.keySubject = val
		}
	}

	encodePayload := *s.defaults.PayloadEnabled
	if val, ok := config[configSourceSchemaExtractionPayloadEnabled]; ok {
		encodePayload, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configSourceSchemaExtractionPayloadEnabled, err)
		}
	}
	if encodePayload {
		s.payloadSubject = *s.defaults.PayloadSubject
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

	if err := s.extractAttachKeySchema(ctx, &rec); err != nil {
		return rec, err
	}
	if err := s.extractAttachPayloadSchema(ctx, &rec); err != nil {
		return rec, err
	}

	return rec, nil
}

func (s *sourceWithSchemaExtraction) extractAttachKeySchema(ctx context.Context, rec *opencdc.Record) error {
	if s.keySubject == "" {
		return nil // key schema extraction is disabled
	}
	if rec.Key == nil {
		return nil
	}

	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		s.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record key is not structured, consider disabling the source schema key encoding using "%s: false"`, configSourceSchemaExtractionKeyEnabled)
		})
		return nil
	}

	if rec.Metadata == nil {
		// ensure we have a metadata value, to make it safe for retrieving and setting values
		rec.Metadata = opencdc.Metadata{}
	}
	sch, err := s.extractKeySchema(ctx, *rec)
	if err != nil {
		return err // already wrapped
	}

	schema.AttachKeySchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithSchemaExtraction) extractKeySchema(ctx context.Context, rec opencdc.Record) (schema.Schema, error) {
	// If the record has a key schema already,
	// then we return it (no need to extract a schema).
	subject, err := rec.Metadata.GetKeySchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get key schema subject: %w", err)
	}

	version, err := rec.Metadata.GetKeySchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get key schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		return schema.Get(ctx, subject, version)
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return schema.Schema{}, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected key schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataKeySchemaSubject, subject, opencdc.MetadataKeySchemaVersion, version)
	}

	// No schema subject or version is attached, we need to extract the schema.
	subject = s.keySubject
	if collection, err := rec.Metadata.GetCollection(); err == nil {
		subject = collection + "." + subject
	}

	sch, err := s.schemaForType(ctx, rec.Key, subject)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to extract schema for key: %w", err)
	}

	return sch, nil
}

func (s *sourceWithSchemaExtraction) extractAttachPayloadSchema(ctx context.Context, rec *opencdc.Record) error {
	if s.payloadSubject == "" {
		return nil // payload schema encoding is disabled
	}
	if (rec.Payload == opencdc.Change{}) {
		return nil
	}
	_, beforeIsStructured := rec.Payload.Before.(opencdc.StructuredData)
	_, afterIsStructured := rec.Payload.After.(opencdc.StructuredData)
	if !beforeIsStructured && !afterIsStructured {
		// log warning once, to avoid spamming the logs
		s.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record payload is not structured, consider disabling the source schema payload encoding using "%s: false"`, configSourceSchemaExtractionPayloadEnabled)
		})
		return nil
	}

	if rec.Metadata == nil {
		// ensure we have a metadata value, to make it safe for retrieving and setting values
		rec.Metadata = opencdc.Metadata{}
	}
	sch, err := s.extractPayloadSchema(ctx, *rec)
	if err != nil {
		return fmt.Errorf("failed to extract schema for payload: %w", err)
	}

	schema.AttachPayloadSchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithSchemaExtraction) extractPayloadSchema(ctx context.Context, rec opencdc.Record) (schema.Schema, error) {
	// If the record has a payload schema already,
	// then we return it (no need to extract a schema).
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get payload schema subject: %w", err)
	}

	version, err := rec.Metadata.GetPayloadSchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get payload schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		return schema.Get(ctx, subject, version)
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return schema.Schema{}, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected payload schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataPayloadSchemaSubject, subject, opencdc.MetadataPayloadSchemaVersion, version)
	}

	// No schema subject or version is attached, we need to extract the schema.
	subject = s.payloadSubject
	if collection, err := rec.Metadata.GetCollection(); err == nil {
		subject = collection + "." + subject
	}

	val := rec.Payload.After
	if _, ok := val.(opencdc.StructuredData); !ok {
		// use before as a fallback
		val = rec.Payload.Before
	}

	sch, err := s.schemaForType(ctx, val, subject)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to extract schema for payload: %w", err)
	}

	return sch, nil
}

func (s *sourceWithSchemaExtraction) schemaForType(ctx context.Context, data any, subject string) (schema.Schema, error) {
	srd, err := schema.KnownSerdeFactories[s.schemaType].SerdeForType(data)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema for value: %w", err)
	}

	sch, err := schema.Create(ctx, s.schemaType, subject, []byte(srd.String()))
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema: %w", err)
	}

	return sch, nil
}

// -- SourceWithSchemaContext --------------------------------------------------

// SourceWithSchemaContextConfig is the configuration for the
// SourceWithSchemaContext middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// SourceWithSchemaContextConfig can be used as a SourceMiddlewareOption.
type SourceWithSchemaContextConfig struct {
	Enabled *bool
	Name    *string
}

// Apply sets the default configuration for the SourceWithSchemaExtraction middleware.
func (c SourceWithSchemaContextConfig) Apply(m SourceMiddleware) {
	if s, ok := m.(*SourceWithSchemaContext); ok {
		s.Config = c
	}
}

func (c SourceWithSchemaContextConfig) EnabledParameterName() string {
	return "sdk.schema.context.enabled"
}

func (c SourceWithSchemaContextConfig) NameParameterName() string {
	return "sdk.schema.context.name"
}

func (c SourceWithSchemaContextConfig) parameters() config.Parameters {
	return config.Parameters{
		c.EnabledParameterName(): config.Parameter{
			Default: strconv.FormatBool(*c.Enabled),
			Description: "Specifies whether to use a schema context name. If set to false, no schema context name will " +
				"be used, and schemas will be saved with the subject name specified in the connector " +
				"(not safe because of name conflicts).",
			Type: config.ParameterTypeBool,
		},
		c.NameParameterName(): config.Parameter{
			Default: func() string {
				if c.Name == nil {
					return ""
				}

				return *c.Name
			}(),
			Description: func() string {
				d := "Schema context name to be used. Used as a prefix for all schema subject names."
				if c.Name == nil {
					d += " Defaults to the connector ID."
				}
				return d
			}(),
			Type: config.ParameterTypeString,
		},
	}
}

// SourceWithSchemaContext is a middleware that makes it possible to configure
// the schema context for records read by a source.
type SourceWithSchemaContext struct {
	Config SourceWithSchemaContextConfig
}

// Wrap a Source into the schema middleware. It will apply default configuration
// values if they are not explicitly set.
func (s *SourceWithSchemaContext) Wrap(impl Source) Source {
	if s.Config.Enabled == nil {
		s.Config.Enabled = ptr(true)
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
	s.useContext = *s.mwCfg.Enabled
	if useStr, ok := cfg[s.mwCfg.EnabledParameterName()]; ok {
		use, err := strconv.ParseBool(useStr)
		if err != nil {
			return fmt.Errorf("could not parse `%v`, input %v: %w", s.mwCfg.EnabledParameterName(), useStr, err)
		}
		s.useContext = use
	}

	if s.useContext {
		// The order of precedence is (from highest to lowest):
		// 1. user config
		// 2. default middleware config
		// 3. connector ID (if no context name is configured anywhere)
		s.contextName = internal.ConnectorIDFromContext(ctx)
		if s.mwCfg.Name != nil {
			s.contextName = *s.mwCfg.Name
		}
		if ctxName, ok := cfg[s.mwCfg.NameParameterName()]; ok {
			s.contextName = ctxName
		}
	}

	return s.Source.Configure(schema.WithSchemaContextName(ctx, s.contextName), cfg)
}

func (s *sourceWithSchemaContext) Open(ctx context.Context, pos opencdc.Position) error {
	return s.Source.Open(schema.WithSchemaContextName(ctx, s.contextName), pos)
}

func (s *sourceWithSchemaContext) Read(ctx context.Context) (opencdc.Record, error) {
	return s.Source.Read(schema.WithSchemaContextName(ctx, s.contextName))
}

func (s *sourceWithSchemaContext) Teardown(ctx context.Context) error {
	return s.Source.Teardown(schema.WithSchemaContextName(ctx, s.contextName))
}

func (s *sourceWithSchemaContext) LifecycleOnCreated(ctx context.Context, config config.Config) error {
	return s.Source.LifecycleOnCreated(schema.WithSchemaContextName(ctx, s.contextName), config)
}

func (s *sourceWithSchemaContext) LifecycleOnUpdated(ctx context.Context, configBefore, configAfter config.Config) error {
	return s.Source.LifecycleOnUpdated(schema.WithSchemaContextName(ctx, s.contextName), configBefore, configAfter)
}

func (s *sourceWithSchemaContext) LifecycleOnDeleted(ctx context.Context, config config.Config) error {
	return s.Source.LifecycleOnDeleted(schema.WithSchemaContextName(ctx, s.contextName), config)
}

// -- SourceWithEncoding ------------------------------------------------------

// SourceWithEncoding is a middleware that encodes the record payload and key
// with the provided schema. The schema is registered with the schema service
// and the schema subject is attached to the record metadata.
type SourceWithEncoding struct{}

func (s SourceWithEncoding) Apply(SourceMiddleware) {
}

func (s SourceWithEncoding) Wrap(impl Source) Source {
	return &sourceWithEncoding{Source: impl}
}

// sourceWithEncoding is the actual middleware implementation.
type sourceWithEncoding struct {
	Source

	keyWarnOnce     sync.Once
	payloadWarnOnce sync.Once
}

func (s *sourceWithEncoding) Read(ctx context.Context) (opencdc.Record, error) {
	rec, err := s.Source.Read(ctx)
	if err != nil {
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

func (s *sourceWithEncoding) encodeKey(ctx context.Context, rec *opencdc.Record) error {
	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		s.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record key is not structured, consider disabling the source schema key encoding using "%s: false"`, configSourceSchemaExtractionKeyEnabled)
		})
		return nil
	}

	if rec.Metadata == nil {
		// ensure we have a metadata value, to make it safe for retrieving and setting values
		rec.Metadata = opencdc.Metadata{}
	}
	sch, err := s.schemaForKey(ctx, *rec)
	if err != nil {
		return err // already wrapped
	}

	encoded, err := s.encodeWithSchema(sch, rec.Key)
	if err != nil {
		return fmt.Errorf("failed to encode key: %w", err)
	}

	rec.Key = opencdc.RawData(encoded)
	schema.AttachKeySchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithEncoding) schemaForKey(ctx context.Context, rec opencdc.Record) (schema.Schema, error) {
	subject, err := rec.Metadata.GetKeySchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get key schema subject: %w", err)
	}

	version, err := rec.Metadata.GetKeySchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get key schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		return schema.Get(ctx, subject, version)
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return schema.Schema{}, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected key schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataKeySchemaSubject, subject, opencdc.MetadataKeySchemaVersion, version)
	default:
		// todo handle case
		return schema.Schema{}, nil
	}
}

func (s *sourceWithEncoding) encodePayload(ctx context.Context, rec *opencdc.Record) error {
	_, beforeIsStructured := rec.Payload.Before.(opencdc.StructuredData)
	_, afterIsStructured := rec.Payload.After.(opencdc.StructuredData)
	if !beforeIsStructured && !afterIsStructured {
		// log warning once, to avoid spamming the logs
		s.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record payload is not structured, consider disabling the source schema payload encoding using "%s: false"`, configSourceSchemaExtractionPayloadEnabled)
		})
		return nil
	}

	if rec.Metadata == nil {
		// ensure we have a metadata value, to make it safe for retrieving and setting values
		rec.Metadata = opencdc.Metadata{}
	}
	sch, err := s.schemaForPayload(ctx, *rec)
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
	schema.AttachPayloadSchemaToRecord(*rec, sch)
	return nil
}

func (s *sourceWithEncoding) schemaForPayload(ctx context.Context, rec opencdc.Record) (schema.Schema, error) {
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get payload schema subject: %w", err)
	}

	version, err := rec.Metadata.GetPayloadSchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get payload schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		return schema.Get(ctx, subject, version)
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return schema.Schema{}, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected payload schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataPayloadSchemaSubject, subject, opencdc.MetadataPayloadSchemaVersion, version)
	default:
		// todo handle this better
		return schema.Schema{}, nil
	}
}

func (s *sourceWithEncoding) encodeWithSchema(sch schema.Schema, data any) ([]byte, error) {
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

// -- Utility functions  ------------------------------------------------------

func ptr[T any](t T) *T {
	return &t
}
