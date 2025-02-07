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
	"reflect"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/cchan"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/jpillora/backoff"
)

// SourceMiddleware wraps a Source and adds functionality to it.
type SourceMiddleware interface {
	Wrap(Source) Source
}

var (
	_ SourceMiddleware = (*SourceWithBatch)(nil)
	_ SourceMiddleware = (*SourceWithEncoding)(nil)
	_ SourceMiddleware = (*SourceWithSchemaContext)(nil)
	_ SourceMiddleware = (*SourceWithSchemaExtraction)(nil)
)

type DefaultSourceMiddleware struct {
	UnimplementedSourceConfig

	SourceWithBatch
	SourceWithEncoding
	SourceWithSchemaContext
	SourceWithSchemaExtraction
}

// Validate validates all the [Validatable] structs in the middleware.
func (c *DefaultSourceMiddleware) Validate(ctx context.Context) error {
	// c is a pointer, we need the value to which the pointer points to
	// (so we can enumerate the fields below)
	val := reflect.ValueOf(c).Elem()
	valType := val.Type()
	validatableInterface := reflect.TypeOf((*Validatable)(nil)).Elem()

	var errs []error
	for i := range valType.NumField() {
		f := valType.Field(i)
		if f.Type.Implements(validatableInterface) {
			// This is a DestinationConfig struct, validate it.
			//nolint:forcetypeassert // type checked above with f.Type.Implements()
			errs = append(errs, val.Field(i).Interface().(Validatable).Validate(ctx))
		}
	}

	return errors.Join(errs...)
}

// SourceWithMiddleware wraps the source into the middleware defined in the
// config.
func SourceWithMiddleware(s Source) Source {
	cfg := s.Config()

	cfgVal := reflect.ValueOf(cfg)
	if cfgVal.Kind() != reflect.Ptr {
		panic("The struct returned in Config() must be a pointer")
	}

	// Collect all middlewares from the config and wrap the source with them
	mw := sourceMiddlewareFromConfigRecursive(cfgVal.Elem())

	// Wrap the middleware in reverse order to preserve the order as specified.
	for i := len(mw) - 1; i >= 0; i-- {
		s = mw[i].Wrap(s)
	}

	return s
}

func sourceMiddlewareFromConfigRecursive(cfgVal reflect.Value) []SourceMiddleware {
	sourceMiddlewareType := reflect.TypeFor[SourceMiddleware]()
	cfgType := cfgVal.Type()

	// Collect all middlewares from the config and wrap the source with them
	var mw []SourceMiddleware
	for i := range cfgVal.NumField() {
		field := cfgVal.Field(i)

		// If the field is not a pointer, we need to get the address of it so
		// that the values parsed in Configure are reflected in the config.
		if field.Kind() != reflect.Ptr {
			field = field.Addr()
		}

		switch {
		case field.Type().Implements(sourceMiddlewareType):
			// This is a middleware config, store it.
			//nolint:forcetypeassert // type checked above with field.Type().Implements()
			mw = append(mw, field.Interface().(SourceMiddleware))

		case cfgType.Field(i).Anonymous &&
			cfgType.Field(i).Type.Kind() == reflect.Struct:
			// This is an embedded struct, dive deeper.
			mw = append(mw, sourceMiddlewareFromConfigRecursive(field.Elem())...)
		}
	}

	return mw
}

// -- SourceWithSchemaExtraction -----------------------------------------------

// SourceWithSchemaExtraction is a middleware that extracts a record's
// payload and key schemas. The schema is extracted from the record data
// for each record produced by the source. The schema is registered with the
// schema service and the schema subject is attached to the record metadata.
type SourceWithSchemaExtraction struct {
	// The type of the payload schema.
	SchemaTypeStr string `json:"sdk.schema.extract.type" validate:"inclusion=avro" default:"avro"`
	// Whether to extract and encode the record payload with a schema.
	PayloadEnabled *bool `json:"sdk.schema.extract.payload.enabled" default:"true"`
	// The subject of the payload schema. If the record metadata contains the
	// field "opencdc.collection" it is prepended to the subject name and
	// separated with a dot.
	PayloadSubject *string `json:"sdk.schema.extract.payload.subject" default:"payload"`
	// Whether to extract and encode the record key with a schema.
	KeyEnabled *bool `json:"sdk.schema.extract.key.enabled" default:"true"`
	// The subject of the key schema. If the record metadata contains the field
	// "opencdc.collection" it is prepended to the subject name and separated
	// with a dot.
	KeySubject *string `json:"sdk.schema.extract.key.subject" default:"key"`
}

// SchemaType returns the typed schema type (and not the string value as it's
// in the configuration itself).
// todo: use https://github.com/ConduitIO/conduit-commons/issues/142 to parse
// the string value into schema.Type directly.
func (c *SourceWithSchemaExtraction) SchemaType() schema.Type {
	if c.SchemaTypeStr == "" {
		return schema.TypeAvro
	}

	t := lang.Ptr(schema.Type(0))
	err := t.UnmarshalText([]byte(c.SchemaTypeStr))
	if err != nil {
		// shouldn't happen, because we have validations on SchemaTypeStr
		panic(err)
	}
	return *t
}

// Wrap a Source into the middleware.
func (c *SourceWithSchemaExtraction) Wrap(impl Source) Source {
	return &sourceWithSchemaExtraction{
		Source: impl,
		config: c,
	}
}

// sourceWithSchemaExtraction is the actual middleware implementation.
type sourceWithSchemaExtraction struct {
	Source
	config *SourceWithSchemaExtraction

	payloadWarnOnce sync.Once
	keyWarnOnce     sync.Once
}

func (s *sourceWithSchemaExtraction) Read(ctx context.Context) (opencdc.Record, error) {
	rec, err := s.Source.Read(ctx)
	if err != nil || (!*s.config.KeyEnabled && !*s.config.PayloadEnabled) {
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

func (s *sourceWithSchemaExtraction) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	recs, err := s.Source.ReadN(ctx, n)
	if err != nil || (!*s.config.KeyEnabled && !*s.config.PayloadEnabled) {
		return recs, err
	}

	for i, rec := range recs {
		if err := s.extractAttachKeySchema(ctx, &rec); err != nil {
			return nil, err
		}
		if err := s.extractAttachPayloadSchema(ctx, &rec); err != nil {
			return nil, err
		}
		recs[i] = rec
	}

	return recs, nil
}

func (s *sourceWithSchemaExtraction) extractAttachKeySchema(ctx context.Context, rec *opencdc.Record) error {
	if !*s.config.KeyEnabled {
		return nil // key schema encoding is disabled
	}
	if rec.Key == nil {
		return nil
	}

	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		s.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record key is not structured, consider disabling the source schema key encoding using "%s: false"`, "schema.extraction.key.enabled")
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
	subject = *s.config.KeySubject
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
	if !*s.config.PayloadEnabled {
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
			Logger(ctx).Warn().Msgf(`record payload is not structured, consider disabling the source schema payload encoding using "%s: false"`, "schema.extraction.payload.enabled")
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
	subject = *s.config.PayloadSubject
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
	srd, err := schema.KnownSerdeFactories[s.config.SchemaType()].SerdeForType(data)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema for value: %w", err)
	}

	sch, err := schema.Create(ctx, s.config.SchemaType(), subject, []byte(srd.String()))
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema: %w", err)
	}

	return sch, nil
}

// -- SourceWithSchemaContext --------------------------------------------------

// SourceWithSchemaContext is a middleware that makes it possible to configure
// the schema context for records read by a source.
type SourceWithSchemaContext struct {
	// Specifies whether to use a schema context name. If set to false, no schema context name will
	// be used, and schemas will be saved with the subject name specified in the connector
	// (not safe because of name conflicts).
	Enabled *bool `json:"sdk.schema.context.enabled" default:"true"`
	// Schema context name to be used. Used as a prefix for all schema subject names.
	// If empty, defaults to the connector ID.
	Name *string `json:"sdk.schema.context.name" default:""`
}

// Wrap a Source into the middleware.
func (c *SourceWithSchemaContext) Wrap(s Source) Source {
	return &sourceWithSchemaContext{
		Source: s,
		config: c,
	}
}

type sourceWithSchemaContext struct {
	Source
	config *SourceWithSchemaContext

	contextName string
}

func (s *sourceWithSchemaContext) Open(ctx context.Context, pos opencdc.Position) error {
	if *s.config.Enabled {
		// The connector ID will serve as the schema context
		// if no schema context was provided in the configuration.
		s.contextName = internal.ConnectorIDFromContext(ctx)
		if s.config.Name != nil {
			s.contextName = *s.config.Name
		}
	}
	return s.Source.Open(schema.WithSchemaContextName(ctx, s.contextName), pos)
}

func (s *sourceWithSchemaContext) Read(ctx context.Context) (opencdc.Record, error) {
	return s.Source.Read(schema.WithSchemaContextName(ctx, s.contextName))
}

func (s *sourceWithSchemaContext) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	return s.Source.ReadN(schema.WithSchemaContextName(ctx, s.contextName), n)
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

	if err := s.encode(ctx, &rec); err != nil {
		return rec, err
	}

	return rec, nil
}

func (s *sourceWithEncoding) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	recs, err := s.Source.ReadN(ctx, n)
	if err != nil {
		return recs, err
	}

	for i := range recs {
		if err := s.encode(ctx, &recs[i]); err != nil {
			return recs, fmt.Errorf("unable to encode record %d: %w", i, err)
		}
	}

	return recs, nil
}

func (s *sourceWithEncoding) encode(ctx context.Context, rec *opencdc.Record) error {
	if err := s.encodeKey(ctx, rec); err != nil {
		return err
	}
	if err := s.encodePayload(ctx, rec); err != nil {
		return err
	}

	return nil
}

func (s *sourceWithEncoding) encodeKey(ctx context.Context, rec *opencdc.Record) error {
	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		s.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msg(`record keys produced by this connector are not structured and won't be encoded"`)
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
	// if there's no schema, there's no encoding
	if sch == nil {
		return nil
	}

	encoded, err := s.encodeWithSchema(*sch, rec.Key)
	if err != nil {
		return fmt.Errorf("failed to encode key: %w", err)
	}

	rec.Key = opencdc.RawData(encoded)
	schema.AttachKeySchemaToRecord(*rec, *sch)
	return nil
}

func (s *sourceWithEncoding) schemaForKey(ctx context.Context, rec opencdc.Record) (*schema.Schema, error) {
	subject, err := rec.Metadata.GetKeySchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return nil, fmt.Errorf("failed to get key schema subject: %w", err)
	}

	version, err := rec.Metadata.GetKeySchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return nil, fmt.Errorf("failed to get key schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		sch, err := schema.Get(ctx, subject, version)
		return &sch, err
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return nil, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected key schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataKeySchemaSubject, subject, opencdc.MetadataKeySchemaVersion, version)
	default:
		//nolint:nilnil // we return nil to indicate that no schema is attached
		return nil, nil
	}
}

func (s *sourceWithEncoding) encodePayload(ctx context.Context, rec *opencdc.Record) error {
	_, beforeIsStructured := rec.Payload.Before.(opencdc.StructuredData)
	_, afterIsStructured := rec.Payload.After.(opencdc.StructuredData)
	if !beforeIsStructured && !afterIsStructured {
		// log warning once, to avoid spamming the logs
		s.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msg(`record payloads produced by this connector are not structured and won't be encoded"`)
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
	// if there's no schema, there's no encoding
	if sch == nil {
		return nil
	}

	// encode both before and after with the extracted schema
	if beforeIsStructured {
		encoded, err := s.encodeWithSchema(*sch, rec.Payload.Before)
		if err != nil {
			return fmt.Errorf("failed to encode before payload: %w", err)
		}
		rec.Payload.Before = opencdc.RawData(encoded)
	}
	if afterIsStructured {
		encoded, err := s.encodeWithSchema(*sch, rec.Payload.After)
		if err != nil {
			return fmt.Errorf("failed to encode after payload: %w", err)
		}
		rec.Payload.After = opencdc.RawData(encoded)
	}
	schema.AttachPayloadSchemaToRecord(*rec, *sch)
	return nil
}

func (s *sourceWithEncoding) schemaForPayload(ctx context.Context, rec opencdc.Record) (*schema.Schema, error) {
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return nil, fmt.Errorf("failed to get payload schema subject: %w", err)
	}

	version, err := rec.Metadata.GetPayloadSchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return nil, fmt.Errorf("failed to get payload schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		sch, err := schema.Get(ctx, subject, version)
		return &sch, err
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return nil, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected payload schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataPayloadSchemaSubject, subject, opencdc.MetadataPayloadSchemaVersion, version)
	default:
		//nolint:nilnil // we return nil to indicate that no schema is attached
		return nil, nil
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

// -- SourceWithBatch --------------------------------------------------

// SourceWithBatch adds support for batching on the source.
type SourceWithBatch struct {
	// Maximum size of batch before it gets read from the source.
	BatchSize *int `json:"sdk.batch.size" default:"0" validate:"gt=-1"`
	// Maximum delay before an incomplete batch is read from the source.
	BatchDelay *time.Duration `json:"sdk.batch.delay" default:"0"`
}

// Wrap a Source into the middleware.
func (c *SourceWithBatch) Wrap(impl Source) Source {
	return &sourceWithBatch{
		Source: impl,
		config: c,
	}
}

type sourceWithBatch struct {
	Source
	config *SourceWithBatch

	readCh  chan readResponse
	readNCh chan readNResponse
	stop    chan struct{}

	collectFn func(context.Context, int) ([]opencdc.Record, error)
}

type readNResponse struct {
	Records []opencdc.Record
	Err     error
}

type readResponse struct {
	Record opencdc.Record
	Err    error
}

func (s *sourceWithBatch) Open(ctx context.Context, pos opencdc.Position) error {
	err := s.Source.Open(ctx, pos)
	if err != nil {
		return err
	}

	s.stop = make(chan struct{})
	s.readCh = make(chan readResponse, *s.config.BatchSize)
	s.readNCh = make(chan readNResponse, 1)

	if *s.config.BatchSize > 0 || *s.config.BatchDelay > 0 {
		s.collectFn = s.collectWithReadN
		go s.runReadN(ctx)
	} else {
		// Batching not configured, simply forward the call.
		s.collectFn = s.Source.ReadN
	}

	return nil
}

func (s *sourceWithBatch) runReadN(ctx context.Context) {
	defer close(s.readNCh)

	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second * 5,
	}

	for {
		recs, err := s.Source.ReadN(ctx, *s.config.BatchSize)
		if err != nil {
			switch {
			case errors.Is(err, ErrBackoffRetry):
				// the plugin wants us to retry reading later
				_, _, err := cchan.ChanOut[time.Time](time.After(b.Duration())).Recv(ctx)
				if err != nil {
					// The plugin is using the SDK for long-polling
					// and relying on the SDK to check for a cancelled context.
					return
				}
				continue

			case errors.Is(err, context.Canceled):
				s.readNCh <- readNResponse{Err: err}
				return

			case errors.Is(err, ErrUnimplemented):
				Logger(ctx).Info().Msg("source does not support batch reads, falling back to single reads")

				go s.runRead(ctx)
				s.readNCh <- readNResponse{Err: err}
				return

			default:
				s.readNCh <- readNResponse{Err: err}
				return
			}
		}

		select {
		case s.readNCh <- readNResponse{Records: recs}:
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		}
	}
}

func (s *sourceWithBatch) runRead(ctx context.Context) {
	defer close(s.readCh)

	b := &backoff.Backoff{
		Factor: 2,
		Min:    time.Millisecond * 100,
		Max:    time.Second * 5,
	}

	for {
		rec, err := s.Source.Read(ctx)
		if err != nil {
			if errors.Is(err, ErrBackoffRetry) {
				// the plugin wants us to retry reading later
				_, _, err := cchan.ChanOut[time.Time](time.After(b.Duration())).Recv(ctx)
				if err != nil {
					// The plugin is using the SDK for long-polling
					// and relying on the SDK to check for a cancelled context.
					return
				}
				continue
			}
			if errors.Is(err, context.Canceled) {
				s.readNCh <- readNResponse{Err: err}
				return
			}

			s.readCh <- readResponse{Err: err}
			return
		}

		select {
		case s.readCh <- readResponse{Record: rec}:
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		}
	}
}

func (s *sourceWithBatch) Read(ctx context.Context) (opencdc.Record, error) {
	return s.Source.Read(ctx)
}

func (s *sourceWithBatch) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	return s.collectFn(ctx, n)
}

func (s *sourceWithBatch) collectWithRead(ctx context.Context, _ int) ([]opencdc.Record, error) {
	batch := make([]opencdc.Record, 0, *s.config.BatchSize)
	var delay <-chan time.Time

	for {
		select {
		case resp, ok := <-s.readCh:
			if !ok {
				return batch, ctx.Err()
			}
			if resp.Err != nil {
				return nil, resp.Err
			}

			batch = append(batch, resp.Record)
			if *s.config.BatchSize > 0 && len(batch) >= *s.config.BatchSize {
				// batch is full, flush it
				return batch, nil
			}

			if *s.config.BatchDelay > 0 && delay == nil {
				// start the delay timer after we have received the first batch
				delay = time.After(*s.config.BatchDelay)
			}

			// continue reading until the batch is full or the delay timer has expired
		case <-delay:
			// delay timer has expired, flush the batch
			return batch, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *sourceWithBatch) collectWithReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	batch := make([]opencdc.Record, 0, *s.config.BatchSize)
	var delay <-chan time.Time

	for {
		select {
		case resp, ok := <-s.readNCh:
			if !ok {
				return batch, ctx.Err()
			}
			if resp.Err != nil {
				if errors.Is(resp.Err, ErrUnimplemented) {
					// the plugin doesn't support batch reads, fallback to single reads
					s.collectFn = s.collectWithRead
					return s.collectWithRead(ctx, n)
				}
				return nil, resp.Err
			}

			if len(batch) == 0 && len(resp.Records) >= *s.config.BatchSize {
				// source returned a batch that is already full, flush it
				return resp.Records, nil
			}

			batch = append(batch, resp.Records...)
			if *s.config.BatchSize > 0 && len(batch) >= *s.config.BatchSize {
				// batch is full, flush it
				return batch, nil
			}

			if *s.config.BatchDelay > 0 && delay == nil {
				// start the delay timer after we have received the first batch
				delay = time.After(*s.config.BatchDelay)
			}

			// continue reading until the batch is full or the delay timer has expired
		case <-delay:
			// delay timer has expired, flush the batch
			return batch, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
