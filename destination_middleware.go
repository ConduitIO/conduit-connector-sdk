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
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"golang.org/x/time/rate"
)

var destinationMiddlewareType = reflect.TypeFor[DestinationMiddleware]()

// DestinationMiddleware wraps a Destination and adds functionality to it.
type DestinationMiddleware interface {
	Wrap(Destination) Destination
}

var (
	_ DestinationMiddleware = (*DestinationWithRateLimit)(nil)
	_ DestinationMiddleware = (*DestinationWithRecordFormat)(nil)
	_ DestinationMiddleware = (*DestinationWithBatch)(nil)
	_ DestinationMiddleware = (*DestinationWithSchemaExtraction)(nil)
)

// DefaultDestinationMiddleware should be embedded in the DestinationConfig
// struct to provide a list of default middlewares. Note that if the embedding
// struct overwrites Validate manually, it should call Validate on this struct
// as well.
type DefaultDestinationMiddleware struct {
	UnimplementedDestinationConfig

	DestinationWithRateLimit
	DestinationWithRecordFormat
	DestinationWithBatch
	DestinationWithSchemaExtraction
}

// Validate validates all the [Validatable] structs in the middleware.
func (c *DefaultDestinationMiddleware) Validate(ctx context.Context) error {
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

// DestinationWithMiddleware wraps the destination into the middleware defined
// in the config.
func DestinationWithMiddleware(d Destination) Destination {
	cfg := d.Config()

	cfgVal := reflect.ValueOf(cfg)
	if cfgVal.Kind() != reflect.Ptr {
		panic("The struct returned in Config() must be a pointer")
	}
	cfgVal = cfgVal.Elem()

	// Collect all middlewares from the config and wrap the destination with them
	var mw []DestinationMiddleware
	for i := range cfgVal.NumField() {
		field := cfgVal.Field(i)

		// If the field is not a pointer, we need to get the address of it so
		// that the values parsed in Configure are reflected in the config.
		if field.Kind() != reflect.Ptr {
			field = field.Addr()
		}
		if field.Type().Implements(destinationMiddlewareType) {
			// This is a middleware config, store it.
			//nolint:forcetypeassert // type checked above with field.Type().Implements()
			mw = append(mw, field.Interface().(DestinationMiddleware))
		}
	}

	// Wrap the middleware in reverse order to preserve the order as specified.
	for i := len(mw) - 1; i >= 0; i-- {
		d = mw[i].Wrap(d)
	}

	return d
}

// -- DestinationWithBatch -----------------------------------------------------

type ctxKeyBatchConfig struct{}

// DestinationWithBatch adds support for batching on the destination.
type DestinationWithBatch struct {
	UnimplementedDestinationConfig

	// Maximum size of batch before it gets written to the destination.
	BatchSize int `json:"sdk.batch.size" default:"0" validate:"gt=-1"`
	// Maximum delay before an incomplete batch is written to the destination.
	BatchDelay time.Duration `json:"sdk.batch.delay" default:"0" validate:"gt=-1"`
}

// Wrap a Destination into the middleware.
func (c *DestinationWithBatch) Wrap(impl Destination) Destination {
	return &destinationWithBatch{
		Destination: impl,
		config:      c,
	}
}

type destinationWithBatch struct {
	Destination
	config *DestinationWithBatch
}

func (d *destinationWithBatch) Open(ctx context.Context) error {
	err := d.Destination.Open(ctx)
	if err != nil {
		return err
	}

	// TODO: emit this warning once we move to source batching
	// if cfg.BatchDelay > 0 || cfg.BatchSize > 0 {
	// 	Logger(ctx).Warn().Msg("Batching in the destination is deprecated. Consider moving the `sdk.batch.size` and `sdk.batch.delay` parameters to the source connector to enable batching at the source.")
	// }

	// Batching is actually implemented in the plugin adapter because it is the
	// only place we have access to acknowledgments.
	// We need to signal back to the adapter that batching is enabled. We do
	// this by changing a pointer that is stored in the context. It's a bit
	// hacky, but the only way to propagate a value back to the adapter without
	// changing the interface.
	d.setBatchConfig(ctx, *d.config)

	return nil
}

// setBatchConfig stores a DestinationWithBatch instance in the context. If the context already
// contains the key it will update the DestinationWithBatch under that key and return the
// same context, otherwise it will return a new context with the stored value.
// This is used to signal to destinationPluginAdapter if the Destination is
// wrapped into DestinationWithBatchConfig middleware.
func (*destinationWithBatch) setBatchConfig(ctx context.Context, cfg DestinationWithBatch) context.Context {
	ctxCfg, ok := ctx.Value(ctxKeyBatchConfig{}).(*DestinationWithBatch)
	if ok {
		*ctxCfg = cfg
	} else {
		ctx = context.WithValue(ctx, ctxKeyBatchConfig{}, &cfg)
	}
	return ctx
}

func (*destinationWithBatch) getBatchConfig(ctx context.Context) DestinationWithBatch {
	ctxCfg, ok := ctx.Value(ctxKeyBatchConfig{}).(*DestinationWithBatch)
	if !ok {
		return DestinationWithBatch{}
	}
	return *ctxCfg
}

// -- DestinationWithRateLimit -------------------------------------------------

// DestinationWithRateLimit adds support for rate limiting to the destination.
type DestinationWithRateLimit struct {
	UnimplementedDestinationConfig

	// Maximum number of records written per second (0 means no rate limit).
	RatePerSecond float64 `json:"sdk.rate.perSecond" default:"0" validate:"gt=-1"`
	// Allow bursts of at most X records (0 or less means that bursts are not
	// limited). Only takes effect if a rate limit per second is set. Note that
	// if `sdk.batch.size` is bigger than `sdk.rate.burst`, the effective batch
	// size will be equal to `sdk.rate.burst`.
	Burst int `json:"sdk.rate.burst" default:"0" validate:"gt=-1"`
}

// Wrap a Destination into the middleware.
func (c *DestinationWithRateLimit) Wrap(impl Destination) Destination {
	return &destinationWithRateLimit{
		Destination: impl,
		config:      c,
	}
}

type destinationWithRateLimit struct {
	Destination

	config  *DestinationWithRateLimit
	limiter *rate.Limiter
}

func (d *destinationWithRateLimit) Open(ctx context.Context) error {
	err := d.Destination.Open(ctx)
	if err != nil {
		return err
	}

	if d.config.RatePerSecond > 0 {
		burst := d.config.Burst
		if d.config.Burst <= 0 {
			// non-positive numbers would prevent all writes, we default it to
			// be the same size as the per second limit
			burst = int(math.Ceil(d.config.RatePerSecond))
		}
		d.limiter = rate.NewLimiter(rate.Limit(d.config.RatePerSecond), burst)
		Logger(ctx).Info().Msgf("Rate limiting enabled: %.2f records per second, bursts of %d records", d.config.RatePerSecond, burst)
	}

	return nil
}

func (d *destinationWithRateLimit) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	if d.limiter == nil {
		return d.Destination.Write(ctx, recs)
	}

	// split the records into smaller chunks
	// this is necessary because the rate limiter only allows bursts of a certain size
	// and we need to wait for each chunk
	written := 0
	chunkSize := d.limiter.Burst()
	for len(recs) > 0 {
		if chunkSize > len(recs) {
			chunkSize = len(recs)
		}
		chunk := recs[:chunkSize]
		recs = recs[chunkSize:]
		err := d.limiter.WaitN(ctx, len(chunk))
		if err != nil {
			return 0, fmt.Errorf("rate limiter: %w", err)
		}
		n, err := d.Destination.Write(ctx, chunk)
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// -- DestinationWithRecordFormat ----------------------------------------------

// DestinationWithRecordFormat adds support for changing the output format of
// records, specifically of the Record.Bytes method.
type DestinationWithRecordFormat struct {
	UnimplementedDestinationConfig

	// The format of the output record. See the Conduit documentation for a full
	// list of supported formats (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
	RecordFormat *string `json:"sdk.record.format" default:"opencdc/json"`
	// Options to configure the chosen output record format. Options are normally
	// key=value pairs separated with comma (e.g. opt1=val2,opt2=val2), except
	// for the `template` record format, where options are a Go template.
	RecordFormatOptions string `json:"sdk.record.format.options" default:""`

	// RecordSerializers can be set to change the list uf supported formats. It
	// defaults to the output of DestinationWithRecordFormat.DefaultSerializers.
	RecordSerializers []RecordSerializer `json:"-"`
}

// Wrap a Destination into the middleware.
func (c *DestinationWithRecordFormat) Wrap(impl Destination) Destination {
	if c.RecordFormat == nil {
		c.RecordFormat = lang.Ptr(defaultSerializer.Name())
	}
	if c.RecordSerializers == nil {
		c.RecordSerializers = c.DefaultRecordSerializers()
	}
	// sort record serializers by name to ensure we can binary search them
	slices.SortFunc(
		c.RecordSerializers,
		func(a, b RecordSerializer) int {
			return strings.Compare(a.Name(), b.Name())
		},
	)
	return &destinationWithRecordFormat{
		Destination: impl,
		config:      c,
	}
}

// DefaultRecordSerializers returns the list of record serializers that are used
// if DestinationWithRecordFormat.RecordSerializers is nil.
func (c *DestinationWithRecordFormat) DefaultRecordSerializers() []RecordSerializer {
	serializers := []RecordSerializer{
		// define specific serializers here
		TemplateRecordSerializer{},
	}

	// add generic serializers here, they are combined in all possible combinations
	genericConverters := []Converter{
		OpenCDCConverter{},
		DebeziumConverter{},
	}
	genericEncoders := []Encoder{
		JSONEncoder{},
	}

	for _, conv := range genericConverters {
		for _, enc := range genericEncoders {
			serializers = append(
				serializers,
				GenericRecordSerializer{
					Converter: conv,
					Encoder:   enc,
				},
			)
		}
	}
	return serializers
}

func (c *DestinationWithRecordFormat) formats() []string {
	names := make([]string, len(c.RecordSerializers))

	i := 0
	for _, c := range c.RecordSerializers {
		names[i] = c.Name()
		i++
	}
	return names
}

func (c *DestinationWithRecordFormat) getRecordSerializer() (RecordSerializer, error) {
	i, ok := slices.BinarySearch(c.formats(), *c.RecordFormat)
	if !ok {
		return nil, fmt.Errorf("invalid sdk.record.format: %q not found in %v", *c.RecordFormat, c.formats())
	}

	serializer := c.RecordSerializers[i]
	serializer, err := serializer.Configure(c.RecordFormatOptions)
	if err != nil {
		return nil, fmt.Errorf("invalid sdk.record.format.options for %q: %w", *c.RecordFormat, err)
	}

	return serializer, nil
}

func (c *DestinationWithRecordFormat) Validate(context.Context) error {
	_, err := c.getRecordSerializer()
	return err
}

type destinationWithRecordFormat struct {
	Destination
	config *DestinationWithRecordFormat

	serializer RecordSerializer
}

func (d *destinationWithRecordFormat) Open(ctx context.Context) error {
	err := d.Destination.Open(ctx)
	if err != nil {
		return err
	}

	d.serializer, _ = d.config.getRecordSerializer()
	return nil
}

func (d *destinationWithRecordFormat) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	for i := range recs {
		recs[i].SetSerializer(d.serializer)
	}
	return d.Destination.Write(ctx, recs)
}

// -- DestinationWithSchemaExtraction ------------------------------------------

// DestinationWithSchemaExtraction is a middleware that extracts and decodes the
// key and/or payload of a record using a schema. It takes the schema subject and
// version from the record metadata, fetches the schema from the schema service,
// and decodes the key and/or payload using the schema.
// If the schema subject and version is not found in the record metadata, it will
// log a warning and skip decoding the key and/or payload. This middleware is
// useful when the source connector sends the data with the schema attached.
// This middleware is the counterpart of SourceWithSchemaExtraction.
type DestinationWithSchemaExtraction struct {
	UnimplementedDestinationConfig

	// Whether to extract and decode the record payload with a schema.
	PayloadEnabled *bool `json:"sdk.schema.extract.payload.enabled" default:"true"`
	// Whether to extract and decode the record key with a schema.
	KeyEnabled *bool `json:"sdk.schema.extract.key.enabled" default:"true"`
}

// Wrap a Destination into the middleware.
func (c *DestinationWithSchemaExtraction) Wrap(impl Destination) Destination {
	return &destinationWithSchemaExtraction{
		Destination: impl,
		config:      c,
	}
}

// destinationWithSchemaExtraction is the actual middleware implementation.
type destinationWithSchemaExtraction struct {
	Destination
	config *DestinationWithSchemaExtraction

	payloadWarnOnce sync.Once
	keyWarnOnce     sync.Once
}

func (d *destinationWithSchemaExtraction) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i := range records {
		if *d.config.KeyEnabled {
			if err := d.decodeKey(ctx, &records[i]); err != nil {
				if len(records) > 0 {
					err = fmt.Errorf("record %d: %w", i, err)
				}
				return 0, err
			}
		}

		if *d.config.PayloadEnabled {
			if err := d.decodePayload(ctx, &records[i]); err != nil {
				if len(records) > 0 {
					err = fmt.Errorf("record %d: %w", i, err)
				}
				return 0, err
			}
		}
	}

	return d.Destination.Write(ctx, records)
}

func (d *destinationWithSchemaExtraction) decodeKey(ctx context.Context, rec *opencdc.Record) error {
	subject, errSubject := rec.Metadata.GetKeySchemaSubject()
	version, errVersion := rec.Metadata.GetKeySchemaVersion()
	switch {
	case errSubject != nil && !errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get key schema subject from metadata: %w", errSubject)
	case errVersion != nil && !errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get key schema version from metadata: %w", errVersion)
	case errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound) ||
		errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		// log warning once, to avoid spamming the logs
		d.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msg(`record does not have an attached schema for the key, consider disabling the destination schema key decoding using "sdk.schema.extract.key.enabled: false"`)
		})
		return nil
	}

	if rec.Key != nil {
		decodedKey, err := d.decode(ctx, rec.Key, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode key: %w", err)
		}
		rec.Key = decodedKey
	}

	return nil
}

func (d *destinationWithSchemaExtraction) decodePayload(ctx context.Context, rec *opencdc.Record) error {
	subject, errSubject := rec.Metadata.GetPayloadSchemaSubject()
	version, errVersion := rec.Metadata.GetPayloadSchemaVersion()
	switch {
	case errSubject != nil && !errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get payload schema subject from metadata: %w", errSubject)
	case errVersion != nil && !errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get payload schema version from metadata: %w", errVersion)
	case errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound) ||
		errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		// log warning once, to avoid spamming the logs
		d.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msg(`record does not have an attached schema for the payload, consider disabling the destination schema payload decoding using "sdk.schema.extract.payload.enabled: false"`)
		})
		return nil
	}

	if rec.Payload.Before != nil {
		decodedPayloadBefore, err := d.decode(ctx, rec.Payload.Before, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode payload.before: %w", err)
		}
		rec.Payload.Before = decodedPayloadBefore
	}
	if rec.Payload.After != nil {
		decodedPayloadAfter, err := d.decode(ctx, rec.Payload.After, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode payload.after: %w", err)
		}
		rec.Payload.After = decodedPayloadAfter
	}

	return nil
}

func (d *destinationWithSchemaExtraction) decode(ctx context.Context, data opencdc.Data, subject string, version int) (opencdc.StructuredData, error) {
	switch data := data.(type) {
	case opencdc.StructuredData:
		return data, nil // already decoded
	case opencdc.RawData: // let's decode it
	default:
		return nil, fmt.Errorf("unexpected data type %T", data)
	}

	sch, err := schema.Get(ctx, subject, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for %s:%d: %w", subject, version, err)
	}

	var structuredData opencdc.StructuredData
	err = sch.Unmarshal(data.Bytes(), &structuredData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal bytes with schema: %w", err)
	}

	return structuredData, nil
}
