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
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"golang.org/x/time/rate"
)

// DestinationMiddleware wraps a Destination and adds functionality to it.
type DestinationMiddleware interface {
	Wrap(Destination) Destination
}

// DestinationMiddlewareOption can be used to change the behavior of the default
// destination middleware created with DefaultDestinationMiddleware.
type DestinationMiddlewareOption interface {
	Apply(DestinationMiddleware)
}

// Available destination middleware options.
var (
	_ DestinationMiddlewareOption = DestinationWithBatchConfig{}
	_ DestinationMiddlewareOption = DestinationWithRecordFormatConfig{}
	_ DestinationMiddlewareOption = DestinationWithRateLimitConfig{}
	_ DestinationMiddlewareOption = DestinationWithSchemaExtractionConfig{}
)

// DefaultDestinationMiddleware returns a slice of middleware that should be
// added to all destinations unless there's a good reason not to.
func DefaultDestinationMiddleware(opts ...DestinationMiddlewareOption) []DestinationMiddleware {
	middleware := []DestinationMiddleware{
		&DestinationWithRateLimit{},
		&DestinationWithRecordFormat{},
		&DestinationWithBatch{},
		&DestinationWithSchemaExtraction{},
	}
	// apply options to all middleware
	for _, m := range middleware {
		for _, opt := range opts {
			opt.Apply(m)
		}
	}
	return middleware
}

// DestinationWithMiddleware wraps the destination into the supplied middleware.
func DestinationWithMiddleware(d Destination, middleware ...DestinationMiddleware) Destination {
	// apply middleware in reverse order to preserve the order as specified
	for i := len(middleware) - 1; i >= 0; i-- {
		d = middleware[i].Wrap(d)
	}
	return d
}

// -- DestinationWithBatch -----------------------------------------------------

const (
	configDestinationBatchSize  = "sdk.batch.size"
	configDestinationBatchDelay = "sdk.batch.delay"
)

type ctxKeyBatchConfig struct{}

// DestinationWithBatchConfig is the configuration for the
// DestinationWithBatch middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// DestinationWithBatchConfig can be used as a DestinationMiddlewareOption.
type DestinationWithBatchConfig struct {
	// BatchSize is the default value for the batch size.
	BatchSize int
	// BatchDelay is the default value for the batch delay.
	BatchDelay time.Duration
}

// Apply sets the default configuration for the DestinationWithBatch middleware.
func (c DestinationWithBatchConfig) Apply(m DestinationMiddleware) {
	if d, ok := m.(*DestinationWithBatch); ok {
		d.Config = c
	}
}

func (c DestinationWithBatchConfig) BatchSizeParameterName() string {
	return configDestinationBatchSize
}

func (c DestinationWithBatchConfig) BatchDelayParameterName() string {
	return configDestinationBatchDelay
}

func (c DestinationWithBatchConfig) parameters() config.Parameters {
	return config.Parameters{
		configDestinationBatchSize: {
			Default:     strconv.Itoa(c.BatchSize),
			Description: "Maximum size of batch before it gets written to the destination.",
			Type:        config.ParameterTypeInt,
		},
		configDestinationBatchDelay: {
			Default:     c.BatchDelay.String(),
			Description: "Maximum delay before an incomplete batch is written to the destination.",
			Type:        config.ParameterTypeDuration,
		},
	}
}

// DestinationWithBatch adds support for batching on the destination. It adds
// two parameters to the destination config:
//   - `sdk.batch.size` - Maximum size of batch before it gets written to the
//     destination.
//   - `sdk.batch.delay` - Maximum delay before an incomplete batch is written
//     to the destination.
//
// To change the defaults of these parameters use the fields of this struct.
type DestinationWithBatch struct {
	Config DestinationWithBatchConfig
}

// Wrap a Destination into the batching middleware.
func (d *DestinationWithBatch) Wrap(impl Destination) Destination {
	return &destinationWithBatch{
		Destination: impl,
		defaults:    d.Config,
	}
}

type destinationWithBatch struct {
	Destination
	defaults DestinationWithBatchConfig
}

func (d *destinationWithBatch) Parameters() config.Parameters {
	return mergeParameters(d.Destination.Parameters(), d.defaults.parameters())
}

func (d *destinationWithBatch) Configure(ctx context.Context, config config.Config) error {
	err := d.Destination.Configure(ctx, config)
	if err != nil {
		return err
	}

	cfg := d.defaults

	if batchSizeRaw := config[configDestinationBatchSize]; batchSizeRaw != "" {
		batchSizeInt, err := strconv.Atoi(batchSizeRaw)
		if err != nil {
			return fmt.Errorf("invalid %q: %w", configDestinationBatchSize, err)
		}
		cfg.BatchSize = batchSizeInt
	}

	if delayRaw := config[configDestinationBatchDelay]; delayRaw != "" {
		delayDur, err := time.ParseDuration(delayRaw)
		if err != nil {
			return fmt.Errorf("invalid %q: %w", configDestinationBatchDelay, err)
		}
		cfg.BatchDelay = delayDur
	}

	if cfg.BatchSize < 0 {
		return fmt.Errorf("invalid %q: must not be negative", configDestinationBatchSize)
	}
	if cfg.BatchDelay < 0 {
		return fmt.Errorf("invalid %q: must not be negative", configDestinationBatchDelay)
	}

	// Batching is actually implemented in the plugin adapter because it is the
	// only place we have access to acknowledgments.
	// We need to signal back to the adapter that batching is enabled. We do
	// this by changing a pointer that is stored in the context. It's a bit
	// hacky, but the only way to propagate a value back to the adapter without
	// changing the interface.
	d.setBatchConfig(ctx, cfg)

	return nil
}

// setBatchEnabled stores the boolean in the context. If the context already
// contains the key it will update the boolean under that key and return the
// same context, otherwise it will return a new context with the stored value.
// This is used to signal to destinationPluginAdapter if the Destination is
// wrapped into DestinationWithBatchConfig middleware.
func (*destinationWithBatch) setBatchConfig(ctx context.Context, cfg DestinationWithBatchConfig) context.Context {
	ctxCfg, ok := ctx.Value(ctxKeyBatchConfig{}).(*DestinationWithBatchConfig)
	if ok {
		*ctxCfg = cfg
	} else {
		ctx = context.WithValue(ctx, ctxKeyBatchConfig{}, &cfg)
	}
	return ctx
}

func (*destinationWithBatch) getBatchConfig(ctx context.Context) DestinationWithBatchConfig {
	ctxCfg, ok := ctx.Value(ctxKeyBatchConfig{}).(*DestinationWithBatchConfig)
	if !ok {
		return DestinationWithBatchConfig{}
	}
	return *ctxCfg
}

// -- DestinationWithRateLimit -------------------------------------------------

const (
	configDestinationRatePerSecond = "sdk.rate.perSecond"
	configDestinationRateBurst     = "sdk.rate.burst"
)

// DestinationWithRateLimitConfig is the configuration for the
// DestinationWithRateLimit middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// DestinationWithRateLimitConfig can be used as a DestinationMiddlewareOption.
type DestinationWithRateLimitConfig struct {
	// RatePerSecond is the default value for the rate per second.
	RatePerSecond float64
	// Burst is the default value for the allowed burst count.
	Burst int
}

// Apply sets the default configuration for the DestinationWithRateLimit middleware.
func (c DestinationWithRateLimitConfig) Apply(m DestinationMiddleware) {
	if d, ok := m.(*DestinationWithRateLimit); ok {
		d.Config = c
	}
}

func (c DestinationWithRateLimitConfig) RatePerSecondParameterName() string {
	return configDestinationRatePerSecond
}

func (c DestinationWithRateLimitConfig) RateBurstParameterName() string {
	return configDestinationRateBurst
}

func (c DestinationWithRateLimitConfig) parameters() config.Parameters {
	return config.Parameters{
		configDestinationRatePerSecond: {
			Default:     strconv.FormatFloat(c.RatePerSecond, 'f', -1, 64),
			Description: "Maximum number of records written per second (0 means no rate limit).",
			Type:        config.ParameterTypeFloat,
		},
		configDestinationRateBurst: {
			Default:     strconv.Itoa(c.Burst),
			Description: "Allow bursts of at most X records (0 or less means that bursts are not limited). Only takes effect if a rate limit per second is set. Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the effective batch size will be equal to `sdk.rate.burst`.",
			Type:        config.ParameterTypeInt,
		},
	}
}

// DestinationWithRateLimit adds support for rate limiting to the destination.
// It adds two parameters to the destination config:
//   - `sdk.rate.perSecond` - Maximum number of records written per second (0
//     means no rate limit).
//   - `sdk.rate.burst` - Allow bursts of at most X records (0 or less means
//     that bursts are not limited). Only takes effect if a rate limit per
//     second is set. Note that if `sdk.batch.size` is bigger than
//     `sdk.rate.burst`, the effective batch size will be equal to `sdk.rate.burst`.
//
// To change the defaults of these parameters use the fields of this struct.
type DestinationWithRateLimit struct {
	Config DestinationWithRateLimitConfig
}

// Wrap a Destination into the rate limiting middleware.
func (d *DestinationWithRateLimit) Wrap(impl Destination) Destination {
	return &destinationWithRateLimit{
		Destination: impl,
		defaults:    d.Config,
	}
}

type destinationWithRateLimit struct {
	Destination

	defaults DestinationWithRateLimitConfig
	limiter  *rate.Limiter
}

func (d *destinationWithRateLimit) Parameters() config.Parameters {
	return mergeParameters(d.Destination.Parameters(), d.defaults.parameters())
}

func (d *destinationWithRateLimit) Configure(ctx context.Context, config config.Config) error {
	err := d.Destination.Configure(ctx, config)
	if err != nil {
		return err
	}

	limit := rate.Limit(d.defaults.RatePerSecond)
	burst := d.defaults.Burst

	limitRaw := config[configDestinationRatePerSecond]
	if limitRaw != "" {
		limitFloat, err := strconv.ParseFloat(limitRaw, 64)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", configDestinationRatePerSecond, err)
		}
		limit = rate.Limit(limitFloat)
	}
	burstRaw := config[configDestinationRateBurst]
	if burstRaw != "" {
		burstInt, err := strconv.Atoi(burstRaw)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", configDestinationRateBurst, err)
		}
		burst = burstInt
	}

	if limit > 0 {
		if burst <= 0 {
			// non-positive numbers would prevent all writes, we default it to
			// be the same size as the per second limit
			burst = int(math.Ceil(float64(limit)))
		}
		d.limiter = rate.NewLimiter(limit, burst)
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

const (
	configDestinationRecordFormat        = "sdk.record.format"
	configDestinationRecordFormatOptions = "sdk.record.format.options"
)

// DestinationWithRecordFormatConfig is the configuration for the
// DestinationWithRecordFormat middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// DestinationWithRecordFormatConfig can be used as a DestinationMiddlewareOption.
type DestinationWithRecordFormatConfig struct {
	// DefaultRecordFormat is the default record format.
	DefaultRecordFormat string
	RecordSerializers   []RecordSerializer
}

// Apply sets the default configuration for the DestinationWithRecordFormat middleware.
func (c DestinationWithRecordFormatConfig) Apply(m DestinationMiddleware) {
	if d, ok := m.(*DestinationWithRecordFormat); ok {
		d.Config = c
	}
}

func (c DestinationWithRecordFormatConfig) RecordFormatParameterName() string {
	return configDestinationRecordFormat
}

func (c DestinationWithRecordFormatConfig) RecordFormatOptionsParameterName() string {
	return configDestinationRecordFormatOptions
}

// DefaultRecordSerializers returns the list of record serializers that are used
// if DestinationWithRecordFormatConfig.RecordSerializers is nil.
func (c DestinationWithRecordFormatConfig) DefaultRecordSerializers() []RecordSerializer {
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

	for _, c := range genericConverters {
		for _, e := range genericEncoders {
			serializers = append(
				serializers,
				GenericRecordSerializer{
					Converter: c,
					Encoder:   e,
				},
			)
		}
	}
	return serializers
}

func (c DestinationWithRecordFormatConfig) parameters() config.Parameters {
	return config.Parameters{
		configDestinationRecordFormat: {
			Default:     c.DefaultRecordFormat,
			Description: "The format of the output record.",
			Validations: []config.Validation{
				config.ValidationInclusion{List: c.formats()},
			},
		},
		configDestinationRecordFormatOptions: {
			Description: "Options to configure the chosen output record format. Options are key=value pairs separated with comma (e.g. opt1=val2,opt2=val2).",
		},
	}
}

func (c DestinationWithRecordFormatConfig) formats() []string {
	names := make([]string, len(c.RecordSerializers))
	i := 0
	for _, c := range c.RecordSerializers {
		names[i] = c.Name()
		i++
	}
	return names
}

// DestinationWithRecordFormat adds support for changing the output format of
// records, specifically of the Record.Bytes method. It adds two parameters to
// the destination config:
//   - `sdk.record.format` - The format of the output record. The inclusion
//     validation exposes a list of valid options.
//   - `sdk.record.format.options` - Options are used to configure the format.
type DestinationWithRecordFormat struct {
	Config DestinationWithRecordFormatConfig
}

// Wrap a Destination into the record format middleware.
func (d *DestinationWithRecordFormat) Wrap(impl Destination) Destination {
	if d.Config.DefaultRecordFormat == "" {
		d.Config.DefaultRecordFormat = defaultSerializer.Name()
	}
	if len(d.Config.RecordSerializers) == 0 {
		d.Config.RecordSerializers = d.Config.DefaultRecordSerializers()
	}

	// sort record serializers by name to ensure we can binary search them
	sort.Slice(
		d.Config.RecordSerializers,
		func(i, j int) bool {
			return d.Config.RecordSerializers[i].Name() < d.Config.RecordSerializers[j].Name()
		},
	)

	return &destinationWithRecordFormat{
		Destination: impl,
		defaults:    d.Config,
	}
}

type destinationWithRecordFormat struct {
	Destination
	defaults DestinationWithRecordFormatConfig

	serializer RecordSerializer
}

func (d *destinationWithRecordFormat) Parameters() config.Parameters {
	return mergeParameters(d.Destination.Parameters(), d.defaults.parameters())
}

func (d *destinationWithRecordFormat) Configure(ctx context.Context, config config.Config) error {
	err := d.Destination.Configure(ctx, config)
	if err != nil {
		return err
	}

	format := d.defaults.DefaultRecordFormat
	if f, ok := config[configDestinationRecordFormat]; ok {
		format = f
	}

	i := sort.SearchStrings(d.defaults.formats(), format)
	// if the string is not found i is equal to the size of the slice
	if i == len(d.defaults.RecordSerializers) {
		return fmt.Errorf("invalid %s: %q not found in %v", configDestinationRecordFormat, format, d.defaults.formats())
	}

	serializer := d.defaults.RecordSerializers[i]
	serializer, err = serializer.Configure(config[configDestinationRecordFormatOptions])
	if err != nil {
		return fmt.Errorf("invalid %s for %q: %w", configDestinationRecordFormatOptions, format, err)
	}

	d.serializer = serializer
	return nil
}

func (d *destinationWithRecordFormat) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	for i := range recs {
		recs[i].SetSerializer(d.serializer)
	}
	return d.Destination.Write(ctx, recs)
}

// -- DestinationWithSchemaExtraction ------------------------------------------

const (
	configDestinationWithSchemaExtractionPayloadEnabled = "sdk.schema.extract.payload.enabled"
	configDestinationWithSchemaExtractionKeyEnabled     = "sdk.schema.extract.key.enabled"
)

// DestinationWithSchemaExtractionConfig is the configuration for the
// DestinationWithSchemaExtraction middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// DestinationWithSchemaExtractionConfig can be used as a DestinationMiddlewareOption.
type DestinationWithSchemaExtractionConfig struct {
	// Whether to extract and decode the record payload with a schema.
	// If unset, defaults to true.
	PayloadEnabled *bool
	// Whether to extract and decode the record key with a schema.
	// If unset, defaults to true.
	KeyEnabled *bool
}

// Apply sets the default configuration for the DestinationWithSchemaExtraction middleware.
func (c DestinationWithSchemaExtractionConfig) Apply(m DestinationMiddleware) {
	if d, ok := m.(*DestinationWithSchemaExtraction); ok {
		d.Config = c
	}
}

func (c DestinationWithSchemaExtractionConfig) SchemaPayloadEnabledParameterName() string {
	return configDestinationWithSchemaExtractionPayloadEnabled
}

func (c DestinationWithSchemaExtractionConfig) SchemaKeyEnabledParameterName() string {
	return configDestinationWithSchemaExtractionKeyEnabled
}

func (c DestinationWithSchemaExtractionConfig) parameters() config.Parameters {
	return config.Parameters{
		configDestinationWithSchemaExtractionKeyEnabled: {
			Default:     strconv.FormatBool(*c.KeyEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and decode the record key with a schema.",
		},
		configDestinationWithSchemaExtractionPayloadEnabled: {
			Default:     strconv.FormatBool(*c.PayloadEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to extract and decode the record payload with a schema.",
		},
	}
}

// DestinationWithSchemaExtraction is a middleware that extracts and decodes the
// key and/or payload of a record using a schema. It takes the schema subject and
// version from the record metadata, fetches the schema from the schema service,
// and decodes the key and/or payload using the schema.
// If the schema subject and version is not found in the record metadata, it will
// log a warning and skip decoding the key and/or payload. This middleware is
// useful when the source connector sends the data with the schema attached.
// This middleware is the counterpart of SourceWithSchemaExtraction.
//
// It adds two parameters to the destination config:
//   - `sdk.schema.extract.key.enabled` - Whether to extract and decode the
//     record key with a schema.
//   - `sdk.schema.extract.payload.enabled` - Whether to extract and decode the
//     record payload with a schema.
type DestinationWithSchemaExtraction struct {
	Config DestinationWithSchemaExtractionConfig
}

// Wrap a Destination into the schema middleware. It will apply default configuration
// values if they are not explicitly set.
func (s *DestinationWithSchemaExtraction) Wrap(impl Destination) Destination {
	if s.Config.KeyEnabled == nil {
		s.Config.KeyEnabled = ptr(true)
	}
	if s.Config.PayloadEnabled == nil {
		s.Config.PayloadEnabled = ptr(true)
	}
	return &destinationWithSchemaExtraction{
		Destination: impl,
		defaults:    s.Config,
	}
}

// destinationWithSchemaExtraction is the actual middleware implementation.
type destinationWithSchemaExtraction struct {
	Destination
	defaults DestinationWithSchemaExtractionConfig

	payloadEnabled bool
	keyEnabled     bool

	payloadWarnOnce sync.Once
	keyWarnOnce     sync.Once
}

func (d *destinationWithSchemaExtraction) Parameters() config.Parameters {
	return mergeParameters(d.Destination.Parameters(), d.defaults.parameters())
}

func (d *destinationWithSchemaExtraction) Configure(ctx context.Context, config config.Config) error {
	err := d.Destination.Configure(ctx, config)
	if err != nil {
		return err
	}

	d.keyEnabled = *d.defaults.KeyEnabled
	if val, ok := config[configDestinationWithSchemaExtractionKeyEnabled]; ok {
		d.keyEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configDestinationWithSchemaExtractionKeyEnabled, err)
		}
	}

	d.payloadEnabled = *d.defaults.PayloadEnabled
	if val, ok := config[configDestinationWithSchemaExtractionPayloadEnabled]; ok {
		d.payloadEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configDestinationWithSchemaExtractionPayloadEnabled, err)
		}
	}

	return nil
}

func (d *destinationWithSchemaExtraction) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	if d.keyEnabled {
		for i := range records {
			if err := d.decodeKey(ctx, &records[i]); err != nil {
				if len(records) > 0 {
					err = fmt.Errorf("record %d: %w", i, err)
				}
				return 0, err
			}
		}
	}
	if d.payloadEnabled {
		for i := range records {
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
			Logger(ctx).Warn().Msgf(`record does not have an attached schema for the key, consider disabling the destination schema key decoding using "%s: false"`, configDestinationWithSchemaExtractionKeyEnabled)
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
			Logger(ctx).Warn().Msgf(`record does not have an attached schema for the payload, consider disabling the destination schema payload decoding using "%s: false"`, configDestinationWithSchemaExtractionPayloadEnabled)
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
