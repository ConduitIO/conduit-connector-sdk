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
	"sort"
	"strconv"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"golang.org/x/time/rate"
)

// DestinationMiddleware wraps a Destination and adds functionality to it.
type DestinationMiddleware interface {
	Wrap(Destination) Destination
}

// DestinationMiddlewareOption can be used to change the behavior of the default
// destination middleware created with DefaultDestinationMiddleware.
type DestinationMiddlewareOption func(DestinationMiddleware)

// Available destination middleware options.
var (
	_ DestinationMiddlewareOption = DestinationWithBatchConfig{}.Apply
	_ DestinationMiddlewareOption = DestinationWithRecordFormatConfig{}.Apply
	_ DestinationMiddlewareOption = DestinationWithRateLimitConfig{}.Apply
)

// DefaultDestinationMiddleware returns a slice of middleware that should be
// added to all destinations unless there's a good reason not to.
func DefaultDestinationMiddleware(opts ...DestinationMiddlewareOption) []DestinationMiddleware {
	middleware := []DestinationMiddleware{
		&DestinationWithRateLimit{},
		&DestinationWithRecordFormat{},
		&DestinationWithBatch{},
	}
	// apply options to all middleware
	for _, m := range middleware {
		for _, opt := range opts {
			opt(m)
		}
	}
	return middleware
}

// DestinationWithMiddleware wraps the destination into the supplied middleware.
func DestinationWithMiddleware(d Destination, middleware ...DestinationMiddleware) Destination {
	for _, m := range middleware {
		d = m.Wrap(d)
	}
	return d
}

// -- DestinationWithBatch -----------------------------------------------------

const (
	configDestinationBatchSize  = "sdk.batch.size"
	configDestinationBatchDelay = "sdk.batch.delay"
)

type ctxKeyBatchEnabled struct{}

// DestinationWithBatchConfig is the configuration for the
// DestinationWithBatch middleware. Fields set to their zero value are
// ignored and will be set to the default value.
type DestinationWithBatchConfig struct {
	// BatchSize is the default value for the batch size.
	BatchSize int
	// BatchDelay is the default value for the batch delay.
	BatchDelay time.Duration
}

// Apply sets the default configuration for the DestinationWithBatch middleware.
// Apply can be used as a DestinationMiddlewareOption.
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
	// Batching is actually implemented in the plugin adapter because it is the
	// only place we have access to acknowledgments.
	// We need to signal back to the adapter that batching is enabled. We do
	// this by changing a pointer that is stored in the context. It's a bit
	// hacky, but the only way to propagate a value back to the adapter without
	// changing the interface.
	d.setBatchEnabled(ctx, true)

	// set defaults in the config, they will be visible to the caller as well
	if config[configDestinationBatchSize] == "" {
		config[configDestinationBatchSize] = strconv.Itoa(d.defaults.BatchSize)
	}
	if config[configDestinationBatchDelay] == "" {
		config[configDestinationBatchDelay] = d.defaults.BatchDelay.String()
	}

	return d.Destination.Configure(ctx, config)
}

// setBatchEnabled stores the boolean in the context. If the context already
// contains the key it will update the boolean under that key and return the
// same context, otherwise it will return a new context with the stored value.
// This is used to signal to destinationPluginAdapter if the Destination is
// wrapped into DestinationWithBatchConfig middleware.
func (*destinationWithBatch) setBatchEnabled(ctx context.Context, enabled bool) context.Context {
	flag, ok := ctx.Value(ctxKeyBatchEnabled{}).(*bool)
	if ok {
		*flag = enabled
	} else {
		ctx = context.WithValue(ctx, ctxKeyBatchEnabled{}, &enabled)
	}
	return ctx
}

func (*destinationWithBatch) getBatchEnabled(ctx context.Context) bool {
	flag, ok := ctx.Value(ctxKeyBatchEnabled{}).(*bool)
	if !ok {
		return false
	}
	return *flag
}

// -- DestinationWithRateLimit -------------------------------------------------

const (
	configDestinationRatePerSecond = "sdk.rate.perSecond"
	configDestinationRateBurst     = "sdk.rate.burst"
)

// DestinationWithRateLimitConfig is the configuration for the
// DestinationWithRateLimit middleware. Fields set to their zero value are
// ignored and will be set to the default value.
type DestinationWithRateLimitConfig struct {
	// RatePerSecond is the default value for the rate per second.
	RatePerSecond float64
	// Burst is the default value for the allowed burst count.
	Burst int
}

// Apply sets the default configuration for the DestinationWithRateLimit middleware.
// Apply can be used as a DestinationMiddlewareOption.
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
			Description: "Maximum times records can be written per second (0 means no rate limit).",
			Type:        config.ParameterTypeFloat,
		},
		configDestinationRateBurst: {
			Default:     strconv.Itoa(c.Burst),
			Description: "Allow bursts of at most X writes (1 or less means that bursts are not allowed). Only takes effect if a rate limit per second is set.",
			Type:        config.ParameterTypeInt,
		},
	}
}

// DestinationWithRateLimit adds support for rate limiting to the destination.
// It adds two parameters to the destination config:
//   - `sdk.rate.perSecond` - Maximum times the Write function can be called per
//     second (0 means no rate limit).
//   - `sdk.rate.burst` - Allow bursts of at most X writes (0 means that bursts
//     are not allowed).
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
			burst = 1 // non-positive numbers would prevent all writes, we don't allow that, we default it to 1
		}
		d.limiter = rate.NewLimiter(limit, burst)
	}

	return nil
}

func (d *destinationWithRateLimit) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	if d.limiter != nil {
		err := d.limiter.Wait(ctx)
		if err != nil {
			return 0, fmt.Errorf("rate limiter: %w", err)
		}
	}
	return d.Destination.Write(ctx, recs)
}

// -- DestinationWithRecordFormat ----------------------------------------------

const (
	configDestinationRecordFormat        = "sdk.record.format"
	configDestinationRecordFormatOptions = "sdk.record.format.options"
)

type DestinationWithRecordFormatConfig struct {
	// DefaultRecordFormat is the default record format.
	DefaultRecordFormat string
	RecordSerializers   []RecordSerializer
}

// Apply sets the default configuration for the DestinationWithRecordFormat middleware.
// Apply can be used as a DestinationMiddlewareOption.
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
