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
	"time"

	"golang.org/x/time/rate"
)

// DestinationMiddleware wraps a Destination and adds functionality to it.
type DestinationMiddleware interface {
	Wrap(Destination) Destination
}

// DefaultDestinationMiddleware returns a slice of middleware that should be
// added to all destinations unless there's a good reason not to.
func DefaultDestinationMiddleware() []DestinationMiddleware {
	return []DestinationMiddleware{
		DestinationWithRateLimit{},
		// DestinationWithBatch{}, // TODO enable batch middleware once batching is implemented
	}
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

// DestinationWithBatch adds support for batching on the destination. It adds
// two parameters to the destination config:
//   - `sdk.batch.size` - Maximum size of batch before it gets written to the
//     destination.
//   - `sdk.batch.delay` - Maximum delay before an incomplete batch is written
//     to the destination.
// To change the defaults of these parameters use the fields of this struct.
type DestinationWithBatch struct {
	// DefaultBatchSize is the default value for the batch size.
	DefaultBatchSize int
	// DefaultBatchDelay is the default value for the batch delay.
	DefaultBatchDelay time.Duration
}

// Wrap a Destination into the batching middleware.
func (d DestinationWithBatch) Wrap(impl Destination) Destination {
	return &destinationWithBatch{
		Destination: impl,
		defaults:    d,
	}
}

// setBatchEnabled stores the boolean in the context. If the context already
// contains the key it will update the boolean under that key and return the
// same context, otherwise it will return a new context with the stored value.
// This is used to signal to destinationPluginAdapter if the Destination is
// wrapped into DestinationWithBatch middleware.
func (DestinationWithBatch) setBatchEnabled(ctx context.Context, enabled bool) context.Context {
	flag, ok := ctx.Value(ctxKeyBatchEnabled{}).(*bool)
	if ok {
		*flag = enabled
	} else {
		ctx = context.WithValue(ctx, ctxKeyBatchEnabled{}, &enabled)
	}
	return ctx
}
func (DestinationWithBatch) getBatchEnabled(ctx context.Context) bool {
	flag, ok := ctx.Value(ctxKeyBatchEnabled{}).(*bool)
	if !ok {
		return false
	}
	return *flag
}

type destinationWithBatch struct {
	Destination
	defaults DestinationWithBatch
}

func (d *destinationWithBatch) Parameters() map[string]Parameter {
	return mergeParameters(d.Destination.Parameters(), map[string]Parameter{
		configDestinationBatchSize: {
			Default:     strconv.Itoa(d.defaults.DefaultBatchSize),
			Required:    false,
			Description: "Maximum size of batch before it gets written to the destination.",
		},
		configDestinationBatchDelay: {
			Default:     d.defaults.DefaultBatchDelay.String(),
			Required:    false,
			Description: "Maximum delay before an incomplete batch is written to the destination.",
		},
	})
}

func (d *destinationWithBatch) Configure(ctx context.Context, config map[string]string) error {
	// Batching is actually implemented in the plugin adapter because it is the
	// only place we have access to acknowledgments.
	// We need to signal back to the adapter that batching is enabled. We do
	// this by changing a pointer that is stored in the context. It's a bit
	// hacky, but the only way to propagate a value back to the adapter without
	// changing the interface.
	d.defaults.setBatchEnabled(ctx, true)

	// set defaults in the config, they will be visible to the caller as well
	if config[configDestinationBatchSize] == "" {
		config[configDestinationBatchSize] = strconv.Itoa(d.defaults.DefaultBatchSize)
	}
	if config[configDestinationBatchDelay] == "" {
		config[configDestinationBatchDelay] = d.defaults.DefaultBatchDelay.String()
	}

	return d.Destination.Configure(ctx, config)
}

// -- DestinationWithRateLimit -------------------------------------------------

const (
	configDestinationRatePerSecond = "sdk.rate.perSecond"
	configDestinationRateBurst     = "sdk.rate.burst"
)

// DestinationWithRateLimit adds support for rate limiting to the destination.
// It adds two parameters to the destination config:
//   - `sdk.rate.perSecond` - Maximum times the Write function can be called per
//     second (0 means no rate limit).
//   - `sdk.rate.burst` - Allow bursts of at most X writes (0 means that bursts
//     are not allowed).
// To change the defaults of these parameters use the fields of this struct.
type DestinationWithRateLimit struct {
	// DefaultRatePerSecond is the default value for the rate per second.
	DefaultRatePerSecond float64
	// DefaultBurst is the default value for the allowed burst count.
	DefaultBurst int
}

// Wrap a Destination into the rate limiting middleware.
func (d DestinationWithRateLimit) Wrap(impl Destination) Destination {
	return &destinationWithRateLimit{
		Destination: impl,
		defaults:    d,
	}
}

type destinationWithRateLimit struct {
	Destination

	defaults DestinationWithRateLimit
	limiter  *rate.Limiter
}

func (d *destinationWithRateLimit) Parameters() map[string]Parameter {
	return mergeParameters(d.Destination.Parameters(), map[string]Parameter{
		configDestinationRatePerSecond: {
			Default:     strconv.FormatFloat(d.defaults.DefaultRatePerSecond, 'f', -1, 64),
			Required:    false,
			Description: "Maximum times records can be written per second (0 means no rate limit).",
		},
		configDestinationRateBurst: {
			Default:     strconv.Itoa(d.defaults.DefaultBurst),
			Required:    false,
			Description: "Allow bursts of at most X writes (1 or less means that bursts are not allowed). Only takes effect if a rate limit per second is set.",
		},
	})
}

func (d *destinationWithRateLimit) Configure(ctx context.Context, config map[string]string) error {
	err := d.Destination.Configure(ctx, config)
	if err != nil {
		return err
	}

	limit := rate.Limit(d.defaults.DefaultRatePerSecond)
	burst := d.defaults.DefaultBurst

	limitRaw := config[configDestinationRatePerSecond]
	if limitRaw != "" {
		limitFloat, err := strconv.ParseFloat(limitRaw, 64)
		if err != nil {
			return fmt.Errorf("invalid sdk.rateLimit.writesPerSecond: %w", err)
		}
		limit = rate.Limit(limitFloat)
	}
	burstRaw := config[configDestinationRateBurst]
	if burstRaw != "" {
		burstInt, err := strconv.Atoi(burstRaw)
		if err != nil {
			return fmt.Errorf("invalid sdk.rateLimit.bursts: %w", err)
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

func (d *destinationWithRateLimit) Write(ctx context.Context, recs []Record) (int, error) {
	if d.limiter != nil {
		err := d.limiter.Wait(ctx)
		if err != nil {
			return 0, fmt.Errorf("rate limiter: %w", err)
		}
	}
	return d.Destination.Write(ctx, recs)
}
