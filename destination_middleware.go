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

// DefaultDestinationMiddleware is a slice of middleware that should be added to
// all destinations unless there's a good reason not to.
var DefaultDestinationMiddleware = []DestinationMiddleware{
	DestinationWithRateLimit{},
	DestinationWithBatch{},
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

type ctxBatchFlag struct{}

// DestinationWithBatch enables batching on the destination. Batching is
// disabled by default.
type DestinationWithBatch struct {
	// TODO
	DefaultBatchSize int
	// TODO
	DefaultBatchDelay time.Duration
}

// Wrap a Destination into the batching middleware.
func (d DestinationWithBatch) Wrap(impl Destination) Destination {
	return &destinationWithBatch{
		Destination: impl,
		defaults:    d,
	}
}

func (DestinationWithBatch) setBatchFlag(ctx context.Context, enabled bool) context.Context {
	flag, ok := ctx.Value(ctxBatchFlag{}).(*bool)
	if ok {
		*flag = enabled
	} else {
		ctx = context.WithValue(ctx, ctxBatchFlag{}, &enabled)
	}
	return ctx
}
func (DestinationWithBatch) getBatchFlag(ctx context.Context) bool {
	flag, ok := ctx.Value(ctxBatchFlag{}).(*bool)
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
	return map[string]Parameter{
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
	}
}

func (d *destinationWithBatch) Configure(ctx context.Context, config map[string]string) error {
	// Batching is actually implemented in the plugin adapter because it is the
	// only place we have access to acknowledgments.
	// We need to signal back to the adapter that batching is enabled. We do
	// this by changing a pointer that is stored in the context. It's a bit
	// hacky, but the only way to propagate a value back to the adapter without
	// changing the interface.
	d.defaults.setBatchFlag(ctx, true)

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

// DestinationWithRateLimit TODO
type DestinationWithRateLimit struct {
	// TODO
	DefaultRateLimit float64 // writes per second
	// TODO
	DefaultBurst int // allow bursts of at most X writes
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
			Default:     strconv.FormatFloat(d.defaults.DefaultRateLimit, 'f', -1, 64),
			Required:    false,
			Description: "Maximum times the Write function can be called per second (0 means no rate limit).",
		},
		configDestinationRateBurst: {
			Default:     strconv.Itoa(d.defaults.DefaultBurst),
			Required:    false,
			Description: "Permit bursts of at most X writes (0 means that bursts are not permitted).",
		},
	})
}

func (d *destinationWithRateLimit) Configure(ctx context.Context, config map[string]string) error {
	err := d.Destination.Configure(ctx, config)
	if err != nil {
		return err
	}

	limit := rate.Limit(d.defaults.DefaultRateLimit)
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
	return d.Write(ctx, recs)
}
