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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
)

func TestDestinationWithBatch_Parameters(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	d := (&DestinationWithBatch{}).Wrap(dst)

	want := config.Parameters{
		"foo": {
			Default:     "bar",
			Description: "baz",
		},
	}

	dst.EXPECT().Parameters().Return(want)
	got := d.Parameters()

	is.Equal(got["foo"], want["foo"])
	is.Equal(len(got), 3) // expected middleware to inject 2 parameters
}

func TestDestinationWithBatch_Configure(t *testing.T) {
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	testCases := []struct {
		name       string
		middleware DestinationWithBatch
		have       config.Config
		want       DestinationWithBatchConfig
	}{{
		name:       "empty config",
		middleware: DestinationWithBatch{},
		have:       config.Config{},
		want: DestinationWithBatchConfig{
			BatchSize:  0,
			BatchDelay: 0,
		},
	}, {
		name: "empty config, custom defaults",
		middleware: DestinationWithBatch{
			Config: DestinationWithBatchConfig{
				BatchSize:  5,
				BatchDelay: time.Second,
			},
		},
		have: config.Config{},
		want: DestinationWithBatchConfig{
			BatchSize:  5,
			BatchDelay: time.Second * 1,
		},
	}, {
		name: "config with values",
		middleware: DestinationWithBatch{
			Config: DestinationWithBatchConfig{
				BatchSize:  5,
				BatchDelay: time.Second,
			},
		},
		have: config.Config{
			configDestinationBatchSize:  "12",
			configDestinationBatchDelay: "2s",
		},
		want: DestinationWithBatchConfig{
			BatchSize:  12,
			BatchDelay: time.Second * 2,
		},
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			d := tt.middleware.Wrap(dst)

			ctx := (&destinationWithBatch{}).setBatchConfig(context.Background(), DestinationWithBatchConfig{})
			dst.EXPECT().Configure(ctx, gomock.AssignableToTypeOf(config.Config{})).Return(nil)

			err := d.Configure(ctx, tt.have)

			is.NoErr(err)
			is.Equal(tt.want, (&destinationWithBatch{}).getBatchConfig(ctx))
		})
	}
}

func TestDestinationWithRateLimit_Parameters(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	d := (&DestinationWithRateLimit{}).Wrap(dst)

	want := config.Parameters{
		"foo": {
			Default:     "bar",
			Description: "baz",
		},
	}

	dst.EXPECT().Parameters().Return(want)
	got := d.Parameters()

	is.Equal(got["foo"], want["foo"])
	is.Equal(len(got), 3) // expected middleware to inject 2 parameters
}

func TestDestinationWithRateLimit_Configure(t *testing.T) {
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)
	ctx := context.Background()

	testCases := []struct {
		name        string
		middleware  DestinationWithRateLimit
		have        config.Config
		wantLimiter bool
		wantLimit   rate.Limit
		wantBurst   int
	}{{
		name:        "empty config",
		middleware:  DestinationWithRateLimit{},
		have:        config.Config{},
		wantLimiter: false,
	}, {
		name: "empty config, custom defaults",
		middleware: DestinationWithRateLimit{
			Config: DestinationWithRateLimitConfig{
				RatePerSecond: 1.23,
				Burst:         4,
			},
		},
		have:        config.Config{},
		wantLimiter: true,
		wantLimit:   rate.Limit(1.23),
		wantBurst:   4,
	}, {
		name: "negative burst default",
		middleware: DestinationWithRateLimit{
			Config: DestinationWithRateLimitConfig{
				RatePerSecond: 1.23,
				Burst:         -2,
			},
		},
		have:        config.Config{},
		wantLimiter: true,
		wantLimit:   rate.Limit(1.23),
		wantBurst:   1, // burst will be at minimum 1
	}, {
		name: "config with values",
		middleware: DestinationWithRateLimit{
			Config: DestinationWithRateLimitConfig{
				RatePerSecond: 1.23,
				Burst:         4,
			},
		},
		have: config.Config{
			configDestinationRatePerSecond: "12.34",
			configDestinationRateBurst:     "5",
		},
		wantLimiter: true,
		wantLimit:   rate.Limit(12.34),
		wantBurst:   5,
	}, {
		name: "config with zero burst",
		middleware: DestinationWithRateLimit{
			Config: DestinationWithRateLimitConfig{
				RatePerSecond: 1.23,
				Burst:         4,
			},
		},
		have: config.Config{
			configDestinationRateBurst: "0",
		},
		wantLimiter: true,
		wantLimit:   rate.Limit(1.23),
		wantBurst:   1, // burst will be at minimum 1
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			d := tt.middleware.Wrap(dst).(*destinationWithRateLimit)

			dst.EXPECT().Configure(ctx, tt.have).Return(nil)

			err := d.Configure(ctx, tt.have)
			is.NoErr(err)

			if !tt.wantLimiter {
				is.Equal(d.limiter, nil)
			} else {
				is.Equal(d.limiter.Limit(), tt.wantLimit)
				is.Equal(d.limiter.Burst(), tt.wantBurst)
			}
		})
	}
}

func TestDestinationWithRateLimit_Write(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)
	ctx := context.Background()

	d := (&DestinationWithRateLimit{}).Wrap(dst)

	dst.EXPECT().Configure(ctx, gomock.Any()).Return(nil)

	err := d.Configure(ctx, config.Config{
		configDestinationRatePerSecond: "10",
		configDestinationRateBurst:     "2",
	})
	is.NoErr(err)

	recs := []opencdc.Record{{}, {}}

	const tolerance = time.Millisecond * 10

	expectWriteAfter := func(delay time.Duration) {
		start := time.Now()
		dst.EXPECT().Write(ctx, recs).DoAndReturn(func(context.Context, []opencdc.Record) (int, error) {
			dur := time.Since(start)
			diff := dur - delay
			if diff < 0 {
				diff = -diff
			}
			if diff > tolerance {
				t.Fatalf("expected delay: %s, actual delay: %s, tolerance: %s", delay, dur, tolerance)
			}
			return len(recs), nil
		})
	}

	// first write should happen right away
	expectWriteAfter(0)
	_, err = d.Write(ctx, recs)
	is.NoErr(err)

	// second write happens right away because we allow bursts of 2
	expectWriteAfter(0)
	_, err = d.Write(ctx, recs)
	is.NoErr(err)

	// third write needs to wait for 100ms
	expectWriteAfter(100 * time.Millisecond)
	_, err = d.Write(ctx, recs)
	is.NoErr(err)
}

func TestDestinationWithRateLimit_Write_CancelledContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	d := (&DestinationWithRateLimit{}).Wrap(dst)

	ctx, cancel := context.WithCancel(context.Background())

	dst.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
	err := d.Configure(ctx, config.Config{
		configDestinationRatePerSecond: "10",
	})
	is.NoErr(err)

	cancel()
	_, err = d.Write(ctx, []opencdc.Record{{}})
	is.True(errors.Is(err, ctx.Err()))
}

func TestDestinationWithRecordFormat_Configure(t *testing.T) {
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)
	ctx := context.Background()

	testCases := []struct {
		name           string
		middleware     DestinationWithRecordFormat
		have           config.Config
		wantSerializer RecordSerializer
	}{{
		name:           "empty config",
		middleware:     DestinationWithRecordFormat{},
		have:           config.Config{},
		wantSerializer: defaultSerializer,
	}, {
		name:       "valid config",
		middleware: DestinationWithRecordFormat{},
		have: config.Config{
			configDestinationRecordFormat: "debezium/json",
		},
		wantSerializer: GenericRecordSerializer{
			Converter: DebeziumConverter{
				RawDataKey: debeziumDefaultRawDataKey,
			},
			Encoder: JSONEncoder{},
		},
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			d := tt.middleware.Wrap(dst).(*destinationWithRecordFormat)

			dst.EXPECT().Configure(ctx, tt.have).Return(nil)

			err := d.Configure(ctx, tt.have)
			is.NoErr(err)

			is.Equal(d.serializer, tt.wantSerializer)
		})
	}
}
