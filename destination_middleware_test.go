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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestDestinationWithBatch_Parameters(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dwb := DestinationWithBatch{}.Wrap(dst)

	want := map[string]Parameter{
		"foo": {
			Default:     "bar",
			Required:    true,
			Description: "baz",
		},
	}

	dst.EXPECT().Parameters().Return(want)
	got := dwb.Parameters()

	is.Equal(got["foo"], want["foo"])
	is.Equal(len(got), 3) // expected destination with batch to inject 2 parameters
}

func TestDestinationWithBatch_Configure_Default(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	testCases := []struct {
		name string
		dwb  DestinationWithBatch
		have map[string]string
		want map[string]string
	}{{
		name: "empty config",
		dwb:  DestinationWithBatch{},
		have: map[string]string{},
		want: map[string]string{
			configDestinationBatchSize:  "0",
			configDestinationBatchDelay: "0s",
		},
	}, {
		name: "empty config, different defaults",
		dwb: DestinationWithBatch{
			DefaultBatchSize:  5,
			DefaultBatchDelay: time.Second,
		},
		have: map[string]string{},
		want: map[string]string{
			configDestinationBatchSize:  "5",
			configDestinationBatchDelay: "1s",
		},
	}, {
		name: "config with values",
		dwb: DestinationWithBatch{
			DefaultBatchSize:  5,
			DefaultBatchDelay: time.Second,
		},
		have: map[string]string{
			configDestinationBatchSize:  "12",
			configDestinationBatchDelay: "2s",
		},
		want: map[string]string{
			configDestinationBatchSize:  "12",
			configDestinationBatchDelay: "2s",
		},
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			d := tt.dwb.Wrap(dst)

			ctx := DestinationWithBatch{}.setBatchEnabled(context.Background(), false)
			dst.EXPECT().Configure(ctx, gomock.AssignableToTypeOf(map[string]string{})).Return(nil)

			err := d.Configure(ctx, tt.have)

			is.NoErr(err)
			is.Equal(tt.have, tt.want) // expceted Configure to inject default parameters
			is.True(DestinationWithBatch{}.getBatchEnabled(ctx))
		})
	}
}
