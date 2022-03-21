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

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestSourcePluginAdapter_Start_OpenContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	var gotCtx context.Context
	src.EXPECT().Open(gomock.Any(), Position(nil)).
		DoAndReturn(func(ctx context.Context, _ Position) error {
			gotCtx = ctx // assign to gotCtx so it can be inspected
			return ctx.Err()
		})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{})
	is.NoErr(err)
	is.NoErr(gotCtx.Err()) // expected context to be open

	// even if we cancel the context afterwards, the context in Open should stay open
	cancel()
	is.NoErr(gotCtx.Err()) // expected context to be open
}

func TestSourcePluginAdapter_Start_ClosedContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	var gotCtx context.Context
	src.EXPECT().Open(gomock.Any(), Position(nil)).
		DoAndReturn(func(ctx context.Context, _ Position) error {
			gotCtx = ctx // assign to gotCtx so it can be inspected
			select {
			case <-ctx.Done():
				return ctx.Err() // that's expected
			case <-time.After(time.Millisecond * 10):
				is.Fail() // didn't see context getting closed in time
				return nil
			}
		})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{})
	is.True(err != nil)
	is.Equal(err, ctx.Err())
	is.Equal(gotCtx.Err(), context.Canceled)
}
