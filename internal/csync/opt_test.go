// Copyright © 2023 Meroxa, Inc.
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

package csync

import (
	"context"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-sdk/internal/cchan"
	"github.com/matryer/is"
)

type testOption struct {
	foo any
}

func (t testOption) apply() {}

func TestCtxOption_WithTimeout_DeadlineExceeded(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	opts := []Option{
		testOption{foo: "test"}, // not a context option
		WithTimeout(time.Millisecond),
	}

	gotCtx, gotCancel, gotOpts := applyAndRemoveCtxOptions(ctx, opts)
	is.True(ctx != gotCtx)
	is.True(gotCancel != nil)
	is.Equal(gotOpts, []Option{testOption{foo: "test"}})

	_, _, err := cchan.ChanOut[struct{}](gotCtx.Done()).RecvTimeout(ctx, time.Millisecond*100)
	is.NoErr(err)
	is.Equal(gotCtx.Err(), context.DeadlineExceeded)

	// running the cancel func should be a noop at this point, just testing if it panics
	gotCancel()
}

func TestCtxOption_WithTimeout_Cancel(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	opts := []Option{
		testOption{foo: "test"}, // not a context option
		WithTimeout(time.Second),
	}

	gotCtx, gotCancel, gotOpts := applyAndRemoveCtxOptions(ctx, opts)
	is.True(ctx != gotCtx)
	is.True(gotCancel != nil)
	is.Equal(gotOpts, []Option{testOption{foo: "test"}})

	_, _, err := cchan.ChanOut[struct{}](gotCtx.Done()).RecvTimeout(ctx, time.Millisecond*100)
	is.Equal(err, context.DeadlineExceeded)

	// running the cancel func should cancel the context now
	gotCancel()

	_, _, err = cchan.ChanOut[struct{}](gotCtx.Done()).RecvTimeout(ctx, time.Millisecond*100)
	is.NoErr(err)
	is.Equal(gotCtx.Err(), context.Canceled)
}

func TestCtxOption_Empty(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	gotCtx, gotCancel, gotOpts := applyAndRemoveCtxOptions(ctx, nil)
	is.Equal(ctx, gotCtx)
	is.True(gotCancel != nil)
	is.Equal(gotOpts, nil)

	gotCancel() // canceling the context shouldn't do anything

	_, _, err := cchan.ChanOut[struct{}](gotCtx.Done()).RecvTimeout(ctx, time.Millisecond*100)
	is.Equal(err, context.DeadlineExceeded)
}
