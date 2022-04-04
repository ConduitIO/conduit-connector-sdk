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

package internal

import (
	"context"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestDetachContext(t *testing.T) {
	is := is.New(t)

	type key struct{}
	wantValue := "foo"

	assertDetachedContext := func(ctx context.Context) {
		is.NoErr(ctx.Err())
		is.Equal(ctx.Done(), nil)
		deadline, ok := ctx.Deadline()
		is.Equal(deadline, time.Time{})
		is.True(!ok)
		gotValue := ctx.Value(key{})
		is.Equal(gotValue, wantValue)
	}

	// prepare context with deadline and value
	ctx := context.Background()
	ctx = context.WithValue(ctx, key{}, wantValue)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second))
	defer cancel()

	// detach context and assert it is detached
	detachedCtx := DetachContext(ctx)
	assertDetachedContext(detachedCtx)

	// cancel parent context and assert again
	cancel()
	assertDetachedContext(detachedCtx)
}
