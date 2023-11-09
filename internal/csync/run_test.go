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

	"github.com/matryer/is"
)

func TestRun_Success(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	var executed bool
	err := Run(ctx, func() { executed = true })
	is.NoErr(err)
	is.True(executed)
}

func TestRun_Canceled(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// run function that blocks for 1 second
	err := Run(ctx, func() { <-time.After(time.Second) })
	is.Equal(err, context.Canceled)
}

func TestRun_DeadlineReached(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	start := time.Now()
	err := Run(ctx, func() { <-time.After(time.Second) }, WithTimeout(time.Millisecond*100))
	since := time.Since(start)

	is.Equal(err, context.DeadlineExceeded)
	is.True(since >= time.Millisecond*100)
}
