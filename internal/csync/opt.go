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
	"time"
)

type Option interface {
	apply() // dummy method, real methods are in specific options
}

type ctxOption interface {
	Option
	applyCtx(context.Context) (context.Context, context.CancelFunc)
}

type ctxOptionFn func(ctx context.Context) (context.Context, context.CancelFunc)

func (c ctxOptionFn) apply() { /* noop */ }
func (c ctxOptionFn) applyCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return c(ctx)
}

func applyAndRemoveCtxOptions(ctx context.Context, opts []Option) (context.Context, context.CancelFunc, []Option) {
	if len(opts) == 0 {
		return ctx, func() {}, opts // shortcut
	}

	remainingOpts := make([]Option, 0, len(opts))
	var cancelFns []context.CancelFunc
	for _, opt := range opts {
		ctxOpt, ok := opt.(ctxOption)
		if !ok {
			remainingOpts = append(remainingOpts, opt)
			continue
		}

		var cancel context.CancelFunc
		ctx, cancel = ctxOpt.applyCtx(ctx)
		cancelFns = append(cancelFns, cancel)
	}
	return ctx, func() {
		// call cancel functions in reverse
		for i := len(cancelFns) - 1; i >= 0; i-- {
			cancelFns[i]()
		}
	}, remainingOpts
}

// WithTimeout cancels the operation after the timeout if it didn't succeed yet.
// The function returns context.DeadlineExceeded if the timeout is reached.
func WithTimeout(timeout time.Duration) Option {
	return ctxOptionFn(func(ctx context.Context) (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, timeout)
	})
}
