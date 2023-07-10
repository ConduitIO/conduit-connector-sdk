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

package internal

import (
	"sync"
	"time"
)

type Batcher[T any] struct {
	sizeThreshold  int
	delayThreshold time.Duration
	fn             BatchFn[T]

	batch      []T
	results    []chan error
	flushTimer *time.Timer
	m          sync.Mutex
}

type BatchFn[T any] func([]T) error

type (
	EnqueueResult interface{ enqueueResult() }
	Scheduled     struct{ Err <-chan error }
	Flushed       struct{ Err error }
)

func (Scheduled) enqueueResult() {}
func (Flushed) enqueueResult()   {}

func NewBatcher[T any](sizeThreshold int, delayThreshold time.Duration, fn BatchFn[T]) *Batcher[T] {
	return &Batcher[T]{
		sizeThreshold:  sizeThreshold,
		delayThreshold: delayThreshold,
		fn:             fn,
	}
}

func (b *Batcher[T]) Enqueue(item T) EnqueueResult {
	b.m.Lock()
	defer b.m.Unlock()

	result := make(chan error, 1)
	b.batch = append(b.batch, item)
	b.results = append(b.results, result)

	if len(b.batch) == b.sizeThreshold {
		// trigger flush synchronously
		b.flushNow()
		return Flushed{Err: <-result}
	}
	if b.flushTimer == nil {
		b.flushTimer = time.AfterFunc(b.delayThreshold, b.Flush)
	}
	return Scheduled{Err: result}
}

func (b *Batcher[T]) Flush() {
	b.m.Lock()
	defer b.m.Unlock()
	b.flushNow()
}

func (b *Batcher[T]) flushNow() {
	if b.flushTimer != nil {
		b.flushTimer.Stop()
		b.flushTimer = nil
	}
	if len(b.batch) == 0 {
		// nothing to flush
		return
	}
	batchCopy := make([]T, len(b.batch))
	copy(batchCopy, b.batch)

	err := b.fn(batchCopy)
	for _, c := range b.results {
		c <- err
	}

	b.batch = b.batch[:0]
	b.results = b.results[:0]
}
