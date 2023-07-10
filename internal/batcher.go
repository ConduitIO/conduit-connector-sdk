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
	results        chan BatchResult

	batch      []T
	flushTimer *time.Timer
	m          sync.Mutex
}

type BatchFn[T any] func([]T) error

type BatchResult struct {
	At   time.Time
	Size int
	Err  error
}

type EnqueueStatus int

const (
	Scheduled EnqueueStatus = iota + 1
	Flushed
)

func NewBatcher[T any](sizeThreshold int, delayThreshold time.Duration, fn BatchFn[T]) *Batcher[T] {
	return &Batcher[T]{
		sizeThreshold:  sizeThreshold,
		delayThreshold: delayThreshold,
		fn:             fn,
		results:        make(chan BatchResult, 1),
	}
}

func (b *Batcher[T]) Results() <-chan BatchResult {
	return b.results
}

func (b *Batcher[T]) Enqueue(item T) EnqueueStatus {
	b.m.Lock()
	defer b.m.Unlock()

	b.batch = append(b.batch, item)

	if len(b.batch) == b.sizeThreshold {
		// trigger flush synchronously
		b.flushNow()
		return Flushed
	}
	if b.flushTimer == nil {
		b.flushTimer = time.AfterFunc(b.delayThreshold, b.Flush)
	}
	return Scheduled
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

	at := time.Now()
	err := b.fn(batchCopy)
	r := BatchResult{
		At:   at,
		Size: len(batchCopy),
		Err:  err,
	}
	b.results <- r
	b.batch = b.batch[:0]
}
