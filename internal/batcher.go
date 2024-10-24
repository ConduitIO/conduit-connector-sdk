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

	batchSize  int
	batch      []T
	flushTimer *time.Timer
	m          sync.Mutex
}

type BatchFn[T any] func([]T, int) error

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

func (b *Batcher[T]) Enqueue(item T, size int) EnqueueStatus {
	b.m.Lock()
	defer b.m.Unlock()

	b.batch = append(b.batch, item)
	b.batchSize += size

	if b.batchSize == b.sizeThreshold {
		// trigger flush synchronously
		_ = b.flushNow()
		return Flushed
	}
	if b.flushTimer == nil && b.delayThreshold > 0 {
		b.flushTimer = time.AfterFunc(b.delayThreshold, func() { b.Flush() })
	}
	return Scheduled
}

func (b *Batcher[T]) Flush() bool {
	b.m.Lock()
	defer b.m.Unlock()
	return b.flushNow()
}

func (b *Batcher[T]) flushNow() bool {
	if b.flushTimer != nil {
		b.flushTimer.Stop()
		b.flushTimer = nil
	}
	if b.batchSize == 0 {
		// nothing to flush
		return false
	}

	at := time.Now()
	err := b.fn(b.batch, b.batchSize)
	r := BatchResult{
		At:   at,
		Size: b.batchSize,
		Err:  err,
	}
	b.results <- r
	b.batch = b.batch[:0]
	b.batchSize = 0
	return true
}
