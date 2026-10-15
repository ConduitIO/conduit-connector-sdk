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
	"errors"
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

// Results returns the channel that carries the result of each flush. It holds
// one result. A flush never waits for it to be received: if the previous
// result is still there, the two are combined into one (see deliver), so a
// result can cover more than one flush.
func (b *Batcher[T]) Results() <-chan BatchResult {
	return b.results
}

func (b *Batcher[T]) Enqueue(item T, size int) EnqueueStatus {
	b.m.Lock()
	defer b.m.Unlock()

	b.batch = append(b.batch, item)
	b.batchSize += size

	if b.batchSize >= b.sizeThreshold {
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
	b.deliver(r)
	b.batch = b.batch[:0]
	b.batchSize = 0
	return true
}

// deliver puts r on the results channel without blocking. It is called with
// the lock held, and the goroutine that would receive the pending result may
// be the one holding it (a synchronous flush in Enqueue right after the delay
// timer flushed), so a blocking send could wait forever with the lock held
// (ConduitIO/conduit-connector-sdk#413). If a result is still pending, the
// two are combined: the earlier time, the summed size and both errors. Every
// sender holds the lock, so once the pending result is taken the send cannot
// fail; a receiver that takes it first just lets the next attempt through.
func (b *Batcher[T]) deliver(r BatchResult) {
	for {
		select {
		case b.results <- r:
			return
		default:
		}
		select {
		case pending := <-b.results:
			r = BatchResult{
				At:   pending.At,
				Size: pending.Size + r.Size,
				Err:  errors.Join(pending.Err, r.Err),
			}
		default:
			// a receiver took it in the meantime; the send will succeed
		}
	}
}
