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
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Batcher[T any] struct {
	sizeThreshold  int
	delayThreshold time.Duration
	fn             BatchFn[T]

	batch      chan T // batch collects elements of a batch
	flushTimer *time.Timer
	flushedAt  time.Time
	m          sync.Mutex // m gets locked when a flush is triggered

	stop    chan struct{}
	stopped chan struct{}
	flush   chan struct{}
	errors  chan error
}

type BatchFn[T any] func([]T) error

type EnqueueResult int

const (
	Scheduled EnqueueResult = iota + 1
	Flushed
)

func NewBatcher[T any](sizeThreshold int, delayThreshold time.Duration, fn BatchFn[T]) *Batcher[T] {
	if sizeThreshold < 2 {
		panic(fmt.Errorf("batch size threshold must be at least 2, got %d", sizeThreshold))
	}

	status := atomic.Value{}
	status.Store(Scheduled)
	return &Batcher[T]{
		sizeThreshold:  sizeThreshold,
		delayThreshold: delayThreshold,
		batch:          make(chan T),
		errors:         make(chan error, 1),
		flush:          make(chan struct{}),
		fn:             fn,
	}
}

func (b *Batcher[T]) Start() error {
	b.m.Lock()
	defer b.m.Unlock()

	if b.stop != nil {
		return errors.New("batcher already started")
	}
	b.stop = make(chan struct{})
	b.stopped = make(chan struct{})
	go func() {
		batch := make([]T, 0, b.sizeThreshold)
		var flushTimer *time.Timer
		var flushTimerC <-chan time.Time
		for {
			select {
			case <-b.stop:
				b.m.Lock()
				b.stop = nil
				close(b.stopped)
				b.m.Unlock()
				return

			case item := <-b.batch:
				batch = append(batch, item)
				switch len(batch) {
				case 1:
					// start timer for flush
					flushTimer = time.NewTimer(b.delayThreshold)
					flushTimerC = flushTimer.C
				}
				if len(batch) == b.sizeThreshold {
					flushTimer.Stop()
					b.errors <- b.fn(batch)
				}
			case <-flushTimerC:
				flushTimer.Stop()
				b.errors <- b.fn(batch)
			case <-b.flush:
				flushTimer.Stop()
				b.errors <- b.fn(batch)
			}
		}
	}()
	return nil
}

func (b *Batcher[T]) Stop() {
	b.m.Lock()
	if b.stop == nil {
		b.m.Unlock()
		return // batcher is not started
	}
	select {
	case <-b.stop:
		// stop already closed
	default:
		close(b.stop)
	}
	b.m.Unlock()

	<-b.stopped
}

func (b *Batcher[T]) Enqueue(item T) {
	b.batch <- item
}

func (b *Batcher[T]) Flush() {
	b.flush <- struct{}{}
}

func (b *Batcher[T]) Errors() chan error {
	return b.errors
}
