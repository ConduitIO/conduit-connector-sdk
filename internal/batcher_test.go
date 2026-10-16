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
	"testing"
	"time"

	"github.com/matryer/is"
)

func BenchmarkBatcher_Enqueue(b *testing.B) {
	batchSizes := []int{10, 100, 1000, 10000, 100000}

	var es EnqueueStatus
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			batcher := NewBatcher(
				batchSize,
				time.Hour, // don't trigger based on time
				func(batch []int, size int) error { return nil },
			)

			for i := 0; i < b.N; i++ {
				es = batcher.Enqueue(i, 1)
				if es == Flushed {
					<-batcher.Results()
				}
			}
		})
	}
}

func TestBatcher_Enqueue_Scheduled(t *testing.T) {
	is := is.New(t)
	var gotBatches [][]int

	b := NewBatcher(
		2,
		time.Second,
		func(batch []int, size int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	status := b.Enqueue(1, 1)
	is.Equal(Scheduled, status)
	select {
	case <-b.Results():
		t.Fatal("did not expect the channel to contain a value")
	default:
		// all good
	}
	is.Equal(len(gotBatches), 0) // did not expect any batch to be executed
}

func TestBatcher_Enqueue_Flushed(t *testing.T) {
	is := is.New(t)
	var gotBatches [][]int

	b := NewBatcher(
		2,
		time.Second,
		func(batch []int, size int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	status := b.Enqueue(1, 1) // first item gets scheduled
	is.Equal(Scheduled, status)

	status = b.Enqueue(2, 1) // second item triggers a flush
	is.Equal(Flushed, status)

	// the scheduled result should contain the same error, i.e. nil
	select {
	case result := <-b.Results():
		is.NoErr(result.Err)
	default:
		t.Fatal("expected the channel to contain a value")
	}

	is.Equal(len(gotBatches), 1)
	is.Equal(gotBatches[0], []int{1, 2})
}

func TestBatcher_Enqueue_Flushed_OverBatchSize(t *testing.T) {
	is := is.New(t)
	var gotBatches [][]int

	b := NewBatcher(
		2,
		time.Second,
		func(batch []int, size int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	status := b.Enqueue(1, 1) // first item gets scheduled
	is.Equal(Scheduled, status)

	status = b.Enqueue(2, 2) // second item triggers a flush
	is.Equal(Flushed, status)

	// the scheduled result should contain the same error, i.e. nil
	select {
	case result := <-b.Results():
		is.NoErr(result.Err)
	default:
		t.Fatal("expected the channel to contain a value")
	}

	is.Equal(len(gotBatches), 1)
	is.Equal(gotBatches[0], []int{1, 2})
}

func TestBatcher_Enqueue_Delay(t *testing.T) {
	is := is.New(t)
	var gotBatches [][]int
	const wantDelay = time.Millisecond * 10

	b := NewBatcher(
		2,
		wantDelay,
		func(batch []int, size int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	start := time.Now()
	status := b.Enqueue(1, 1) // first item gets scheduled
	is.Equal(Scheduled, status)

	select {
	case result := <-b.Results():
		is.NoErr(result.Err)
		is.True(time.Since(start) >= wantDelay)
		is.Equal(len(gotBatches), 1)
		is.Equal(gotBatches[0], []int{1})
	case <-time.After(5 * time.Second):
		// only a deadline for a timer that never fires: the lower bound
		// above is the assertion (ConduitIO/conduit-connector-sdk#417)
		t.Fatal("expected the channel to contain a value after delay")
	}

	status = b.Enqueue(1, 1) // next item gets scheduled again
	is.Equal(Scheduled, status)
}

func TestBatcher_Enqueue_FlushedError(t *testing.T) {
	is := is.New(t)
	wantErr := errors.New("test error")

	b := NewBatcher(
		2,
		time.Second,
		func(batch []int, size int) error {
			return wantErr
		},
	)

	status := b.Enqueue(1, 1) // first item gets scheduled
	is.Equal(Scheduled, status)

	status = b.Enqueue(2, 1) // second item triggers a flush
	is.Equal(Flushed, status)

	select {
	case result := <-b.Results():
		is.Equal(wantErr, result.Err)
	default:
		t.Fatal("expected the channel to contain a value")
	}
}

func TestBatcher_Flush(t *testing.T) {
	is := is.New(t)
	var gotBatches [][]int

	b := NewBatcher(
		10,
		time.Second,
		func(batch []int, size int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	for i := 0; i < 9; i++ {
		status := b.Enqueue(i, 1)
		is.Equal(Scheduled, status)
	}

	is.Equal(len(gotBatches), 0)
	b.Flush()

	is.Equal(len(gotBatches), 1)
	is.Equal(gotBatches[0], []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

	select {
	case result := <-b.Results():
		is.NoErr(result.Err)
		is.Equal(result.Size, 9)
	default:
		t.Fatal("expected the channel to contain a value")
	}
}

func TestBatcher_Concurrent(t *testing.T) {
	is := is.New(t)
	var gotBatches [][]int
	const workers = 100
	const batchSize = 5

	b := NewBatcher(
		batchSize,
		time.Second,
		func(batch []int, size int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(c int) {
			defer wg.Done()
			for i := 0; i < batchSize; i++ {
				_ = b.Enqueue(c*batchSize+i, 1)
			}
		}(i)
	}

	// A flush never waits for its result to be received, so results of
	// flushes that land before the previous one is received are combined
	// (#413). Every record has to be covered by exactly one result.
	for total := 0; total < workers*batchSize; {
		select {
		case result := <-b.Results():
			is.NoErr(result.Err)
			total += result.Size
		case <-time.After(10 * time.Second):
			t.Fatalf("results cover %d of %d records; a result was lost", total, workers*batchSize)
		}
	}
	wg.Wait()
	select {
	case r := <-b.Results():
		t.Fatalf("unexpected extra result covering %d records", r.Size)
	default:
	}

	is.Equal(len(gotBatches), workers)
	for _, gotBatch := range gotBatches {
		is.Equal(len(gotBatch), batchSize)
	}
}

// TestBatcher_SyncFlushWithPendingResult is the regression test for
// ConduitIO/conduit-connector-sdk#413. The delay timer flushes and leaves its
// result pending; then an item that alone fills a batch is enqueued. The
// synchronous flush used to block sending its result to the full channel
// while holding the lock, and the only goroutine that would receive it was
// the one blocked in Enqueue, so the destination stopped for good.
func TestBatcher_SyncFlushWithPendingResult(t *testing.T) {
	timerErr := errors.New("timer flush failed")
	syncErr := errors.New("sync flush failed")
	for _, tc := range []struct {
		name     string
		timerErr error
		syncErr  error
	}{
		{"both succeed", nil, nil},
		{"timer flush failed", timerErr, nil},
		{"sync flush failed", nil, syncErr},
		{"both failed", timerErr, syncErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			var flushes [][]int
			b := NewBatcher(5, time.Hour, func(batch []int, _ int) error {
				flushes = append(flushes, append([]int(nil), batch...))
				if len(flushes) == 1 {
					return tc.timerErr
				}
				return tc.syncErr
			})

			is.Equal(b.Enqueue(1, 1), Scheduled)
			is.True(b.Flush()) // what the delay timer does; its result is pending

			done := make(chan EnqueueStatus, 1)
			go func() { done <- b.Enqueue(2, 5) }() // alone fills a batch
			var status EnqueueStatus
			select {
			case status = <-done:
			case <-time.After(10 * time.Second):
				// a bound, not a wait: the fixed Enqueue returns at once
				t.Fatal("Enqueue blocked: synchronous flush waited on a full results channel (#413)")
			}
			is.Equal(status, Flushed)
			is.Equal(flushes, [][]int{{1}, {2}})

			// one combined result covers both flushes and reports both errors
			select {
			case r := <-b.Results():
				is.Equal(r.Size, 6)
				is.Equal(errors.Is(r.Err, timerErr), tc.timerErr != nil)
				is.Equal(errors.Is(r.Err, syncErr), tc.syncErr != nil)
				is.Equal(r.Err == nil, tc.timerErr == nil && tc.syncErr == nil)
			default:
				t.Fatal("expected a result")
			}
			select {
			case r := <-b.Results():
				t.Fatalf("unexpected second result %+v", r)
			default:
			}
		})
	}
}
