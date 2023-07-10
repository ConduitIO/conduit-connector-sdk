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
	"testing"
	"time"

	"github.com/matryer/is"
)

func BenchmarkBatcher_Enqueue(b *testing.B) {
	batcher := NewBatcher(
		1000,
		time.Second,
		func(batch []int) error { return nil },
	)

	var er EnqueueResult
	for i := 0; i < b.N; i++ {
		er = batcher.Enqueue(i)
	}
	_ = er
}

func TestBatcher_Enqueue_Scheduled(t *testing.T) {
	is := is.New(t)
	var gotBatches [][]int

	b := NewBatcher(
		2,
		time.Second,
		func(batch []int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	result := b.Enqueue(1)
	rs, ok := result.(Scheduled)
	is.True(ok)
	select {
	case <-rs.Err:
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
		func(batch []int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	result := b.Enqueue(1) // first item gets scheduled
	rs, ok := result.(Scheduled)
	is.True(ok)

	result = b.Enqueue(2) // second item triggers a flush
	rf, ok := result.(Flushed)
	is.True(ok)
	is.NoErr(rf.Err)

	// the scheduled result should contain the same error, i.e. nil
	select {
	case err := <-rs.Err:
		is.NoErr(err)
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
		func(batch []int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	start := time.Now()
	result := b.Enqueue(1) // first item gets scheduled
	rs, ok := result.(Scheduled)
	is.True(ok)

	select {
	case err := <-rs.Err:
		is.NoErr(err)
		is.True(time.Since(start) >= wantDelay)
		is.Equal(len(gotBatches), 1)
		is.Equal(gotBatches[0], []int{1, 2})
	case <-time.After(wantDelay + time.Millisecond*5):
		t.Fatal("expected the channel to contain a value after delay")
	}

	result = b.Enqueue(1) // next item gets scheduled again
	_, ok = result.(Scheduled)
	is.True(ok)
}

func TestBatcher_Enqueue_FlushedError(t *testing.T) {
	is := is.New(t)
	wantErr := errors.New("test error")

	b := NewBatcher(
		2,
		time.Second,
		func(batch []int) error {
			return wantErr
		},
	)

	result := b.Enqueue(1) // first item gets scheduled
	rs, ok := result.(Scheduled)
	is.True(ok)

	result = b.Enqueue(2) // second item triggers a flush
	rf, ok := result.(Flushed)
	is.True(ok)
	is.Equal(wantErr, rf.Err)

	select {
	case err := <-rs.Err:
		is.Equal(wantErr, err)
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
		func(batch []int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	var results []Scheduled
	for i := 0; i < 9; i++ {
		result := b.Enqueue(i)
		results = append(results, result.(Scheduled))
	}

	is.Equal(len(gotBatches), 0)
	b.Flush()

	is.Equal(len(gotBatches), 1)
	is.Equal(gotBatches[0], []int{0, 1, 2, 3, 4, 5, 6, 7, 8})

	for _, rs := range results {
		select {
		case err := <-rs.Err:
			is.NoErr(err)
		default:
			t.Fatal("expected the channel to contain a value")
		}
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
		func(batch []int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(c int) {
			defer wg.Done()
			var results []EnqueueResult
			for i := 0; i < batchSize; i++ {
				r := b.Enqueue(c*batchSize + i)
				results = append(results, r)
			}
			for _, r := range results {
				switch r := r.(type) {
				case Scheduled:
					is.NoErr(<-r.Err)
				case Flushed:
					is.NoErr(r.Err)
				default:
					t.Fatal("unknown result type")
				}
			}
		}(i)
	}

	wg.Wait()
	is.Equal(len(gotBatches), workers)
	for _, gotBatch := range gotBatches {
		is.Equal(len(gotBatch), batchSize)
	}
}
