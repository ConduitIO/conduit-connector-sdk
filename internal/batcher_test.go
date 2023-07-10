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
	"testing"
	"time"

	"github.com/matryer/is"
)

func BenchmarkBatcher_Enqueue(b *testing.B) {
	batchSizes := []int{10, 100, 1000, 10000, 100000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprint(batchSize), func(b *testing.B) {
			batcher := NewBatcher(
				batchSize,
				time.Second,
				func(batch []int) error { return nil },
			)
			_ = batcher.Start()
			defer batcher.Stop()

			for i := 0; i < b.N; i++ {
				batcher.Enqueue(i)
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
		func(batch []int) error {
			gotBatches = append(gotBatches, batch)
			return nil
		},
	)
	err := b.Start()
	is.NoErr(err)
	defer b.Stop()

	b.Enqueue(1)
	select {
	case <-b.Errors():
		t.Fatal("did not expect the channel to contain a value")
	default:
		// all good
	}
	is.Equal(0, len(gotBatches)) // did not expect any batch to be executed
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
	err := b.Start()
	is.NoErr(err)
	defer b.Stop()

	b.Enqueue(1) // first item gets scheduled
	b.Enqueue(2) // second item triggers a flush

	is.NoErr(<-b.Errors())

	is.Equal(1, len(gotBatches))
	is.Equal([]int{1, 2}, gotBatches[0])
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
	err := b.Start()
	is.NoErr(err)
	defer b.Stop()

	errs := b.Errors()

	start := time.Now()
	b.Enqueue(1) // first item gets scheduled

	select {
	case err := <-errs:
		is.NoErr(err)
		is.True(time.Since(start) >= wantDelay)
		is.Equal(len(gotBatches), 1)
		is.Equal(gotBatches[0], []int{1})
	case <-time.After(wantDelay + time.Millisecond*5):
		t.Fatal("expected the channel to contain a value after delay")
	}

	b.Enqueue(2) // next item gets scheduled again
	select {
	case <-errs:
		t.Fatal("did not expect the channel to contain a value")
	default:
		// all good
	}
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
	err := b.Start()
	is.NoErr(err)
	defer b.Stop()

	b.Enqueue(1) // first item gets scheduled
	b.Enqueue(2) // second item triggers a flush

	is.Equal(wantErr, <-b.Errors())
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
	err := b.Start()
	is.NoErr(err)
	defer b.Stop()

	for i := 0; i < 9; i++ {
		b.Enqueue(i)
	}

	is.Equal(len(gotBatches), 0)
	b.Flush()

	is.NoErr(<-b.Errors())

	is.Equal(len(gotBatches), 1)
	is.Equal(gotBatches[0], []int{0, 1, 2, 3, 4, 5, 6, 7, 8})
}

// func TestBatcher_Concurrent(t *testing.T) {
// 	is := is.New(t)
// 	var gotBatches [][]int
// 	const workers = 100
// 	const batchSize = 5
//
// 	b := NewBatcher(
// 		batchSize,
// 		time.Second,
// 		func(batch []int) error {
// 			gotBatches = append(gotBatches, batch)
// 			return nil
// 		},
// 	)
//
// 	var wg sync.WaitGroup
// 	wg.Add(workers)
// 	for i := 0; i < workers; i++ {
// 		go func(c int) {
// 			defer wg.Done()
// 			var errs []chan error
// 			for i := 0; i < batchSize; i++ {
// 				err := make(chan error, 1)
// 				_ = b.Enqueue(c*batchSize+i, WithErrorResult(err))
// 				errs = append(errs, err)
// 			}
// 			for _, err := range errs {
// 				is.NoErr(<-err)
// 			}
// 		}(i)
// 	}
//
// 	wg.Wait()
// 	is.Equal(len(gotBatches), workers)
// 	for _, gotBatch := range gotBatches {
// 		is.Equal(len(gotBatch), batchSize)
// 	}
// }
