// Copyright © 2022 Meroxa, Inc.
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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"go.uber.org/goleak"
)

func TestHolder_GetEmptyValue(t *testing.T) {
	is := is.New(t)

	var h Holder[int]
	got := h.Load()
	is.Equal(0, got)
}

func TestHolder_GetEmptyPtr(t *testing.T) {
	is := is.New(t)

	var h Holder[*int]
	got := h.Load()
	is.Equal(nil, got)
}

func TestHolder_PutGetValue(t *testing.T) {
	is := is.New(t)

	var h Holder[int]
	want := 123
	h.Store(want)
	got := h.Load()
	is.Equal(want, got)
}

func TestHolder_PutGetPtr(t *testing.T) {
	is := is.New(t)

	var h Holder[*int]
	want := 123
	h.Store(&want)
	got := h.Load()
	is.Equal(&want, got)
}

func TestHolder_AwaitSuccess(t *testing.T) {
	goleak.VerifyNone(t)
	is := is.New(t)

	var h Holder[int]

	putValue := make(chan int)
	defer close(putValue)
	go func() {
		for val := range putValue {
			h.Store(val)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	i := 0
	err := h.Await(ctx, func(val int) bool {
		i++
		switch i {
		case 1:
			is.Equal(0, val) // expected first value to be 0
			putValue <- 123  // put next value
			return false     // not the value we are looking for
		case 2:
			is.Equal(123, val)
			putValue <- 555 // put next value
			return false    // not the value we are looking for
		case 3:
			is.Equal(555, val)
			return true // that's what we were looking for
		default:
			is.Fail() // unexpected value for i
			return false
		}
	})
	is.NoErr(err)
	is.Equal(3, i)

	got := h.Load()
	is.Equal(555, got)

	// we can still put more values into the holder
	h.Store(666)
	got = h.Load()
	is.Equal(666, got)
}

func TestHolder_AwaitContextCancel(t *testing.T) {
	goleak.VerifyNone(t)
	is := is.New(t)

	var h Holder[int]

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	i := 0
	err := h.Await(ctx, func(val int) bool {
		i++
		is.Equal(0, val)
		return false
	})

	is.Equal(ctx.Err(), err)
	is.Equal(1, i)
}

func TestHolder_AwaitMultiple(t *testing.T) {
	goleak.VerifyNone(t)
	is := is.New(t)

	var h Holder[int]

	var wg1, wg2, wg3 sync.WaitGroup
	wg1.Add(1)
	wg2.Add(1)
	wg3.Add(1)
	go func() {
		defer wg3.Done()
		err := h.Await(context.Background(), func(val int) bool {
			wg1.Done()
			wg2.Wait() // wait until test says it's ok to return
			return true
		})
		is.NoErr(err)
	}()

	wg1.Wait() // wait for Await to actually run

	// try to run await a second time
	err := h.Await(context.Background(), func(val int) bool { return false })
	is.True(err != nil) // expected an error from second Await call

	wg2.Done() // signal to first Await call to return
	wg3.Wait() // wait for goroutine to stop running
}
