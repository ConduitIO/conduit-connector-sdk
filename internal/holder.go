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
	"errors"
	"fmt"
	"sync"
)

// Holder holds a reference to a value. Multiple goroutines are able to put or
// retrieve the value into/from the Holder, as well as wait for a certain value
// to be put into the Holder.
type Holder[T any] struct {
	val      T
	m        sync.RWMutex
	listener chan T // a single listener, if needed we can expand to multiple in the future
}

// Put stores val in Holder and notifies the goroutine that called Await about
// the new value, if such a goroutine exists.
func (h *Holder[T]) Put(val T) {
	h.m.Lock()
	defer h.m.Unlock()

	h.val = val
	if h.listener != nil {
		h.listener <- val
	}
}

// Get returns the current value in Holder.
func (h *Holder[T]) Get() T {
	h.m.RLock()
	defer h.m.RUnlock()

	return h.val
}

// Await blocks and calls foundVal for every value that is put into the Holder.
// Once foundVal returns true it stops blocking and returns nil. First call to
// foundVal will be with the current value stored in Holder. Await can only be
// called by one goroutine at a time (we don't need anything more fancy right
// now), if two goroutines call Await one will receive an error. If the context
// gets cancelled before foundVal returns true, the function will return the
// context error.
func (h *Holder[T]) Await(ctx context.Context, foundVal func(val T) bool) error {
	err := h.subscribe()
	if err != nil {
		// the only option subscribe produces an error is if it is called
		// concurrently which is an invalid use case at the moment
		return fmt.Errorf("invalid use of Holder.Await: %w", err)
	}
	defer h.unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case val := <-h.listener:
			if foundVal(val) {
				return nil
			}
		}
	}
}

func (h *Holder[T]) subscribe() error {
	h.m.Lock()
	defer h.m.Unlock()

	if h.listener != nil {
		return errors.New("another goroutine is already subscribed to changes")
	}

	h.listener = make(chan T)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// make sure the current value is sent to the listener
		currentVal := h.val // copy val
		wg.Done()           // signal to subscribe function it can unlock the mutex
		h.listener <- currentVal
	}()

	// wait for goroutine to copy val
	wg.Wait()

	return nil
}

func (h *Holder[T]) unsubscribe() {
	// drain channel and remove it
	go func(in chan T) {
		for range in {
			// do nothing, just drain channel in case new values come in
			// while we try to unsubscribe
		}
	}(h.listener)

	h.m.Lock()
	defer h.m.Unlock()

	close(h.listener)
	h.listener = nil
}
