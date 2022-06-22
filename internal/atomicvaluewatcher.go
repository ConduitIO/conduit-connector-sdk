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

// AtomicValueWatcher holds a reference to a value. Multiple goroutines are able to put or
// retrieve the value into/from the AtomicValueWatcher, as well as wait for a certain value
// to be put into the AtomicValueWatcher.
// It is similar to atomic.Value except the caller can call Await to be notified
// each time the value in AtomicValueWatcher changes.
type AtomicValueWatcher[T any] struct {
	val      T
	m        sync.RWMutex
	listener chan T // a single listener, if needed we can expand to multiple in the future
}

// Store sets val in AtomicValueWatcher and notifies the goroutine that called Await about
// the new value, if such a goroutine exists.
func (h *AtomicValueWatcher[T]) Store(val T) {
	h.m.Lock()
	defer h.m.Unlock()

	h.val = val
	if h.listener != nil {
		h.listener <- val
	}
}

// Load returns the current value stored in AtomicValueWatcher.
func (h *AtomicValueWatcher[T]) Load() T {
	h.m.RLock()
	defer h.m.RUnlock()

	return h.val
}

// Await blocks and calls foundVal for every value that is put into the AtomicValueWatcher.
// Once foundVal returns true it stops blocking and returns nil. First call to
// foundVal will be with the current value stored in AtomicValueWatcher. Await can only be
// called by one goroutine at a time (we don't need anything more fancy right
// now), if two goroutines call Await one will receive an error. If the context
// gets cancelled before foundVal returns true, the function will return the
// context error.
func (h *AtomicValueWatcher[T]) Await(ctx context.Context, foundVal func(val T) bool) error {
	val, err := h.subscribe()
	if err != nil {
		// the only option subscribe produces an error is if it is called
		// concurrently which is an invalid use case at the moment
		return fmt.Errorf("invalid use of AtomicValueWatcher.Await: %w", err)
	}
	defer h.unsubscribe()

	if foundVal(val) {
		// first call to foundVal is with the current value
		return nil
	}
	// val was not found yet, we need to wait some more
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

// subscribe creates listener and returns the current value stored in AtomicValueWatcher at
// the time the listener was created.
func (h *AtomicValueWatcher[T]) subscribe() (T, error) {
	h.m.Lock()
	defer h.m.Unlock()

	if h.listener != nil {
		var empty T
		return empty, errors.New("another goroutine is already subscribed to changes")
	}
	h.listener = make(chan T)

	return h.val, nil
}

func (h *AtomicValueWatcher[T]) unsubscribe() {
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
