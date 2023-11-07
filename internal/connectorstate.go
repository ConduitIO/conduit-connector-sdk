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

//go:generate stringer -type ConnectorState -trimprefix State

package internal

import (
	"context"
	"fmt"
	"slices"

	"github.com/conduitio/conduit-connector-sdk/internal/csync"
)

type ConnectorState int

const (
	StateInitial ConnectorState = iota
	StateConfiguring
	StateConfigured
	StateStarting
	StateStarted
	StateInitiatingRun
	StateRunning
	StateInitiatingStop
	StateStopping
	StateStopped
	StateTearingDown
	StateTornDown

	StateErrored ConnectorState = 500
)

type ConnectorStateWatcher csync.ValueWatcher[ConnectorState]

type DoWithLockOptions struct {
	ExpectedStates       []ConnectorState
	StateBefore          ConnectorState
	StateAfter           ConnectorState
	WaitForExpectedState bool
}

func (w *ConnectorStateWatcher) DoWithLock(
	ctx context.Context,
	opts DoWithLockOptions,
	f func(currentState ConnectorState) error,
) error {
	vw := (*csync.ValueWatcher[ConnectorState])(w)
	lockedWatcher := vw.Lock()
	locked := true // keep track if the lock is still locked
	defer func() {
		if locked {
			lockedWatcher.Unlock()
		}
	}()

	currentState := lockedWatcher.Get()

	if len(opts.ExpectedStates) > 0 {
		for !slices.Contains(opts.ExpectedStates, currentState) {
			if !opts.WaitForExpectedState {
				return fmt.Errorf("expected connector state %q, actual connector state is %q", opts.ExpectedStates, currentState)
			}
			lockedWatcher.Unlock()
			lockedWatcher = nil // discard locked watcher after unlock
			locked = false      // prevent another unlock in defer

			_, err := vw.Watch(ctx, csync.WatchValues(opts.ExpectedStates...))
			if err != nil {
				return err
			}

			// lock watcher again and check current state in case it changed between
			// watch and the second lock
			lockedWatcher = vw.Lock()
			locked = true
			currentState = lockedWatcher.Get()
		}
	}

	w.swap(lockedWatcher, opts.StateBefore)

	err := f(currentState)
	if err != nil {
		lockedWatcher.Set(StateErrored)
		return err
	}

	w.swap(lockedWatcher, opts.StateAfter)
	return nil
}

func (w *ConnectorStateWatcher) Set(newState ConnectorState) bool {
	lockedWatcher := (*csync.ValueWatcher[ConnectorState])(w).Lock()
	defer lockedWatcher.Unlock()
	return w.swap(lockedWatcher, newState)
}

func (w *ConnectorStateWatcher) swap(lvw *csync.LockedValueWatcher[ConnectorState], newState ConnectorState) bool {
	if lvw.Get() >= newState {
		// states can only increase
		return false
	}
	lvw.Set(newState)
	return true
}
