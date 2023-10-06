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
	"fmt"

	"github.com/conduitio/conduit-connector-sdk/internal/csync"
)

type ConnectorState int

const (
	StateInitial ConnectorState = iota
	StateConfiguring
	StateConfigured
	StateStarting
	StateStarted
	StateRunning
	StateStopping
	StateStopped
	StateTorndown

	StateErrored ConnectorState = 500
)

type UnexpectedConnectorStateError struct {
	ExpectedState ConnectorState
	ActualState   ConnectorState
}

func (e *UnexpectedConnectorStateError) Error() string {
	return fmt.Sprintf("expected connector state %q, actual connector state is %q", e.ExpectedState, e.ActualState)
}

func NewUnexpectedConnectorStateError(expected, actual ConnectorState) *UnexpectedConnectorStateError {
	return &UnexpectedConnectorStateError{
		ExpectedState: expected,
		ActualState:   actual,
	}
}

type ConnectorStateWatcher csync.ValueWatcher[ConnectorState]

func (w *ConnectorStateWatcher) Compare(expectedState ConnectorState) error {
	vw := (*csync.ValueWatcher[ConnectorState])(w)
	if s := vw.Get(); s != expectedState {
		return NewUnexpectedConnectorStateError(expectedState, s)
	}
	return nil
}

func (w *ConnectorStateWatcher) Swap(newState ConnectorState) {
	(*csync.ValueWatcher[ConnectorState])(w).Set(newState)
}

func (w *ConnectorStateWatcher) CompareAndSwap(oldState ConnectorState, newState ConnectorState) error {
	if err := w.Compare(oldState); err != nil {
		return fmt.Errorf("can't change connector state to %q: %w", newState, err)
	}
	w.Swap(newState)
	return nil
}

func (w *ConnectorStateWatcher) CheckErrorAndSwap(err error, newState ConnectorState) {
	vw := (*csync.ValueWatcher[ConnectorState])(w)
	if err != nil {
		vw.Set(StateErrored)
	} else {
		vw.Set(newState)
	}
}
