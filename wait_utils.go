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

package sdk

import (
	"context"
	"sync"
	"time"
)

// waitTimeout returns true if the given WaitGroup's counter is zero
// before the given timeout is reached. Returns false otherwise.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	withTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return waitOrDone(withTimeout, wg) == nil
}

// waitTimeout returns nil if the given WaitGroup's counter is zero
// before the given context is done. Returns the context's Err() otherwise.
func waitOrDone(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return waitForClose(ctx, done)
}

// waitForClose waits until the given channel receives a struct or until the given context is done.
// If the channel receives a struct before the context is done, nil is returned.
// Returns context's Err() otherwise.
func waitForClose(ctx context.Context, stop chan struct{}) error {
	select {
	case <-stop:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
