// Copyright © 2026 Meroxa, Inc.
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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/matryer/is"
)

// batchSizeRecordingDestination records the size of every Write call, so tests
// can assert whether records were coalesced into a single Write or written one
// per call.
type batchSizeRecordingDestination struct {
	UnimplementedDestination

	mu      sync.Mutex
	batches []int
}

func (*batchSizeRecordingDestination) Config() DestinationConfig      { return nil }
func (*batchSizeRecordingDestination) Open(context.Context) error     { return nil }
func (*batchSizeRecordingDestination) Teardown(context.Context) error { return nil }

func (d *batchSizeRecordingDestination) Write(_ context.Context, recs []opencdc.Record) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.batches = append(d.batches, len(recs))
	return len(recs), nil
}

func (d *batchSizeRecordingDestination) snapshot() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]int(nil), d.batches...)
}

// TestDestinationPluginAdapter_DelayOnlyBatching is the regression test for the
// sdk.batch.delay-without-sdk.batch.size configuration: records arriving inside
// the delay window must be coalesced into one Write flushed by the timer, not
// written one per Write (which the size threshold of 0 used to force, ignoring
// the delay entirely).
func TestDestinationPluginAdapter_DelayOnlyBatching(t *testing.T) {
	is := is.New(t)
	dst := &batchSizeRecordingDestination{}
	plugin := newBatchingAdapter(t, dst, DestinationWithBatch{BatchSize: 0, BatchDelay: 200 * time.Millisecond})

	stream := newRecordingRunStream(100)
	ctx := context.Background()
	runDone := make(chan error, 1)
	go func() { runDone <- plugin.Run(ctx, stream) }()

	rec := opencdc.Record{Position: opencdc.Position("1"), Payload: opencdc.Change{After: opencdc.RawData("x")}}
	// 5 records arrive well inside the delay window, so none may flush by size.
	for i := 0; i < 5; i++ {
		stream.reqs <- pconnector.DestinationRunRequest{Records: []opencdc.Record{rec}}
	}

	// The timer flushes the coalesced batch; drain its result through Run.
	deadline := time.Now().Add(5 * time.Second)
	for len(dst.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	is.Equal(dst.snapshot(), []int{5}) // one Write of all 5 records, not five writes of 1

	close(stream.reqs)
	is.NoErr(<-runDone)
}
