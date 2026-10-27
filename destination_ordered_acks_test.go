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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// TestDestinationPluginAdapter_Open_DeclaresOrderedAcks pins the capability
// the engine reads to run several writes in flight (ConduitIO/conduit#3292).
func TestDestinationPluginAdapter_Open_DeclaresOrderedAcks(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)
	dst.EXPECT().Open(gomock.Any()).Return(nil)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil)
	resp, err := dstPlugin.Open(context.Background(), pconnector.DestinationOpenRequest{})
	is.NoErr(err)
	is.True(resp.OrderedAcks)
}

// ackCollector drains the ack side of a stream in the background, so the
// adapter never blocks on an ack and trailing (extra) acks are seen.
type ackCollector struct {
	mu   sync.Mutex
	acks []pconnector.DestinationRunResponseAck
}

func collectAcks(client pconnector.DestinationRunStreamClient) *ackCollector {
	c := &ackCollector{}
	go func() {
		for {
			resp, err := client.Recv()
			if err != nil {
				return
			}
			c.mu.Lock()
			c.acks = append(c.acks, resp.Acks...)
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *ackCollector) positions() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.acks))
	for i, a := range c.acks {
		out[i] = string(a.Position)
	}
	return out
}

// waitFor waits until n acks arrived, then lingers so a trailing extra ack
// would be collected too.
func (c *ackCollector) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for len(c.positions()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("got %d of %d acks", len(c.positions()), n)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
}

// TestDestinationPluginAdapter_Run_AcksInReceiveOrder checks the guarantee the
// SDK declares: acks leave in the order records were received, across several
// requests, exactly one per record (no trailing extra ack), for the single and
// the batching write strategy, with partial-write failures in the middle of a
// request, and with the delay timer racing Enqueue.
func TestDestinationPluginAdapter_Run_AcksInReceiveOrder(t *testing.T) {
	const requests, perRequest = 20, 5

	testCases := []struct {
		name      string
		batchSize int // 0: single strategy
		delay     time.Duration
		// failEvery makes Write report a partial write (n = len/2 and an
		// error) for every failEvery-th call, 0 disables it.
		failEvery int
	}{
		{name: "single"},
		{name: "single with partial failures", failEvery: 3},
		{name: "batch", batchSize: 7, delay: 5 * time.Millisecond},
		{name: "batch with partial failures", batchSize: 12, delay: 5 * time.Millisecond, failEvery: 2},
		{name: "batch timer flush racing enqueue", batchSize: 1000, delay: time.Millisecond},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctrl := gomock.NewController(t)
			dst := NewMockDestination(ctrl)

			var calls atomic.Int64
			dst.EXPECT().Open(gomock.Any()).Return(nil)
			dst.EXPECT().Write(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, recs []opencdc.Record) (int, error) {
					c := calls.Add(1)
					if tc.failEvery > 0 && c%int64(tc.failEvery) == 0 {
						return len(recs) / 2, errors.New("boom")
					}
					return len(recs), nil
				}).AnyTimes()

			a := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
			ctx := context.Background()
			_, err := a.Open(ctx, pconnector.DestinationOpenRequest{})
			is.NoErr(err)
			if tc.batchSize > 0 {
				a.writeStrategy = newWriteStrategyBatch(
					&writeStrategySingle{impl: dst, ackFn: a.ack}, tc.batchSize, tc.delay)
			}

			stream := NewInMemoryDestinationRunStream(ctx)
			runDone := make(chan error, 1)
			go func() { runDone <- a.Run(ctx, stream) }()

			want := ackOrderPositions(requests, perRequest)
			acks := collectAcks(stream.Client())
			go sendAckOrderRequests(stream.Client(), requests, perRequest)
			if tc.batchSize > 0 {
				// the last batch is flushed by the timer; make sure it is
				// before waiting, the wait itself is bounded anyway
				time.Sleep(tc.delay)
			}
			acks.waitFor(t, len(want))

			is.Equal(acks.positions(), want) // receive order, exactly one per record

			stream.Close(nil)
			<-runDone
		})
	}
}

// TestDestinationPluginAdapter_Run_TeardownWithoutStop_AcksArePrefix covers the
// ordered_acks exception: Teardown arrives while records are buffered by the
// batch strategy. The acks sent must be an in-order prefix of the received
// records and none may arrive after Teardown.
func TestDestinationPluginAdapter_Run_TeardownWithoutStop_AcksArePrefix(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)
	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().Teardown(gomock.Any()).Return(nil)
	// Counted here and asserted on the test goroutine: a gomock failure inside
	// Write would run on the batcher's goroutine and strand the in-flight
	// counter, which makes Teardown wait for its full timeout.
	var writes atomic.Int64
	dst.EXPECT().Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, recs []opencdc.Record) (int, error) {
			writes.Add(1)
			return len(recs), nil
		}).AnyTimes()

	a := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
	ctx := context.Background()
	_, err := a.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)
	// No wall clock: the delay timer never fires in this test. The buffered
	// batch is flushed explicitly after Teardown instead.
	a.writeStrategy = newWriteStrategyBatch(&writeStrategySingle{impl: dst, ackFn: a.ack}, 10, time.Hour)

	stream := NewInMemoryDestinationRunStream(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(ctx, stream) }()
	acks := collectAcks(stream.Client())

	// 3 requests of 5: the first two fill the batch (flushed and acked), the
	// third stays buffered.
	sendAckOrderRequests(stream.Client(), 3, 5)
	all := ackOrderPositions(3, 5)
	acks.waitFor(t, 10)

	_, err = a.Teardown(ctx, pconnector.DestinationTeardownRequest{})
	is.NoErr(err)
	// Flush the third request's records into the closed strategy, which is
	// what the delay timer would do: they must not reach the connector or be
	// acked.
	is.NoErr(a.writeStrategy.Flush(ctx))
	time.Sleep(100 * time.Millisecond) // let a wrongly sent ack arrive

	is.Equal(writes.Load(), int64(1))    // only the first, full batch was written
	is.Equal(acks.positions(), all[:10]) // an in-order prefix, nothing after Teardown

	stream.Close(nil)
	<-runDone
}

func TestDestinationPluginAdapter_Run_InvalidWriteCountIsNacked(t *testing.T) {
	for _, n := range []int{-1, 4} { // batch has 3 records
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			is := is.New(t)
			ctrl := gomock.NewController(t)
			dst := NewMockDestination(ctrl)
			dst.EXPECT().Open(gomock.Any()).Return(nil)
			dst.EXPECT().Write(gomock.Any(), gomock.Any()).Return(n, nil).Times(1)

			a := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
			ctx := context.Background()
			_, err := a.Open(ctx, pconnector.DestinationOpenRequest{})
			is.NoErr(err)

			stream := NewInMemoryDestinationRunStream(ctx)
			runDone := make(chan error, 1)
			go func() { runDone <- a.Run(ctx, stream) }()
			acks := collectAcks(stream.Client())

			sendAckOrderRequests(stream.Client(), 1, 3)
			acks.waitFor(t, 3)

			acks.mu.Lock()
			is.Equal(len(acks.acks), 3) // one ack per record, no panic
			for _, ack := range acks.acks {
				is.True(ack.Error != "") // all nacked
			}
			acks.mu.Unlock()

			stream.Close(nil)
			is.NoErr(<-runDone)
		})
	}
}

func TestDestinationPluginAdapter_Run_EmptyRequestIsIgnored(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)
	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, recs []opencdc.Record) (int, error) { return len(recs), nil }).
		Times(1) // only for the non-empty request

	a := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
	ctx := context.Background()
	_, err := a.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)

	stream := NewInMemoryDestinationRunStream(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(ctx, stream) }()
	acks := collectAcks(stream.Client())

	is.NoErr(stream.Client().Send(pconnector.DestinationRunRequest{}))
	sendAckOrderRequests(stream.Client(), 1, 2)
	acks.waitFor(t, 2)
	is.Equal(acks.positions(), ackOrderPositions(1, 2))

	stream.Close(nil)
	is.NoErr(<-runDone)
}

// ackOrderPositions lists the unique positions sendAckOrderRequests sends, in
// send order.
func ackOrderPositions(requests, perRequest int) []string {
	var out []string
	for r := 0; r < requests; r++ {
		for i := 0; i < perRequest; i++ {
			out = append(out, fmt.Sprintf("pos-%03d-%d", r, i))
		}
	}
	return out
}

func sendAckOrderRequests(client pconnector.DestinationRunStreamClient, requests, perRequest int) {
	for r := 0; r < requests; r++ {
		recs := make([]opencdc.Record, perRequest)
		for i := range recs {
			recs[i] = opencdc.Record{
				Position:  opencdc.Position(fmt.Sprintf("pos-%03d-%d", r, i)),
				Operation: opencdc.OperationCreate,
			}
		}
		_ = client.Send(pconnector.DestinationRunRequest{Records: recs})
	}
}

func TestCheckAckOrder(t *testing.T) {
	recs := func(ps ...string) []opencdc.Record {
		out := make([]opencdc.Record, len(ps))
		for i, p := range ps {
			out[i] = opencdc.Record{Position: opencdc.Position(p)}
		}
		return out
	}
	testCases := []struct {
		name    string
		want    []opencdc.Record
		acked   []string
		wantErr bool
	}{
		{"in order", recs("a", "b", "c"), []string{"a", "b", "c"}, false},
		{"in order across unrelated acks", recs("a", "b"), []string{"x", "a", "y", "b"}, false},
		{"swapped", recs("a", "b", "c"), []string{"a", "c", "b"}, true},
		{"reversed batch", recs("a", "b", "c"), []string{"c", "b", "a"}, true},
		{"missing", recs("a", "b", "c"), []string{"a", "b"}, true},
		{"non-unique positions", recs("a", "a"), []string{"a", "a"}, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkAckOrder(tc.want, tc.acked)
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v, got %v", tc.wantErr, err)
			}
		})
	}
}
