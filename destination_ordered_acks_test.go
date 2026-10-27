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

// TestDestinationPluginAdapter_Run_AcksInReceiveOrder checks the guarantee the
// SDK declares: acks leave in the order records were received, across several
// requests, one per record, for the single and the batching write strategy and
// when a write fails part way through a batch.
func TestDestinationPluginAdapter_Run_AcksInReceiveOrder(t *testing.T) {
	const requests, perRequest = 20, 5

	testCases := []struct {
		name  string
		batch bool
		// failEvery makes Write report a partial write (n = len/2 and an
		// error) for every failEvery-th call, 0 disables it.
		failEvery int
	}{
		{name: "single"},
		{name: "single with partial failures", failEvery: 3},
		{name: "batch", batch: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctrl := gomock.NewController(t)
			dst := NewMockDestination(ctrl)

			calls := 0
			dst.EXPECT().Open(gomock.Any()).Return(nil)
			dst.EXPECT().Write(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, recs []opencdc.Record) (int, error) {
					calls++
					if tc.failEvery > 0 && calls%tc.failEvery == 0 {
						return len(recs) / 2, errors.New("boom")
					}
					return len(recs), nil
				}).AnyTimes()

			a := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
			ctx := context.Background()
			_, err := a.Open(ctx, pconnector.DestinationOpenRequest{})
			is.NoErr(err)
			if tc.batch {
				a.writeStrategy = newWriteStrategyBatch(
					&writeStrategySingle{impl: dst, ackFn: a.ack}, 7, 5*time.Millisecond)
			}

			stream := NewInMemoryDestinationRunStream(ctx)
			runDone := make(chan error, 1)
			go func() { runDone <- a.Run(ctx, stream) }()

			want := ackOrderPositions(requests, perRequest)
			go sendAckOrderRequests(stream.Client(), requests, perRequest)
			got := recvAckPositions(is, stream.Client(), len(want))
			is.Equal(got, want) // acks must be in receive order, exactly one per record

			stream.Close(nil)
		})
	}
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

func recvAckPositions(is *is.I, client pconnector.DestinationRunStreamClient, n int) []string {
	var got []string
	for len(got) < n {
		resp, err := client.Recv()
		is.NoErr(err)
		for _, ack := range resp.Acks {
			got = append(got, string(ack.Position))
		}
	}
	return got
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
