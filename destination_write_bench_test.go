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
	"fmt"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
)

// benchAckStream is a DestinationRunStreamServer whose Send discards the acks.
// It lets the benchmarks measure the middleware's own allocation and CPU cost
// without the noise of the real gRPC transport, while still walking the exact
// ack path (including the make([]DestinationRunResponseAck, ...) per write).
type benchAckStream struct {
	sent int
}

func (s *benchAckStream) Send(resp pconnector.DestinationRunResponse) error {
	s.sent += len(resp.Acks)
	return nil
}

func (*benchAckStream) Recv() (pconnector.DestinationRunRequest, error) {
	return pconnector.DestinationRunRequest{}, fmt.Errorf("unused")
}

func benchRecord(i int) opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position(fmt.Sprintf("position-%d", i)),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{"foo": "bar"},
		Key:       opencdc.RawData("key"),
		Payload:   opencdc.Change{After: opencdc.StructuredData{"x": "y", "z": 3}},
	}
}

func newBenchAdapter(b *testing.B, batch DestinationWithBatch) *destinationPluginAdapter {
	b.Helper()
	adapter := NewDestinationPlugin(nopBenchDestination{}, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
	ctx := context.Background()
	adapter.configureWriteStrategy((&destinationWithBatch{}).setBatchConfig(ctx, batch))
	adapter.writeStrategy.SetStream(&benchAckStream{})
	return adapter
}

// BenchmarkDestinationWrite_Single is the default destination path: no batching
// configured, one record per Write (and one ack per record). The input slice is
// pre-built so the measured allocations are the SDK's, not the harness's.
func BenchmarkDestinationWrite_Single(b *testing.B) {
	adapter := newBenchAdapter(b, DestinationWithBatch{})
	recs := []opencdc.Record{benchRecord(0)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := adapter.writeStrategy.Write(context.Background(), recs); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDestinationWrite_Batch drives the batch strategy one record at a
// time (the shape the Run loop feeds it), so the flush work is amortized over
// the batch size.
func BenchmarkDestinationWrite_Batch(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			adapter := newBenchAdapter(b, DestinationWithBatch{BatchSize: size})
			recs := []opencdc.Record{benchRecord(0)}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := adapter.writeStrategy.Write(context.Background(), recs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
