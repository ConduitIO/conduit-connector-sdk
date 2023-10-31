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

package sdk

import (
	"context"
	"sync"
	"testing"
	"time"
)

// BenchmarkSource is a benchmark that any source implementation can run to figure
// out its performance. The benchmark expects that the source resource contains
// at least b.N number of records. This should be prepared before the benchmark
// is executed.
// The function should be manually called from a benchmark function:
//
//	func BenchmarkConnector(b *testing.B) {
//	    // set up test dependencies and write b.N records to source resource ...
//	    sdk.BenchmarkSource(
//	        b,
//	        mySourceConnector,
//	        map[string]string{...}, // valid source config
//	    )
//	}
//
// The benchmark can be run with a specific number of records by supplying the
// option -benchtime=Nx, where N is the number of records to be benchmarked
// (e.g. -benchtime=100x benchmarks reading 100 records).
func BenchmarkSource(
	b *testing.B,
	s Source,
	cfg map[string]string,
) {
	bm := benchmarkSource{
		source: s,
		config: cfg,
	}
	bm.Run(b)
}

type benchmarkSource struct {
	source Source
	config map[string]string

	// measures
	configure time.Duration
	open      time.Duration
	firstRead time.Duration
	allReads  time.Duration
	stop      time.Duration
	teardown  time.Duration
	firstAck  time.Duration
	allAcks   time.Duration
}

func (bm *benchmarkSource) Run(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bm.measure(&bm.configure, func() {
		err := bm.source.Configure(ctx, bm.config)
		if err != nil {
			b.Fatal(err)
		}
	})

	bm.measure(&bm.open, func() {
		err := bm.source.Open(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	})

	acks := make(chan Record, b.N) // huge buffer so we don't delay reads
	var wg sync.WaitGroup
	wg.Add(1)
	go bm.acker(b, acks, &wg)

	// measure first record read manually, it might be slower
	bm.measure(&bm.firstRead, func() {
		rec, err := bm.source.Read(ctx)
		if err != nil {
			b.Fatal("Read: ", err)
		}
		acks <- rec
	})

	b.ResetTimer() // we are only interested in measuring the read times
	bm.measure(&bm.allReads, func() {
		for i := 0; i < b.N-1; i++ {
			rec, err := bm.source.Read(ctx)
			if err != nil {
				b.Fatal("Read: ", err)
			}
			acks <- rec
		}
	})
	b.StopTimer()

	// stop
	bm.measure(&bm.stop, func() {
		close(acks)
		cancel()
		wg.Wait()
	})

	// teardown
	bm.measure(&bm.teardown, func() {
		err := bm.source.Teardown(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	})

	// report gathered metrics
	bm.reportMetrics(b)
}

func (bm *benchmarkSource) acker(b *testing.B, c <-chan Record, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()

	rec := <-c // read first ack manually
	bm.measure(&bm.firstAck, func() {
		err := bm.source.Ack(ctx, rec.Position)
		if err != nil {
			b.Fatal("Ack: ", err)
		}
	})

	count := 0
	bm.measure(&bm.allAcks, func() {
		for rec := range c {
			count++
			err := bm.source.Ack(context.Background(), rec.Position)
			if err != nil {
				b.Fatal("Ack: ", err)
			}
		}
	})
}

func (*benchmarkSource) measure(out *time.Duration, f func()) {
	start := time.Now()
	f()
	*out = time.Since(start)
}

func (bm *benchmarkSource) reportMetrics(b *testing.B) {
	b.ReportMetric(0, "ns/op") // suppress ns/op metric, it is misleading in this benchmarkSource
	b.ReportMetric(bm.configure.Seconds(), "configure")
	b.ReportMetric(bm.open.Seconds(), "open")
	b.ReportMetric(bm.firstRead.Seconds(), "firstRead")
	b.ReportMetric(bm.stop.Seconds(), "stop")
	b.ReportMetric(bm.teardown.Seconds(), "teardown")
	b.ReportMetric(float64(b.N-1)/b.Elapsed().Seconds(), "msg/s")
	b.ReportMetric(bm.firstAck.Seconds(), "firstAck")
	b.ReportMetric(float64(b.N-1)/bm.allAcks.Seconds(), "acks/s")
}
