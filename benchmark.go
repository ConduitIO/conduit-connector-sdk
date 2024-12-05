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
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"golang.org/x/sync/errgroup"
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

	bm.open = bm.measure(func() {
		err := bm.source.Open(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	})

	acks := make(chan opencdc.Record, b.N) // huge buffer so we don't delay reads
	var g errgroup.Group
	g.Go(func() error {
		return bm.acker(acks)
	})

	// measure first record read manually, it might be slower
	bm.firstRead = bm.measure(func() {
		rec, err := bm.source.Read(ctx)
		if err != nil {
			b.Fatal("Read:", err)
		}
		acks <- rec
	})

	bm.allReads = bm.measure(func() {
		for i := 0; i < b.N-1; i++ {
			rec, err := bm.source.Read(ctx)
			if err != nil {
				b.Fatal("Read:", err)
			}
			acks <- rec
		}
	})

	// stop
	var err error
	bm.stop = bm.measure(func() {
		close(acks)
		cancel()
		err = g.Wait()
	})
	if err != nil {
		b.Fatal("errgroup:", err)
	}

	// teardown
	bm.teardown = bm.measure(func() {
		err := bm.source.Teardown(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	})

	// report gathered metrics
	bm.reportMetrics(b)
}

func (bm *benchmarkSource) acker(c <-chan opencdc.Record) error {
	ctx := context.Background()

	rec := <-c // read first ack manually

	var err error
	bm.firstAck = bm.measure(func() {
		err = bm.source.Ack(ctx, rec.Position)
	})
	if err != nil {
		return fmt.Errorf("ack fail: %w", err)
	}

	bm.allAcks = bm.measure(func() {
		for rec := range c {
			err = bm.source.Ack(context.Background(), rec.Position)
			if err != nil {
				return
			}
		}
	})
	if err != nil {
		return fmt.Errorf("ack fail: %w", err)
	}

	return nil
}

func (*benchmarkSource) measure(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Since(start)
}

func (bm *benchmarkSource) reportMetrics(b *testing.B) {
	b.ReportMetric(0, "ns/op") // suppress ns/op metric, it is misleading in this benchmarkSource

	b.ReportMetric(bm.open.Seconds(), "open")
	b.ReportMetric(bm.stop.Seconds(), "stop")
	b.ReportMetric(bm.teardown.Seconds(), "teardown")

	b.ReportMetric(bm.firstRead.Seconds(), "firstRead")
	b.ReportMetric(float64(b.N-1)/bm.allReads.Seconds(), "reads/s")

	b.ReportMetric(bm.firstAck.Seconds(), "firstAck")
	b.ReportMetric(float64(b.N-1)/bm.allAcks.Seconds(), "acks/s")
}
