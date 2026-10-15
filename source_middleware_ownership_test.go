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
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// sliceReusingSource returns at most perCall records per ReadN call and
// reuses the same backing array for every returned slice, which is valid when
// the caller is done with a batch before calling again. Each record gets a
// freshly allocated position, so only the slice is shared between calls.
type sliceReusingSource struct {
	UnimplementedSource

	perCall int
	buf     []opencdc.Record
	n       int

	// filled receives one value after every ReadN call has written its
	// records into buf, so a test can wait for the read-ahead without sleeping.
	filled chan struct{}
}

func (s *sliceReusingSource) Open(context.Context, opencdc.Position) error { return nil }

func (s *sliceReusingSource) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n = min(n, s.perCall)
	s.buf = s.buf[:0]
	for range n {
		s.n++
		s.buf = append(s.buf, opencdc.Record{Position: opencdc.Position(fmt.Sprintf("p%04d", s.n))})
	}
	s.filled <- struct{}{}
	return s.buf, nil
}

// TestSourceWithBatch_ConnectorReusesReturnedSlice checks that a batch the
// middleware handed out is not overwritten by its read-ahead call, whether
// the batch was returned as is (full) or assembled from several reads. The
// consumer keeps a batch for a while before asking for the next one, like the
// adapter does while it sends. Invariant 3: a lost batch is lost records.
// See conduit-connector-sdk#421.
func TestSourceWithBatch_ConnectorReusesReturnedSlice(t *testing.T) {
	testCases := []struct {
		name  string
		size  int
		delay time.Duration
		// perCall is how many records each ReadN call returns, callsPerBatch
		// how many calls make up one batch handed to the consumer.
		perCall       int
		callsPerBatch int
		readAhead     bool
	}{
		{name: "batching off", perCall: 2, callsPerBatch: 1},
		{name: "full batch returned as is", size: 2, delay: time.Second, perCall: 2, callsPerBatch: 1, readAhead: true},
		{name: "batch assembled from several reads", size: 4, delay: time.Second, perCall: 2, callsPerBatch: 2, readAhead: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			impl := &sliceReusingSource{
				perCall: tc.perCall,
				buf:     make([]opencdc.Record, 0, tc.perCall),
				filled:  make(chan struct{}, 1024),
			}
			src := (&SourceWithBatch{
				BatchSize:  lang.Ptr(tc.size),
				BatchDelay: lang.Ptr(tc.delay),
			}).Wrap(impl)
			is.NoErr(src.Open(ctx, nil))

			var seen []string
			filled := 0
			for i := 1; i <= 8; i++ {
				batch, err := src.ReadN(ctx, 1)
				is.NoErr(err)
				is.True(len(batch) > 0)

				before := positions(batch)
				if tc.readAhead {
					// The consumer is busy sending. Wait until the middleware
					// has called the connector again, which rewrites the
					// connector's buffer.
					for filled < i*tc.callsPerBatch+1 {
						select {
						case <-impl.filled:
							filled++
						case <-time.After(5 * time.Second):
							t.Fatal("read-ahead ReadN call did not happen")
						}
					}
				}
				is.Equal(before, positions(batch)) // the batch we hold must not change under us

				seen = append(seen, before...)
			}

			for i, p := range seen {
				is.Equal(p, fmt.Sprintf("p%04d", i+1)) // no gaps, no repeats, in order
			}
		})
	}
}

func positions(batch []opencdc.Record) []string {
	out := make([]string, len(batch))
	for i, r := range batch {
		out[i] = string(r.Position)
	}
	return out
}
