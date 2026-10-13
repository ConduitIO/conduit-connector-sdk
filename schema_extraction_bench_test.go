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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
)

// realisticPayload returns a structured payload of 16 fields with mixed types,
// roughly what a database or generator source produces: scalars, a timestamp,
// bytes, a nullable value and one nested record.
func realisticPayload() opencdc.StructuredData {
	return opencdc.StructuredData{
		"id":         int64(9001),
		"uuid":       "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"name":       "Jane Doe",
		"email":      "jane@example.com",
		"active":     true,
		"age":        int32(41),
		"balance":    1234.56,
		"ratio":      float32(0.25),
		"created_at": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		"updated_at": time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC),
		"avatar":     []byte{1, 2, 3, 4},
		"note":       nil,
		"tags":       []any{"a", "b"},
		"address": opencdc.StructuredData{
			"street": "1 Main St",
			"zip":    int64(94107),
		},
		"score":   int64(77),
		"country": "US",
	}
}

// stubBenchSource hands out pre-built records so a benchmark can run the real
// source middleware chain without a connector behind it.
type stubBenchSource struct {
	UnimplementedSource
	recs []opencdc.Record
	i    int
}

func (s *stubBenchSource) Read(context.Context) (opencdc.Record, error) {
	r := s.recs[s.i]
	s.i++
	return r, nil
}

func newBenchRecords(n int, payload opencdc.StructuredData) []opencdc.Record {
	recs := make([]opencdc.Record, n)
	for i := range recs {
		recs[i] = opencdc.Record{
			// Metadata is per record, the middleware mutates it.
			Metadata: opencdc.Metadata{opencdc.MetadataCollection: "users"},
			Key:      opencdc.StructuredData{"id": int64(i)},
			Payload:  opencdc.Change{After: payload},
		}
	}
	return recs
}

func newBenchExtraction() *SourceWithSchemaExtraction {
	return &SourceWithSchemaExtraction{
		SchemaTypeStr:  "avro",
		PayloadEnabled: lang.Ptr(true),
		PayloadSubject: lang.Ptr("payload"),
		KeyEnabled:     lang.Ptr(true),
		KeySubject:     lang.Ptr("key"),
	}
}

// BenchmarkSourceWithSchemaExtraction_Extract measures only the extraction
// step: infer a schema from a record's key and payload, register it and attach
// the subject and version to the metadata.
func BenchmarkSourceWithSchemaExtraction_Extract(b *testing.B) {
	ctx := context.Background()

	b.Run("structured16", func(b *testing.B) {
		sw, ok := newBenchExtraction().Wrap(nil).(*sourceWithSchemaExtraction)
		if !ok {
			b.Fatal("unexpected middleware type")
		}
		recs := newBenchRecords(b.N, realisticPayload())
		b.ReportAllocs()
		b.ResetTimer()
		for i := range recs {
			if err := sw.extractAttachKeySchema(ctx, &recs[i]); err != nil {
				b.Fatal(err)
			}
			if err := sw.extractAttachPayloadSchema(ctx, &recs[i]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("raw", func(b *testing.B) {
		// Raw data is not extracted (one warning, then a type check).
		sw, ok := newBenchExtraction().Wrap(nil).(*sourceWithSchemaExtraction)
		if !ok {
			b.Fatal("unexpected middleware type")
		}
		recs := make([]opencdc.Record, b.N)
		for i := range recs {
			recs[i] = opencdc.Record{
				Metadata: opencdc.Metadata{},
				Key:      opencdc.RawData("k"),
				Payload:  opencdc.Change{After: opencdc.RawData(`{"id":1,"name":"Jane Doe"}`)},
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range recs {
			if err := sw.extractAttachKeySchema(ctx, &recs[i]); err != nil {
				b.Fatal(err)
			}
			if err := sw.extractAttachPayloadSchema(ctx, &recs[i]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSourceSchemaChain_Read measures the default source schema
// middleware pair (extraction, then encoding) end to end for one record, which
// is what the connector pays per record by default.
func BenchmarkSourceSchemaChain_Read(b *testing.B) {
	ctx := context.Background()
	stub := &stubBenchSource{recs: newBenchRecords(b.N, realisticPayload())}
	// Same order as SourceWithMiddleware: extraction wraps the source,
	// encoding wraps extraction.
	src := (&SourceWithEncoding{}).Wrap(newBenchExtraction().Wrap(stub))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := src.Read(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDestinationSchemaChain_Write measures the default destination
// schema middleware for one record: look up the schema referenced in the
// metadata and decode raw key and payload with it.
func BenchmarkDestinationSchemaChain_Write(b *testing.B) {
	ctx := context.Background()

	// Produce wire-format records with the real source chain.
	n := 1024
	stub := &stubBenchSource{recs: newBenchRecords(n, realisticPayload())}
	src := (&SourceWithEncoding{}).Wrap(newBenchExtraction().Wrap(stub))
	encoded := make([]opencdc.Record, n)
	for i := range encoded {
		rec, err := src.Read(ctx)
		if err != nil {
			b.Fatal(err)
		}
		encoded[i] = rec
	}

	dw := (&DestinationWithSchemaExtraction{
		PayloadEnabled: lang.Ptr(true),
		KeyEnabled:     lang.Ptr(true),
	}).Wrap(nopBenchDestination{})

	recs := make([]opencdc.Record, b.N)
	for i := range recs {
		r := encoded[i%n]
		r.Metadata = opencdc.Metadata{}
		for k, v := range encoded[i%n].Metadata {
			r.Metadata[k] = v
		}
		recs[i] = r
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range recs {
		if _, err := dw.Write(ctx, recs[i:i+1]); err != nil {
			b.Fatal(err)
		}
	}
}
