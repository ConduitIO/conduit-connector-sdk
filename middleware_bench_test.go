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
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-sdk/schema"
)

// nopBenchDestination is a minimal Destination implementation used to isolate
// middleware overhead from any actual I/O in benchmarks.
type nopBenchDestination struct {
	UnimplementedDestination
}

func (nopBenchDestination) Write(_ context.Context, recs []opencdc.Record) (int, error) {
	return len(recs), nil
}

// benchmarkSchemaTestData is the structured payload encoded/decoded by the
// schema middleware benchmarks below.
func benchmarkSchemaTestData() opencdc.StructuredData {
	return opencdc.StructuredData{
		"id":         "a1b2c3d4",
		"name":       "Jane Doe",
		"email":      "jane@example.com",
		"active":     true,
		"created_at": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		"balance":    1234.56,
	}
}

// BenchmarkDestinationWithSchemaExtraction_Write benchmarks the destination
// middleware that decodes a record's key and payload using a schema fetched
// from the schema service, run on every record on the destination write path
// when sdk.schema.extract.*.enabled is true (the default).
func BenchmarkDestinationWithSchemaExtraction_Write(b *testing.B) {
	ctx := context.Background()

	testData := benchmarkSchemaTestData()
	srd, err := avro.SerdeForType(testData)
	if err != nil {
		b.Fatal(err)
	}
	sch, err := schema.Create(ctx, schema.TypeAvro, "BenchmarkDestinationWithSchemaExtraction_Write", []byte(srd.String()))
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := sch.Marshal(testData)
	if err != nil {
		b.Fatal(err)
	}
	rawData := opencdc.RawData(encoded)

	schemaMetadata := opencdc.Metadata{
		opencdc.MetadataKeySchemaSubject:     sch.Subject,
		opencdc.MetadataKeySchemaVersion:     strconv.Itoa(sch.Version),
		opencdc.MetadataPayloadSchemaSubject: sch.Subject,
		opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
	}

	dw := (&DestinationWithSchemaExtraction{
		PayloadEnabled: lang.Ptr(true),
		KeyEnabled:     lang.Ptr(true),
	}).Wrap(nopBenchDestination{})

	b.Run("raw_avro_decode", func(b *testing.B) {
		// Records carry raw, avro-encoded key/payload plus the schema
		// subject/version in metadata: the realistic case where the source
		// side attached a schema and the destination must decode it.
		recs := make([]opencdc.Record, b.N)
		for i := range recs {
			recs[i] = opencdc.Record{
				Metadata: schemaMetadata,
				Key:      rawData,
				Payload:  opencdc.Change{After: rawData},
			}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := range recs {
			if _, err := dw.Write(ctx, recs[i:i+1]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("structured_passthrough", func(b *testing.B) {
		// Records already carry structured data (e.g. produced in-process by
		// a source middleware chain): decode() returns immediately without
		// touching the schema service, so this measures pure middleware
		// overhead.
		recs := make([]opencdc.Record, b.N)
		for i := range recs {
			recs[i] = opencdc.Record{
				Metadata: schemaMetadata,
				Key:      testData,
				Payload:  opencdc.Change{After: testData},
			}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := range recs {
			if _, err := dw.Write(ctx, recs[i:i+1]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSourceWithEncoding_Encode benchmarks the source middleware that
// encodes a record's structured key/payload using the schema referenced in
// its metadata, run on every record on the source read path once a schema
// has been extracted (see SourceWithSchemaExtraction).
func BenchmarkSourceWithEncoding_Encode(b *testing.B) {
	ctx := context.Background()

	testData := benchmarkSchemaTestData()
	srd, err := avro.SerdeForType(testData)
	if err != nil {
		b.Fatal(err)
	}
	sch, err := schema.Create(ctx, schema.TypeAvro, "BenchmarkSourceWithEncoding_Encode", []byte(srd.String()))
	if err != nil {
		b.Fatal(err)
	}

	schemaMetadata := opencdc.Metadata{
		opencdc.MetadataKeySchemaSubject:     sch.Subject,
		opencdc.MetadataKeySchemaVersion:     strconv.Itoa(sch.Version),
		opencdc.MetadataPayloadSchemaSubject: sch.Subject,
		opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
	}

	// Wrap(nil) is safe here: sourceWithEncoding.encode never touches the
	// wrapped Source, only Read/ReadN do, and those aren't exercised below.
	sw, ok := (SourceWithEncoding{}).Wrap(nil).(*sourceWithEncoding)
	if !ok {
		b.Fatal("Wrap did not return a *sourceWithEncoding")
	}

	b.Run("structured_avro_encode", func(b *testing.B) {
		recs := make([]opencdc.Record, b.N)
		for i := range recs {
			recs[i] = opencdc.Record{
				Metadata: schemaMetadata,
				Key:      testData,
				Payload:  opencdc.Change{Before: testData, After: testData},
			}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := range recs {
			if err := sw.encode(ctx, &recs[i]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("raw_passthrough", func(b *testing.B) {
		// Records with unstructured data are left untouched (only a one-time
		// warning is logged), measuring pure middleware overhead.
		rawData := opencdc.RawData(`{"id":"a1b2c3d4"}`)
		recs := make([]opencdc.Record, b.N)
		for i := range recs {
			recs[i] = opencdc.Record{
				Metadata: schemaMetadata,
				Key:      rawData,
				Payload:  opencdc.Change{Before: rawData, After: rawData},
			}
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := range recs {
			if err := sw.encode(ctx, &recs[i]); err != nil {
				b.Fatal(err)
			}
		}
	})
}
