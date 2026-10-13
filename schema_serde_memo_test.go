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
	"sync"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
)

func schemaFor(t *testing.T, subject string, sample opencdc.StructuredData) schema.Schema {
	t.Helper()
	srd, err := avro.SerdeForType(sample)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := schema.Create(context.Background(), schema.TypeAvro, subject, []byte(srd.String()))
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func TestSchemaSerdeMemo_SameResultAsSchema(t *testing.T) {
	is := is.New(t)
	var m schemaSerdeMemo

	data := opencdc.StructuredData{"id": int64(1), "name": "a"}
	sch := schemaFor(t, "memo-same-result", data)
	encoded, err := sch.Marshal(data)
	is.NoErr(err)

	for range 3 { // miss, then hits
		var got, want opencdc.StructuredData
		is.NoErr(m.unmarshal(sch, encoded, &got))
		is.NoErr(sch.Unmarshal(encoded, &want))
		is.Equal(got, want)
	}

	// failures read the same as without the memo
	truncated := []byte{0x02, 0xfe, 0xff, 0xff, 0xff, 0x0f} // a string claiming to be ~1GB long
	errMemo := m.unmarshal(sch, truncated, new(opencdc.StructuredData))
	errPlain := sch.Unmarshal(truncated, new(opencdc.StructuredData))
	is.True(errMemo != nil)
	is.Equal(errMemo.Error(), errPlain.Error())

	badSchema := schema.Schema{Subject: "memo-bad", Version: 1, Type: schema.TypeAvro, Bytes: []byte("not a schema")}
	errMemo = m.unmarshal(badSchema, encoded, new(opencdc.StructuredData))
	errPlain = badSchema.Unmarshal(encoded, new(opencdc.StructuredData))
	is.True(errMemo != nil)
	is.Equal(errMemo.Error(), errPlain.Error())
}

// TestSchemaSerdeMemo_NeverServesStaleSerde: if the schema service returns
// other bytes for a subject and version we've seen, the memo must follow it.
func TestSchemaSerdeMemo_NeverServesStaleSerde(t *testing.T) {
	is := is.New(t)
	var m schemaSerdeMemo

	a := schemaFor(t, "memo-stale-a", opencdc.StructuredData{"id": int64(1)})
	b := schemaFor(t, "memo-stale-b", opencdc.StructuredData{"id": "x"})
	// b pretends to be the same subject and version as a
	b.Subject, b.Version, b.ID = a.Subject, a.Version, a.ID

	encodedA, err := a.Marshal(opencdc.StructuredData{"id": int64(7)})
	is.NoErr(err)
	encodedB, err := b.Marshal(opencdc.StructuredData{"id": "y"})
	is.NoErr(err)

	var out opencdc.StructuredData
	is.NoErr(m.unmarshal(a, encodedA, &out))
	is.Equal(out["id"], int64(7))

	out = nil
	is.NoErr(m.unmarshal(b, encodedB, &out)) // same key, other bytes
	is.Equal(out["id"], "y")

	out = nil
	is.NoErr(m.unmarshal(a, encodedA, &out)) // and back
	is.Equal(out["id"], int64(7))
}

func TestSchemaSerdeMemo_Bounded(t *testing.T) {
	is := is.New(t)
	var m schemaSerdeMemo
	sch := schemaFor(t, "memo-bounded", opencdc.StructuredData{"id": int64(1)})
	for i := range 5 * serdeMemoEntries {
		sch.Version = i + 1 // distinct keys, same bytes
		_, err := m.serde(sch)
		is.NoErr(err)
		is.True(len(m.m) <= serdeMemoEntries)
	}
}

func TestSchemaSerdeMemo_ByteBudget(t *testing.T) {
	is := is.New(t)
	var m schemaSerdeMemo
	sch := schemaFor(t, "memo-bytes", opencdc.StructuredData{"id": int64(1)})

	// Pretend the schema text is large by padding the JSON with whitespace.
	pad := make([]byte, 600<<10)
	for i := range pad {
		pad[i] = ' '
	}
	sch.Bytes = append(sch.Bytes, pad...)
	for i := range 200 {
		sch.Version = i + 1
		_, err := m.serde(sch)
		is.NoErr(err)
		is.True(m.bytes <= serdeMemoBytes)
	}
	is.True(len(m.m) > 0)

	// too large to memoize: still works, nothing is kept
	var m2 schemaSerdeMemo
	huge := sch
	huge.Bytes = append(append([]byte{}, sch.Bytes...), make([]byte, serdeMemoBytes/16)...)
	for i := range huge.Bytes[len(sch.Bytes):] {
		huge.Bytes[len(sch.Bytes)+i] = ' '
	}
	_, err := m2.serde(huge)
	is.NoErr(err)
	is.Equal(len(m2.m), 0)
	is.Equal(m2.bytes, 0)
}

func TestSchemaSerdeMemo_HitDoesNotAllocate(t *testing.T) {
	var m schemaSerdeMemo
	sch := schemaFor(t, "memo-allocs", realisticPayload())
	if _, err := m.serde(sch); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = m.serde(sch) }); allocs != 0 {
		t.Fatalf("a hit allocates %v times, want 0", allocs)
	}
}

func TestSchemaSerdeMemo_Concurrent(t *testing.T) {
	var m schemaSerdeMemo
	n := 2 * serdeMemoEntries // more than the memo holds, to force resets
	schemas, encoded := make([]schema.Schema, 0, n), make([][]byte, 0, n)
	for i := range n {
		data := opencdc.StructuredData{"id": int64(i), "f" + strconv.Itoa(i%5): "x"}
		sch := schemaFor(t, "memo-concurrent-"+strconv.Itoa(i), data)
		enc, err := sch.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		schemas, encoded = append(schemas, sch), append(encoded, enc)
	}

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				j := (i*7 + w) % len(schemas)
				var out opencdc.StructuredData
				if err := m.unmarshal(schemas[j], encoded[j], &out); err != nil {
					t.Error(err)
					return
				}
				if out["id"] != int64(j) {
					t.Errorf("decoded %v with the schema of record %d", out, j)
					return
				}
			}
		}()
	}
	wg.Wait()
}
