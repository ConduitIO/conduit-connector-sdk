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
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-protocol/pconnutils"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
)

// -- generator ----------------------------------------------------------------

// shapeGen generates random record values from a deliberately small vocabulary
// (few field names, few types), so that equal shapes recur often and shape
// changes are subtle (one field's type changes, one field disappears, ...).
type shapeGen struct {
	r *rand.Rand
	// unsupported enables values that the shape key refuses to encode.
	unsupported bool
	// narrow limits the vocabulary even further, so that shapes repeat a lot.
	narrow bool
}

var shapeGenFields = []string{"id", "name", "a", "b", "tags", "meta"}

type shapeGenStruct struct {
	A string `json:"a"`
	B int    `json:"b"`
}

func (g *shapeGen) scalar() any {
	if g.narrow {
		return []any{nil, "s", int64(1), 1.5}[g.r.IntN(4)]
	}
	n := 16
	if g.unsupported {
		n = 21
	}
	switch g.r.IntN(n) {
	case 0:
		return nil
	case 1:
		return true
	case 2:
		return g.r.IntN(100)
	case 3:
		return int32(g.r.IntN(100))
	case 4:
		return int64(g.r.IntN(100))
	case 5:
		return uint8(g.r.IntN(100))
	case 6:
		return uint32(g.r.IntN(100))
	case 7:
		return float32(g.r.Float32())
	case 8:
		return g.r.Float64()
	case 9, 10:
		return strconv.Itoa(g.r.IntN(1000))
	case 11:
		return []byte{byte(g.r.IntN(256))}
	case 12:
		return time.Unix(int64(g.r.IntN(1e9)), 0).UTC()
	case 13:
		return time.Duration(g.r.IntN(1e6))
	case 14:
		return int16(g.r.IntN(100))
	case 15:
		return int8(g.r.IntN(100))
	// unsupported by the shape key from here on
	case 16:
		return uint64(g.r.IntN(100)) // makes inference fail
	case 17:
		return lang.Ptr("ptr")
	case 18:
		return []string{"a", "b"}
	case 19:
		return shapeGenStruct{A: "a", B: 1}
	default:
		return map[string]int{"x": 1}
	}
}

func (g *shapeGen) value(depth int) any {
	if depth >= 3 || (g.narrow && depth >= 2) {
		return g.scalar()
	}
	switch g.r.IntN(8) {
	case 0:
		return g.record(depth + 1)
	case 1:
		m := map[string]any{}
		for range g.r.IntN(3) {
			m[shapeGenFields[g.r.IntN(len(shapeGenFields))]] = g.value(depth + 1)
		}
		return m
	case 2:
		s := make([]any, g.r.IntN(4))
		for i := range s {
			s[i] = g.value(depth + 1)
		}
		return s
	default:
		return g.scalar()
	}
}

func (g *shapeGen) record(depth int) opencdc.StructuredData {
	rec := opencdc.StructuredData{}
	fields := shapeGenFields
	if g.narrow {
		fields = fields[:3]
	}
	for _, f := range fields {
		if g.r.IntN(3) == 0 {
			continue
		}
		rec[f] = g.value(depth)
	}
	return rec
}

// -- reference ----------------------------------------------------------------

// inferUncached is the pre-existing inference, without any cache: build a serde
// from the value and take its textual form, exactly like schemaForType does.
func inferUncached(v any) (string, error) {
	srd, err := avro.SerdeForType(v)
	if err != nil {
		return "", err
	}
	return srd.String(), nil
}

// inferAchievable reports whether uncached inference of v can produce the given
// outcome (a schema, or a failure).
//
// Inference is not deterministic for every value: conduit-commons builds record
// fields in map iteration order and only sorts them afterwards, so a
// map[string]any or []any holding two records with two or more fields each
// fails to deduplicate them about one time in five ("can't combine schemas of
// type *avro.RecordSchema"). For such a value both a schema and an error are
// legitimate results of the uncached path, so the tests ask whether the
// outcome is one the uncached path can produce, instead of comparing against a
// single run. For deterministic values the first attempt decides.
func inferAchievable(v any, wantSchema string, wantFailure bool) bool {
	for range 200 {
		got, err := inferUncached(v)
		if (err != nil) == wantFailure && (wantFailure || got == wantSchema) {
			return true
		}
	}
	return false
}

func shapeKeyOf(v any) (string, bool) {
	kb := getShapeKeyBuilder()
	defer putShapeKeyBuilder(kb)
	if !kb.appendShape(v, 0) {
		return "", false
	}
	return string(kb.buf), true
}

// deepCopy copies the types the generator produces for the cacheable shapes.
// (opencdc.StructuredData.Clone does not copy nested []any and map[string]any,
// and Avro marshaling rewrites those in place.)
func deepCopy[T any](v T) T {
	var out any
	switch v := any(v).(type) {
	case opencdc.StructuredData:
		m := make(opencdc.StructuredData, len(v))
		for k, e := range v {
			m[k] = deepCopy(e)
		}
		out = m
	case map[string]any:
		m := make(map[string]any, len(v))
		for k, e := range v {
			m[k] = deepCopy(e)
		}
		out = m
	case []any:
		sl := make([]any, len(v))
		for i, e := range v {
			sl[i] = deepCopy(e)
		}
		out = sl
	case []byte:
		out = append([]byte(nil), v...)
	default:
		out = v
	}
	res, _ := out.(T)
	return res
}

// hasDynamicContainer reports whether v contains a map[string]any or []any.
// Avro encodes those as unions whose branch is resolved at marshal time, and
// map entries are written in map iteration order, so two encodings of the same
// value under the same schema are not guaranteed to be byte-identical (this is
// independent of the cache). Records of scalars and nested records are.
func hasDynamicContainer(v any) bool {
	switch v := v.(type) {
	case opencdc.StructuredData:
		for _, e := range v {
			if hasDynamicContainer(e) {
				return true
			}
		}
		return false
	case map[string]any, []any:
		return true
	default:
		return false
	}
}

// newTestExtraction returns an extraction middleware that registers schemas
// under subjects unique to the test. The default in-memory schema service is
// process wide, and other tests assert on the versions of the default subjects.
func newTestExtraction(t *testing.T) *sourceWithSchemaExtraction {
	t.Helper()
	cfg := newBenchExtraction()
	cfg.PayloadSubject = lang.Ptr("payload-" + t.Name())
	cfg.KeySubject = lang.Ptr("key-" + t.Name())
	sw, ok := cfg.Wrap(nil).(*sourceWithSchemaExtraction)
	if !ok {
		t.Fatal("unexpected middleware type")
	}
	return sw
}

// -- properties ---------------------------------------------------------------

// TestSchemaShapeKey_EqualKeyImpliesEqualSchema is the core safety property:
// whenever two values have the same shape key, uncached inference produces the
// same schema (or the same failure) for both. If this fails, the cache could
// attach a wrong schema to a record (invariant 6).
func TestSchemaShapeKey_EqualKeyImpliesEqualSchema(t *testing.T) {
	for _, mode := range []struct{ unsupported, narrow bool }{{false, false}, {true, false}, {false, true}} {
		t.Run(fmt.Sprintf("unsupported=%v,narrow=%v", mode.unsupported, mode.narrow), func(t *testing.T) {
			seed := rand.Uint64()
			t.Logf("seed: %d", seed)
			g := &shapeGen{r: rand.New(rand.NewPCG(seed, 1)), unsupported: mode.unsupported, narrow: mode.narrow}

			firstByKey := map[string]any{}
			var keyed, repeats int
			for range 30000 {
				v := g.record(0)
				key, ok := shapeKeyOf(v)
				if !ok {
					continue // not cacheable, nothing to prove
				}
				keyed++
				first, seen := firstByKey[key]
				if !seen {
					firstByKey[key] = v
					continue
				}
				repeats++
				if repeats > 2000 {
					continue // enough verified, keep the run short
				}
				got, err := inferUncached(v)
				if !inferAchievable(first, got, err != nil) {
					t.Fatalf("same shape key, different schemas (seed %d)\nvalue: %#v\nfirst: %#v\nnow:   %q (%v)", seed, v, first, got, err)
				}
			}
			minRepeats := 100
			if mode.narrow {
				minRepeats = 1000
			}
			if repeats < minRepeats {
				t.Fatalf("generator produced too few repeated shapes (%d of %d keyed)", repeats, keyed)
			}
			t.Logf("%d keyed values, %d distinct shapes, %d repeats", keyed, len(firstByKey), repeats)
		})
	}
}

// TestSourceWithSchemaExtraction_CachedMatchesUncached runs random records
// through the real extraction middleware (with the cache) and checks that the
// schema attached to every record is byte-identical to the schema uncached
// inference produces, and that the record encodes to byte-identical output
// under it.
func TestSourceWithSchemaExtraction_CachedMatchesUncached(t *testing.T) {
	ctx := context.Background()
	seed := rand.Uint64()
	t.Logf("seed: %d", seed)
	g := &shapeGen{r: rand.New(rand.NewPCG(seed, 2)), unsupported: true}
	gNarrow := &shapeGen{r: rand.New(rand.NewPCG(seed, 4)), narrow: true}

	sw := newTestExtraction(t)
	collection := "prop-" + t.Name()

	var hits int
	for i := range 3000 {
		gen := g
		if i%2 == 0 {
			gen = gNarrow // mostly cache hits
		}
		payload := gen.record(0)
		key := gen.record(0)
		rec := opencdc.Record{
			Metadata: opencdc.Metadata{opencdc.MetadataCollection: collection},
			Key:      key,
			Payload:  opencdc.Change{After: payload},
		}

		before := sw.shapes.len()

		errKey := sw.extractAttachKeySchema(ctx, &rec)
		errPayload := sw.extractAttachPayloadSchema(ctx, &rec)
		if sw.shapes.len() == before {
			hits++
		}

		checkExtracted(t, ctx, i, seed, "key", errKey, rec.Metadata.GetKeySchemaSubject, rec.Metadata.GetKeySchemaVersion, key)
		checkExtracted(t, ctx, i, seed, "payload", errPayload, rec.Metadata.GetPayloadSchemaSubject, rec.Metadata.GetPayloadSchemaVersion, payload)
	}
	if hits == 0 {
		t.Fatal("cache was never hit, the property is vacuous")
	}
}

// checkExtracted verifies the outcome of extracting the schema of data: if it
// failed, uncached inference must be able to fail; if it succeeded, the
// attached schema must be one uncached inference produces, and the data must
// encode identically under it and under a schema registered from the uncached
// result.
func checkExtracted(
	t *testing.T, ctx context.Context, i int, seed uint64, name string, extractErr error,
	getSubject func() (string, error), getVersion func() (int, error), data opencdc.StructuredData,
) {
	t.Helper()
	if extractErr != nil {
		if !inferAchievable(data, "", true) {
			t.Fatalf("#%d %s (seed %d): cached path failed (%v), uncached inference never does\nvalue: %#v", i, name, seed, extractErr, data)
		}
		return
	}
	subject, err := getSubject()
	if err != nil {
		t.Fatal(err)
	}
	version, err := getVersion()
	if err != nil {
		t.Fatal(err)
	}
	got, err := schema.Get(ctx, subject, version)
	if err != nil {
		t.Fatal(err)
	}
	if !inferAchievable(data, string(got.Bytes), false) {
		t.Fatalf("#%d %s (seed %d): attached schema is not one uncached inference produces\nvalue: %#v\ngot: %s", i, name, seed, data, got.Bytes)
	}

	// Encodings must be identical too. Only values the cache handles are
	// compared: for the others the code path is the same as before, and
	// values holding shared pointers can't be marshaled twice anyway.
	if _, cacheable := shapeKeyOf(data); !cacheable {
		return
	}
	ref, err := schema.Create(ctx, schema.TypeAvro, subject+"-ref", got.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	gotEnc, err1 := got.Marshal(deepCopy(data))
	wantEnc, err2 := ref.Marshal(deepCopy(data))
	if (err1 != nil) != (err2 != nil) {
		t.Fatalf("#%d %s (seed %d): encode errors differ: %v vs %v", i, name, seed, err1, err2)
	}
	if hasDynamicContainer(data) {
		// Encodings of such values may legitimately differ in byte order, they
		// must decode to the same value.
		var a, b opencdc.StructuredData
		errA := ref.Unmarshal(gotEnc, &a)
		errB := ref.Unmarshal(wantEnc, &b)
		if (errA != nil) != (errB != nil) {
			// (the Avro library can't read back every union of maps it writes,
			// that has nothing to do with the cache)
			t.Fatalf("#%d %s (seed %d): decode errors differ: %v vs %v", i, name, seed, errA, errB)
		}
		if errA == nil && !reflect.DeepEqual(a, b) {
			t.Fatalf("#%d %s (seed %d): decoded encodings differ for value %#v", i, name, seed, data)
		}
	} else if string(gotEnc) != string(wantEnc) {
		t.Fatalf("#%d %s (seed %d): encodings differ for value %#v", i, name, seed, data)
	}
}

// -- shape changes --------------------------------------------------------------

func TestSourceWithSchemaExtraction_ShapeChange(t *testing.T) {
	ctx := context.Background()
	sw := newTestExtraction(t)
	collection := "shape-change"

	extract := func(t *testing.T, payload opencdc.StructuredData) schema.Schema {
		t.Helper()
		is := is.New(t)
		rec := opencdc.Record{
			Metadata: opencdc.Metadata{opencdc.MetadataCollection: collection},
			Payload:  opencdc.Change{After: payload},
		}
		is.NoErr(sw.extractAttachPayloadSchema(ctx, &rec))
		subject, err := rec.Metadata.GetPayloadSchemaSubject()
		is.NoErr(err)
		version, err := rec.Metadata.GetPayloadSchemaVersion()
		is.NoErr(err)
		sch, err := schema.Get(ctx, subject, version)
		is.NoErr(err)
		is.True(inferAchievable(payload, string(sch.Bytes), false)) // schema must match uncached inference
		return sch
	}

	base := opencdc.StructuredData{"id": int64(1), "name": "a"}
	steps := []struct {
		name    string
		payload opencdc.StructuredData
	}{
		{"base", base},
		{"field added", opencdc.StructuredData{"id": int64(1), "name": "a", "age": int64(3)}},
		{"field removed", opencdc.StructuredData{"id": int64(1)}},
		{"type changed int to string", opencdc.StructuredData{"id": "1", "name": "a"}},
		{"type changed long to int", opencdc.StructuredData{"id": int32(1), "name": "a"}},
		{"value became null", opencdc.StructuredData{"id": int64(1), "name": nil}},
		{"nested record changed", opencdc.StructuredData{"id": int64(1), "name": "a", "n": opencdc.StructuredData{"x": int64(1)}}},
		{"nested record field type changed", opencdc.StructuredData{"id": int64(1), "name": "a", "n": opencdc.StructuredData{"x": "1"}}},
		{"array item type changed", opencdc.StructuredData{"id": int64(1), "l": []any{int64(1)}}},
		{"array item type changed again", opencdc.StructuredData{"id": int64(1), "l": []any{"1"}}},
		{"record vs map with same keys", opencdc.StructuredData{"id": int64(1), "l": map[string]any{"x": int64(1)}}},
		{"record vs map with same keys, 2", opencdc.StructuredData{"id": int64(1), "l": opencdc.StructuredData{"x": int64(1)}}},
		{"time vs long", opencdc.StructuredData{"id": time.Unix(1, 0).UTC()}},
		{"duration vs long", opencdc.StructuredData{"id": time.Second}},
		{"bytes vs string", opencdc.StructuredData{"id": []byte("1")}},
	}

	// Go through the steps twice, in order and then reversed, so that every
	// shape is seen as a miss and as a hit, after other shapes were cached.
	seen := map[string]schema.Schema{}
	order := append(append([]int{}, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),
		14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
	for _, i := range order {
		step := steps[i]
		t.Run(step.name, func(t *testing.T) {
			sch := extract(t, step.payload)
			if prev, ok := seen[step.name]; ok {
				// same shape again: same registered schema
				is.New(t).Equal(prev.Version, sch.Version)
				is.New(t).Equal(prev.ID, sch.ID)
			}
			seen[step.name] = sch
		})
	}

	// all shapes above are distinct, so none may share a registered schema
	versions := map[int]string{}
	for name, sch := range seen {
		if other, ok := versions[sch.Version]; ok {
			t.Errorf("%q and %q were given the same schema version %d", name, other, sch.Version)
		}
		versions[sch.Version] = name
	}
}

func TestSourceWithSchemaExtraction_DifferentSubjectsAreNotShared(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	sw := newTestExtraction(t)
	base := *sw.config.PayloadSubject
	keyBase := *sw.config.KeySubject
	payload := opencdc.StructuredData{"id": int64(1)}

	subjectFor := func(ctx context.Context, md opencdc.Metadata) string {
		rec := opencdc.Record{Metadata: md, Payload: opencdc.Change{After: payload.Clone()}}
		is.NoErr(sw.extractAttachPayloadSchema(ctx, &rec))
		s, err := rec.Metadata.GetPayloadSchemaSubject()
		is.NoErr(err)
		return s
	}

	// same shape, different collection / context / no collection
	is.Equal(subjectFor(ctx, opencdc.Metadata{opencdc.MetadataCollection: "orders"}), "orders."+base)
	is.Equal(subjectFor(ctx, opencdc.Metadata{opencdc.MetadataCollection: "users"}), "users."+base)
	is.Equal(subjectFor(ctx, opencdc.Metadata{}), base)
	// an empty collection counts as no collection
	is.Equal(subjectFor(ctx, opencdc.Metadata{opencdc.MetadataCollection: ""}), base)
	is.Equal(subjectFor(schema.WithSchemaContextName(ctx, "ctx1"), opencdc.Metadata{}), "ctx1:"+base)
	is.Equal(subjectFor(schema.WithSchemaContextName(ctx, "ctx2"), opencdc.Metadata{}), "ctx2:"+base)
	// and again, now served from the cache
	is.Equal(subjectFor(ctx, opencdc.Metadata{opencdc.MetadataCollection: "orders"}), "orders."+base)
	is.Equal(subjectFor(schema.WithSchemaContextName(ctx, "ctx1"), opencdc.Metadata{}), "ctx1:"+base)
	is.Equal(subjectFor(ctx, opencdc.Metadata{}), base)

	// the key and the payload of a record are registered under different subjects
	rec := opencdc.Record{
		Metadata: opencdc.Metadata{},
		Key:      payload.Clone(),
		Payload:  opencdc.Change{After: payload.Clone()},
	}
	is.NoErr(sw.extractAttachKeySchema(ctx, &rec))
	is.NoErr(sw.extractAttachPayloadSchema(ctx, &rec))
	keySubject, _ := rec.Metadata.GetKeySchemaSubject()
	payloadSubject, _ := rec.Metadata.GetPayloadSchemaSubject()
	is.Equal(keySubject, keyBase)
	is.Equal(payloadSubject, base)
}

func TestSourceWithSchemaExtraction_UncacheableShapesTakeTheSlowPath(t *testing.T) {
	ctx := context.Background()

	tests := map[string]opencdc.StructuredData{
		"pointer":         {"p": lang.Ptr("x")},
		"typed slice":     {"s": []string{"a"}},
		"struct":          {"s": shapeGenStruct{A: "a", B: 1}},
		"typed map":       {"m": map[string]int{"a": 1}},
		"too deep":        nestedRecord(schemaShapeMaxDepth + 2),
		"too large a key": wideRecord(2000),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			sw := newTestExtraction(t)
			rec := opencdc.Record{Metadata: opencdc.Metadata{}, Payload: opencdc.Change{After: payload}}
			err := sw.extractAttachPayloadSchema(ctx, &rec)
			is.Equal(sw.shapes.len(), 0) // nothing was cached
			if err != nil {
				is.True(inferAchievable(payload, "", true)) // same outcome as uncached inference
				return
			}
			subject, _ := rec.Metadata.GetPayloadSchemaSubject()
			version, _ := rec.Metadata.GetPayloadSchemaVersion()
			sch, err := schema.Get(ctx, subject, version)
			is.NoErr(err)
			is.True(inferAchievable(payload, string(sch.Bytes), false))
		})
	}
}

func nestedRecord(depth int) opencdc.StructuredData {
	if depth == 0 {
		return opencdc.StructuredData{"leaf": "x"}
	}
	return opencdc.StructuredData{"n": nestedRecord(depth - 1)}
}

func wideRecord(n int) opencdc.StructuredData {
	rec := make(opencdc.StructuredData, n)
	for i := range n {
		rec["field_"+strconv.Itoa(i)] = "x"
	}
	return rec
}

// -- failures -------------------------------------------------------------------

// failingSchemaService fails CreateSchema while failing is set.
type failingSchemaService struct {
	pconnutils.SchemaService
	failing atomic.Bool
	creates atomic.Int64
}

func (f *failingSchemaService) CreateSchema(ctx context.Context, req pconnutils.CreateSchemaRequest) (pconnutils.CreateSchemaResponse, error) {
	f.creates.Add(1)
	if f.failing.Load() {
		return pconnutils.CreateSchemaResponse{}, errors.New("registry rejected the schema")
	}
	return f.SchemaService.CreateSchema(ctx, req)
}

func TestSourceWithSchemaExtraction_RegistryErrorsAreNotCached(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	orig := schema.Service
	t.Cleanup(func() { schema.Service = orig })
	svc := &failingSchemaService{SchemaService: orig}
	schema.Service = svc

	sw := newTestExtraction(t)
	payload := opencdc.StructuredData{"id": int64(1), "reg_err": "x"}
	newRec := func() opencdc.Record {
		return opencdc.Record{Metadata: opencdc.Metadata{}, Payload: opencdc.Change{After: payload.Clone()}}
	}

	svc.failing.Store(true)
	for range 3 {
		rec := newRec()
		is.True(sw.extractAttachPayloadSchema(ctx, &rec) != nil) // rejected
		_, err := rec.Metadata.GetPayloadSchemaSubject()
		is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound)) // nothing attached
	}
	is.Equal(sw.shapes.len(), 0)

	// the registry recovers, the very next record gets a schema
	svc.failing.Store(false)
	rec := newRec()
	is.NoErr(sw.extractAttachPayloadSchema(ctx, &rec))
	is.Equal(sw.shapes.len(), 1)

	// ... and from now on the registry isn't consulted for this shape
	creates := svc.creates.Load()
	for range 10 {
		rec := newRec()
		is.NoErr(sw.extractAttachPayloadSchema(ctx, &rec))
	}
	is.Equal(svc.creates.Load(), creates)
}

// -- cache mechanics --------------------------------------------------------------

func TestSchemaShapeCache_Expiry(t *testing.T) {
	is := is.New(t)
	var c schemaShapeCache
	now := time.Now()
	sch := schema.Schema{Subject: "s", Version: 1}

	c.put([]byte("k"), sch, now)
	got, ok := c.get([]byte("k"), now.Add(schemaShapeCacheTTL-time.Second))
	is.True(ok)
	is.Equal(got, sch)

	_, ok = c.get([]byte("k"), now.Add(schemaShapeCacheTTL+time.Second))
	is.True(!ok) // expired entries are never served
	is.Equal(c.len(), 0)
}

func TestSchemaShapeCache_Bounded(t *testing.T) {
	is := is.New(t)
	var c schemaShapeCache
	now := time.Now()

	hotKey := []byte("always-used")
	c.put(hotKey, schema.Schema{Subject: "hot"}, now)

	var rotations int
	for i := range 20 * schemaShapeCacheEntries {
		key := binary.AppendUvarint(nil, uint64(i))
		if c.put(key, schema.Schema{Subject: "s" + strconv.Itoa(i)}, now) {
			rotations++
		}
		is.True(c.len() <= 2*schemaShapeCacheEntries) // memory bound
		// a shape that keeps being used survives rotation (LRU-like)
		if i%10 == 0 {
			_, ok := c.get(hotKey, now)
			if i > 0 && !ok {
				t.Fatalf("hot entry evicted after %d inserts", i)
			}
			if !ok {
				c.put(hotKey, schema.Schema{Subject: "hot"}, now)
			}
		}
	}
	is.True(rotations > 10)
	// the oldest entries are gone
	_, ok := c.get(binary.AppendUvarint(nil, 0), now)
	is.True(!ok)
}

func TestSchemaShapeKey_NoAllocations(t *testing.T) {
	payload := realisticPayload()
	// Use a builder directly: sync.Pool deliberately drops items under -race,
	// which would make this about the pool and not about the key builder.
	kb := &shapeKeyBuilder{}
	build := func() {
		kb.buf, kb.keys = kb.buf[:0], kb.keys[:0]
		if !kb.appendShape(payload, 0) {
			t.Fatal("realistic payload must be cacheable")
		}
	}
	build() // grow the scratch space
	allocs := testing.AllocsPerRun(100, build)
	if allocs != 0 {
		t.Fatalf("building a shape key allocates %v times per call, want 0", allocs)
	}
}

func TestSchemaShapeKey_IgnoresMapIterationOrder(t *testing.T) {
	is := is.New(t)
	payload := realisticPayload()
	first, ok := shapeKeyOf(payload)
	is.True(ok)
	for range 200 {
		again, ok := shapeKeyOf(payload.Clone())
		is.True(ok)
		is.Equal(again, first)
	}
}

// -- limits -----------------------------------------------------------------------

func TestSchemaShapeCache_ByteBudget(t *testing.T) {
	is := is.New(t)
	var c schemaShapeCache
	now := time.Now()

	// Entries with large schema texts: the count bound (2048) alone would allow
	// 2048 * 512 KiB = 1 GiB.
	text := make([]byte, 512<<10)
	for i := range 1000 {
		c.put(binary.AppendUvarint(nil, uint64(i)), schema.Schema{Bytes: text}, now)
		is.True(c.bytes() <= schemaShapeCacheBytes+schemaShapeCacheBytes/8)
	}
	is.True(c.len() > 0)

	// an entry that is too large is not kept at all
	var c2 schemaShapeCache
	c2.put([]byte("big"), schema.Schema{Bytes: make([]byte, schemaShapeCacheBytes/16)}, now)
	is.Equal(c2.len(), 0)
	is.Equal(c2.bytes(), 0)

	// accounting returns to zero when entries leave
	var c3 schemaShapeCache
	c3.put([]byte("a"), schema.Schema{Bytes: []byte("xyz")}, now)
	is.Equal(c3.bytes(), 4)
	_, ok := c3.get([]byte("a"), now.Add(2*schemaShapeCacheTTL)) // expired
	is.True(!ok)
	is.Equal(c3.bytes(), 0)
}

// Hostile or degenerate records must bypass the cache without the key builder
// doing work proportional to their size.
func TestSchemaShapeKey_RefusesEarly(t *testing.T) {
	t.Run("100k fields", func(t *testing.T) {
		rec := wideRecord(100_000)
		kb := &shapeKeyBuilder{}
		start := time.Now()
		allocs := testing.AllocsPerRun(5, func() {
			kb.buf, kb.keys = kb.buf[:0], kb.keys[:0]
			if kb.appendShape(rec, 0) {
				t.Fatal("a 100k field record must not be keyed")
			}
		})
		if allocs > 1 {
			t.Fatalf("refusing allocates %v times", allocs)
		}
		if cap(kb.keys) != 0 {
			t.Fatalf("collected %d map keys before refusing", cap(kb.keys))
		}
		if d := time.Since(start) / 6; d > 20*time.Millisecond {
			t.Fatalf("refusing took %v", d) // microseconds in practice, sorting 100k keys is ~10+ ms
		}
	})
	t.Run("nested 100k field map", func(t *testing.T) {
		m := make(map[string]any, 100_000)
		for i := range 100_000 {
			m["f"+strconv.Itoa(i)] = int64(i)
		}
		kb := &shapeKeyBuilder{}
		if kb.appendShape(opencdc.StructuredData{"m": m}, 0) {
			t.Fatal("must not be keyed")
		}
		if cap(kb.keys) > schemaShapeMaxKeyLen {
			t.Fatalf("collected %d map keys before refusing", cap(kb.keys))
		}
	})
	t.Run("10 MiB field name", func(t *testing.T) {
		name := string(make([]byte, 10<<20))
		rec := opencdc.StructuredData{name: "x"}
		kb := &shapeKeyBuilder{}
		if kb.appendShape(rec, 0) {
			t.Fatal("must not be keyed")
		}
		if cap(kb.buf) > 2*schemaShapeMaxKeyLen {
			t.Fatalf("key buffer grew to %d bytes", cap(kb.buf))
		}
	})
	t.Run("10 MiB collection or subject", func(t *testing.T) {
		is := is.New(t)
		sw := newTestExtraction(t)
		rec := opencdc.Record{
			Metadata: opencdc.Metadata{opencdc.MetadataCollection: string(make([]byte, 10<<20))},
			Payload:  opencdc.Change{After: opencdc.StructuredData{"id": int64(1)}},
		}
		is.NoErr(sw.extractAttachPayloadSchema(context.Background(), &rec)) // still works, uncached
		is.Equal(sw.shapes.len(), 0)
	})
	t.Run("huge array", func(t *testing.T) {
		kb := &shapeKeyBuilder{}
		if kb.appendShape(opencdc.StructuredData{"l": make([]any, 1_000_000)}, 0) {
			t.Fatal("must not be keyed")
		}
	})
}

func TestSchemaShapeKeyBuilder_PoolHygiene(t *testing.T) {
	kb := getShapeKeyBuilder()
	if !kb.appendShape(opencdc.StructuredData{"secret_column_a": "x", "secret_column_b": int64(1)}, 0) {
		t.Fatal("must be keyed")
	}
	putShapeKeyBuilder(kb)
	// kb itself may or may not have been pooled, either way no field name may
	// be left behind in its scratch space.
	for i, k := range kb.keys[:cap(kb.keys)] {
		if k != "" {
			t.Fatalf("scratch slot %d still holds field name %q", i, k)
		}
	}

	// builders with a large scratch space are dropped, not pooled
	big := &shapeKeyBuilder{keys: make([]string, 0, 10*keyBuilderMaxKeys)}
	putShapeKeyBuilder(big) // must not panic, and must not be kept (not observable, but exercised)
}

func TestSchemaShapeCache_ChurnBypass(t *testing.T) {
	now := time.Now()
	key := func(i int) []byte { return binary.AppendUvarint(nil, uint64(i)) }
	sch := schema.Schema{Bytes: []byte("x")}

	t.Run("a healthy cache is not bypassed", func(t *testing.T) {
		is := is.New(t)
		var c schemaShapeCache
		for i := range 3 * schemaShapeChurnWindow {
			if i%2 == 0 {
				c.put(key(i), sch, now)
			}
			c.get(key(i-i%2), now) // 50% hits
			is.True(!c.skip(now))
		}
		is.True(!c.consumeTrip())
	})

	t.Run("100 percent churn trips the detector, then recovers", func(t *testing.T) {
		is := is.New(t)
		var c schemaShapeCache
		tripped := -1
		for i := range 3 * schemaShapeChurnWindow {
			if c.skip(now) {
				tripped = i
				break
			}
			if _, ok := c.get(key(i), now); !ok {
				c.put(key(i), sch, now)
			}
		}
		is.Equal(tripped, schemaShapeChurnWindow) // right after the first window
		is.True(c.consumeTrip())
		is.True(!c.consumeTrip()) // reported once
		is.Equal(c.len(), 0)      // and the cache was emptied
		c.put(key(1), sch, now)   // stores are ignored while bypassed
		is.Equal(c.len(), 0)

		is.True(c.skip(now.Add(schemaShapeChurnCooldown - time.Second)))
		later := now.Add(schemaShapeChurnCooldown + time.Second)
		is.True(!c.skip(later))
		c.put(key(1), sch, later)
		_, ok := c.get(key(1), later)
		is.True(ok) // back in business
	})
}

// A source whose every record has a shape of its own: correct schemas, bounded
// memory, and the cache steps aside.
func TestSourceWithSchemaExtraction_ChurnBypass(t *testing.T) {
	ctx := context.Background()
	sw := newTestExtraction(t)

	for i := range 2*schemaShapeChurnWindow + 100 {
		payload := opencdc.StructuredData{"id": int64(i), "u" + strconv.Itoa(i): "x"}
		rec := opencdc.Record{Metadata: opencdc.Metadata{}, Payload: opencdc.Change{After: payload}}
		if err := sw.extractAttachPayloadSchema(ctx, &rec); err != nil {
			t.Fatal(err)
		}
		if i%500 == 0 { // spot check, with the real schema service
			subject, _ := rec.Metadata.GetPayloadSchemaSubject()
			version, _ := rec.Metadata.GetPayloadSchemaVersion()
			got, err := schema.Get(ctx, subject, version)
			if err != nil {
				t.Fatal(err)
			}
			want, err := inferUncached(payload)
			if err != nil || string(got.Bytes) != want {
				t.Fatalf("record %d got the wrong schema (%v)", i, err)
			}
		}
	}
	if !sw.shapes.skip(time.Now()) {
		t.Fatal("cache was not bypassed under 100% churn")
	}
	if n := sw.shapes.len(); n != 0 {
		t.Fatalf("%d entries held while bypassed", n)
	}
}

// -- concurrency ------------------------------------------------------------------

// TestSourceWithSchemaExtraction_Concurrent hammers one middleware from many
// goroutines with a mix of shapes, more than the cache holds, so that hits,
// misses, promotions and rotations race. Run with -race.
func TestSourceWithSchemaExtraction_Concurrent(t *testing.T) {
	ctx := context.Background()
	sw := newTestExtraction(t)

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for w := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := &shapeGen{r: rand.New(rand.NewPCG(uint64(w), 3)), unsupported: true}
			for i := range 1500 {
				payload := g.record(0)
				if i%7 == 0 {
					// a distinct shape every time to force rotations
					payload["uniq-"+strconv.Itoa(w)+"-"+strconv.Itoa(i)] = "x"
				}
				rec := opencdc.Record{
					Metadata: opencdc.Metadata{opencdc.MetadataCollection: "concurrent"},
					Payload:  opencdc.Change{After: payload},
				}
				err := sw.extractAttachPayloadSchema(ctx, &rec)
				if err != nil {
					if !inferAchievable(payload, "", true) {
						errs <- fmt.Errorf("cached path failed (%w), uncached inference never does", err)
						return
					}
					continue
				}
				subject, _ := rec.Metadata.GetPayloadSchemaSubject()
				version, _ := rec.Metadata.GetPayloadSchemaVersion()
				got, err := schema.Get(ctx, subject, version)
				if err != nil {
					errs <- fmt.Errorf("get attached schema: %w", err)
					return
				}
				if !inferAchievable(payload, string(got.Bytes), false) {
					errs <- fmt.Errorf("wrong schema attached for %#v", payload)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if n := sw.shapes.len(); n > 2*schemaShapeCacheEntries {
		t.Fatalf("cache holds %d entries, bound is %d", n, 2*schemaShapeCacheEntries)
	}
}

// -- fuzzing ----------------------------------------------------------------------

// FuzzSchemaShapeKey drives the generator from fuzzer input and checks the same
// property as TestSchemaShapeKey_EqualKeyImpliesEqualSchema on a pair of values.
func FuzzSchemaShapeKey(f *testing.F) {
	for i := range uint64(20) {
		f.Add(i, i*7+1)
	}
	f.Fuzz(func(t *testing.T, seed1, seed2 uint64) {
		g := &shapeGen{r: rand.New(rand.NewPCG(seed1, seed2)), unsupported: true}
		// two values from the same stream: related enough to collide often
		a, b := g.record(0), g.record(0)
		ka, oka := shapeKeyOf(a)
		kb, okb := shapeKeyOf(b)
		if !oka || !okb || ka != kb {
			return
		}
		sb, errb := inferUncached(b)
		if !inferAchievable(a, sb, errb != nil) {
			t.Fatalf("same shape key, different inference\na: %#v\nb: %#v\n%q %v", a, b, sb, errb)
		}
	})
}
