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
	"encoding/binary"
	"slices"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
)

// This file implements the cache used by SourceWithSchemaExtraction to avoid
// inferring and registering a schema for every record.
//
// Inferring a schema from a structured value is by far the most expensive step
// of the default source middleware chain (it builds a full Avro serde that is
// thrown away right after its textual form is read). Records produced by a
// source overwhelmingly share a handful of shapes, so the result is cached by
// shape.
//
// Invariant 6 (schema handling never silently mangles data) is the reason this
// cache is built the way it is. The one failure that matters is attaching the
// wrong schema to a record, which happens if two values that infer different
// schemas share a cache key. Therefore:
//
//   - The key is an exact, canonical encoding of everything the schema
//     inference reads from the value, not a hash. Different shapes can never
//     collide, and there is nothing to verify after a hit.
//   - The key is only built for a closed set of Go types (see appendShape). Any
//     value outside the set, any value nested deeper or larger than the limits
//     below, makes the key builder give up and the caller takes the uncached
//     path, which is exactly the pre-existing behavior (including its errors).
//   - Entries expire 15 minutes after they are stored, the same time the
//     schema service client caches CreateSchema responses. The two stack: the
//     client's entry may already be up to 15 minutes old when a shape entry is
//     created from it, so a shape entry can serve a registration that is up to
//     about 30 minutes old (the client does not expose the remaining life of
//     its entry, so it can't be capped any tighter). A middleware that outlives
//     a replacement of schema.Service keeps serving registrations made against
//     the old service until they expire. Errors are never cached.
//   - Memory is bounded by a byte budget, not only by an entry count, and
//     hostile or degenerate shapes are refused before any work is done on them
//     (see the limits below).

const (
	// schemaShapeCacheEntries is the number of entries per generation. The cache
	// keeps at most two generations. Every collection of a multi table source
	// has a key and a payload shape, so this is sized for a few hundred tables.
	schemaShapeCacheEntries = 1024
	// schemaShapeCacheBytes is the byte budget of one cache, counted as
	// len(key)+len(schema text) per entry. A count alone is not a bound: a key
	// near the length limit below can come with a schema text of about 50 KiB,
	// which is about 100 MiB at 2047 entries. When a generation reaches half the
	// budget it rotates, so the total stays under this plus one entry per
	// generation (an entry larger than schemaShapeCacheBytes/16 is not kept).
	schemaShapeCacheBytes = 16 << 20
	// schemaShapeCacheTTL matches the expiry of the CreateSchema cache in the
	// schema package, so that registration is repeated on the same cadence. See
	// the note on staleness above.
	schemaShapeCacheTTL = 15 * time.Minute
	// schemaShapeMaxKeyLen bounds the memory of a single entry and the time
	// spent building a key for huge or highly dynamic values. Larger shapes are
	// not cached, and key building gives up as soon as it knows it will exceed
	// this, before collecting, sorting or copying anything it can't keep.
	schemaShapeMaxKeyLen = 4096
	// schemaShapeMaxDepth bounds recursion for nested values.
	schemaShapeMaxDepth = 16

	// A cache that sees (nearly) only shapes it has not seen before is pure
	// overhead: every record pays for building a key and for an insert on top
	// of the full cost it had without a cache. After schemaShapeChurnWindow
	// lookups with a hit rate under 1%, the cache is bypassed (no key is built,
	// nothing is looked up or stored) for schemaShapeChurnCooldown and emptied.
	schemaShapeChurnWindow   = 4096
	schemaShapeChurnCooldown = time.Minute
	// keyBuilderMaxKeys is the largest scratch slice of map keys kept in the
	// builder pool.
	keyBuilderMaxKeys = 1024
)

// schemaShapeCache maps the shape of a record's key or payload to the schema
// that was registered for it. It is safe for concurrent use.
//
// It approximates an LRU with two generations: lookups hit the hot generation
// first and promote entries found in the cold one; when the hot generation is
// full (by count or by bytes) it becomes the cold one and the previous cold
// generation is dropped. A shape that is still in use survives rotation.
type schemaShapeCache struct {
	mu   sync.Mutex
	hot  map[string]schemaShapeEntry
	cold map[string]schemaShapeEntry

	hotBytes, coldBytes int

	// churn detector, see schemaShapeChurnWindow
	lookups, hits int
	bypassUntil   time.Time
	tripped       bool
}

type schemaShapeEntry struct {
	sch     schema.Schema
	expires time.Time
	size    int
}

// skip reports whether the cache is currently bypassed because shapes aren't
// repeating. Callers must not build a key or call get and put while it is true.
func (c *schemaShapeCache) skip(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return now.Before(c.bypassUntil)
}

// consumeTrip reports (once) that the churn detector has just engaged.
func (c *schemaShapeCache) consumeTrip() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tripped
	c.tripped = false
	return t
}

// get returns the schema cached under key. The conversion string(key) in the
// map index does not allocate.
func (c *schemaShapeCache) get(key []byte, now time.Time) (schema.Schema, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sch, ok := c.getLocked(key, now)
	c.lookups++
	if ok {
		c.hits++
	}
	if c.lookups >= schemaShapeChurnWindow {
		if c.hits*100 < c.lookups {
			c.bypassUntil = now.Add(schemaShapeChurnCooldown)
			c.hot, c.cold, c.hotBytes, c.coldBytes = nil, nil, 0, 0
			c.tripped = true
		}
		c.lookups, c.hits = 0, 0
	}
	return sch, ok
}

func (c *schemaShapeCache) getLocked(key []byte, now time.Time) (schema.Schema, bool) {
	if e, ok := c.hot[string(key)]; ok {
		if now.Before(e.expires) {
			return e.sch, true
		}
		delete(c.hot, string(key))
		c.hotBytes -= e.size
		return schema.Schema{}, false
	}
	if e, ok := c.cold[string(key)]; ok {
		delete(c.cold, string(key))
		c.coldBytes -= e.size
		if now.Before(e.expires) {
			c.putLocked(string(key), e) // promote
			return e.sch, true
		}
	}
	return schema.Schema{}, false
}

// put stores sch under key. It reports whether the insert rotated the
// generations, which only happens once the cache has seen more distinct shapes
// (or bytes) than it can hold.
func (c *schemaShapeCache) put(key []byte, sch schema.Schema, now time.Time) (rotated bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Before(c.bypassUntil) {
		return false
	}
	e := schemaShapeEntry{sch: sch, expires: now.Add(schemaShapeCacheTTL), size: len(key) + len(sch.Bytes)}
	return c.putLocked(string(key), e)
}

func (c *schemaShapeCache) putLocked(key string, e schemaShapeEntry) (rotated bool) {
	if e.size > schemaShapeCacheBytes/16 {
		return false // too big to be worth keeping
	}
	if c.hot == nil {
		c.hot = make(map[string]schemaShapeEntry)
	}
	if old, ok := c.hot[key]; ok {
		c.hotBytes -= old.size
	}
	c.hot[key] = e
	c.hotBytes += e.size
	if len(c.hot) >= schemaShapeCacheEntries || c.hotBytes >= schemaShapeCacheBytes/2 {
		c.cold, c.coldBytes = c.hot, c.hotBytes
		c.hot, c.hotBytes = make(map[string]schemaShapeEntry), 0
		return true
	}
	return false
}

// len returns the number of cached entries (test helper).
func (c *schemaShapeCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.hot) + len(c.cold)
}

// bytes returns the accounted size of the cached entries (test helper).
func (c *schemaShapeCache) bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hotBytes + c.coldBytes
}

// -- shape key ----------------------------------------------------------------

// shapeKeyBuilder builds canonical shape keys. Builders are pooled, so that
// building a key does not allocate in the steady state.
type shapeKeyBuilder struct {
	buf  []byte
	keys []string // scratch space for sorting map keys
}

var shapeKeyBuilderPool = sync.Pool{
	New: func() any { return &shapeKeyBuilder{buf: make([]byte, 0, 512)} },
}

func getShapeKeyBuilder() *shapeKeyBuilder {
	b, _ := shapeKeyBuilderPool.Get().(*shapeKeyBuilder)
	b.buf = b.buf[:0]
	b.keys = b.keys[:0]
	return b
}

func putShapeKeyBuilder(b *shapeKeyBuilder) {
	// don't keep pathological buffers alive in the pool
	if cap(b.buf) > 4*schemaShapeMaxKeyLen || cap(b.keys) > keyBuilderMaxKeys {
		return
	}
	// Drop references to record field names (record data). The length is 0 by
	// now, the names sit in the unused part of the backing array.
	clear(b.keys[:cap(b.keys)])
	shapeKeyBuilderPool.Put(b)
}

// remaining returns how many more bytes the key may take.
func (b *shapeKeyBuilder) remaining() int {
	return schemaShapeMaxKeyLen - len(b.buf)
}

// appendString appends s with a length prefix. It refuses, before copying
// anything, a string that does not fit in what is left of the key budget.
func (b *shapeKeyBuilder) appendString(s string) bool {
	if len(s) > b.remaining()-binary.MaxVarintLen64 {
		return false
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(len(s)))
	b.buf = append(b.buf, s...)
	return true
}

// appendShape appends a canonical encoding of everything schema inference
// reads from v. It returns false if v can't be encoded, in which case the
// caller must not use the cache.
//
// The encoding is a function of the value's dynamic types and, for records and
// maps, of its field names. It deliberately encodes more than the inferred
// schema depends on (e.g. map keys that only contribute to a union), which can
// only split one schema over several entries, never merge two schemas into one.
//
// Only the types below are supported. Everything else, including pointers,
// structs, typed slices and maps, named types and unsigned 64 bit integers
// (which have no Avro type and make inference fail), is left to the uncached
// path, so that it keeps its current behavior and error messages.
//
//nolint:gocyclo,cyclop,funlen // a flat type switch is the clearest way to write this
func (b *shapeKeyBuilder) appendShape(v any, depth int) bool {
	if depth > schemaShapeMaxDepth || len(b.buf) > schemaShapeMaxKeyLen {
		return false
	}
	switch v := v.(type) {
	case nil:
		b.buf = append(b.buf, 'n')
	case bool:
		b.buf = append(b.buf, 'b')
	case int:
		b.buf = append(b.buf, 'i')
	case int8:
		b.buf = append(b.buf, '1')
	case int16:
		b.buf = append(b.buf, '2')
	case int32:
		b.buf = append(b.buf, '3')
	case int64:
		b.buf = append(b.buf, '4')
	case uint8:
		b.buf = append(b.buf, '5')
	case uint16:
		b.buf = append(b.buf, '6')
	case uint32:
		b.buf = append(b.buf, '7')
	case float32:
		b.buf = append(b.buf, 'f')
	case float64:
		b.buf = append(b.buf, 'd')
	case string:
		b.buf = append(b.buf, 's')
	case []byte:
		b.buf = append(b.buf, 'y')
	case time.Time:
		b.buf = append(b.buf, 't')
	case time.Duration:
		b.buf = append(b.buf, 'D')
	case opencdc.StructuredData:
		// StructuredData is inferred as a record, a map[string]any as an Avro map
		// with a union of value types, so they get different tags.
		return b.appendMap('R', v, depth)
	case map[string]any:
		return b.appendMap('M', v, depth)
	case []any:
		if len(v) > b.remaining() { // every element takes at least one byte
			return false
		}
		b.buf = append(b.buf, 'A')
		b.buf = binary.AppendUvarint(b.buf, uint64(len(v)))
		for _, e := range v {
			if !b.appendShape(e, depth+1) {
				return false
			}
		}
	default:
		return false
	}
	return true
}

func (b *shapeKeyBuilder) appendMap(tag byte, m map[string]any, depth int) bool {
	// Every entry takes at least two bytes of key (a length prefix and a value
	// tag). A map that can't fit is refused before its keys are collected and
	// sorted, which is where a record with a huge number of fields would cost.
	if len(m)*2 > b.remaining() {
		return false
	}
	b.buf = append(b.buf, tag)
	b.buf = binary.AppendUvarint(b.buf, uint64(len(m)))

	// Map iteration order is random, the key must not depend on it. The sorted
	// names live on b.keys above the names of the enclosing maps.
	start := len(b.keys)
	for k := range m {
		b.keys = append(b.keys, k)
	}
	names := b.keys[start:]
	slices.Sort(names)
	ok := true
	for _, k := range names {
		if !b.appendString(k) || !b.appendShape(m[k], depth+1) {
			ok = false
			break
		}
	}
	b.keys = b.keys[:start]
	return ok
}
