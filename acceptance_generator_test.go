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
	"reflect"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// TestConfigurableAcceptanceTestDriver_GeneratedDataPassesSchemaExtraction is
// the regression test for ConduitIO/conduit-connector-sdk#395: source schema
// extraction is on by default, so every structured payload the acceptance
// driver generates must be something extraction can derive an Avro schema
// for. Random field names with spaces and punctuation are not valid Avro names
// and failed here, which forced connectors to turn extraction off for the
// acceptance run and left the default configuration untested.
func TestConfigurableAcceptanceTestDriver_GeneratedDataPassesSchemaExtraction(t *testing.T) {
	d := ConfigurableAcceptanceTestDriver{Config: ConfigurableAcceptanceTestDriverConfig{
		GenerateDataType: GenerateStructuredData,
	}}
	factory := schema.KnownSerdeFactories[schema.TypeAvro]
	for i := 0; i < 200; i++ {
		data, ok := d.GenerateData(t).(opencdc.StructuredData)
		if !ok {
			t.Fatalf("GenerateStructuredData produced %T", data)
		}
		// The same two steps as sourceWithSchemaExtraction.schemaForType:
		// derive a schema from the value, then parse its text (schema.Create
		// parses it, and that is where invalid names are rejected).
		srd, err := factory.SerdeForType(data)
		if err != nil {
			t.Fatalf("schema extraction rejects generated data (fields %v): %v", fieldNames(data), err)
		}
		if _, err := factory.Parse(context.Background(), schema.ParseInput{Bytes: []byte(srd.String())}, nil); err != nil {
			t.Fatalf("the schema extracted from generated data does not parse (fields %v): %v", fieldNames(data), err)
		}
	}
}

func TestConfigurableAcceptanceTestDriver_RandFieldName(t *testing.T) {
	d := ConfigurableAcceptanceTestDriver{}
	for _, n := range []int{-1, 0, 1, 2, 32, 1055} {
		name := d.randFieldName(n)
		want := max(n, 1)
		if len(name) != want {
			t.Errorf("randFieldName(%d) has length %d, want %d", n, len(name), want)
		}
		if !isAvroName(name) {
			t.Errorf("randFieldName(%d) = %q is not a valid Avro name", n, name)
		}
	}
}

func isAvroName(s string) bool {
	for i, c := range s {
		switch {
		case c == '_', c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return s != ""
}

func fieldNames(d opencdc.StructuredData) []string {
	out := make([]string, 0, len(d))
	for k := range d {
		if len(k) > 20 {
			k = k[:20] + "…"
		}
		out = append(out, k)
	}
	return out
}

// TestDecodeSchemaEncoded checks that the harness undoes source schema
// extraction: a record encoded by SourceWithSchemaExtraction decodes back to
// the generated data, compared by value, with position and metadata intact.
func TestDecodeSchemaEncoded(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	d := ConfigurableAcceptanceTestDriver{Config: ConfigurableAcceptanceTestDriverConfig{
		GenerateDataType: GenerateStructuredData,
	}}
	want := d.GenerateRecord(t, opencdc.OperationCreate)

	src := NewMockSource(gomock.NewController(t))
	// The default chain: extraction attaches the schema, encoding encodes
	// with it (SourceWithEncoding wraps the extraction).
	s := SourceWithEncoding{}.Wrap((&SourceWithSchemaExtraction{
		SchemaTypeStr:  schema.TypeAvro.String(),
		PayloadEnabled: lang.Ptr(true),
		// Own subjects: the schema service is process-global, and other tests
		// expect version 1 of "key" and "payload".
		PayloadSubject: lang.Ptr("decode-schema-encoded-test-payload"),
		KeyEnabled:     lang.Ptr(true),
		KeySubject:     lang.Ptr("decode-schema-encoded-test-key"),
	}).Wrap(src))
	src.EXPECT().Read(ctx).Return(want.Clone(), nil)
	encoded, err := s.Read(ctx)
	is.NoErr(err)
	_, isRaw := encoded.Payload.After.(opencdc.RawData)
	is.True(isRaw) // extraction encodes the payload

	got, err := decodeSchemaEncoded(ctx, encoded)
	is.NoErr(err)
	is.Equal(got.Position, want.Position)
	is.Equal(got.Metadata, encoded.Metadata)
	is.Equal(normalizeStructured(got.Key), normalizeStructured(want.Key))
	is.Equal(normalizeStructured(got.Payload.After), normalizeStructured(want.Payload.After))
	_, stillRaw := encoded.Payload.After.(opencdc.RawData)
	is.True(stillRaw) // the input record is not modified

	// a record without an attached schema is returned as is
	plain := want.Clone()
	got, err = decodeSchemaEncoded(ctx, plain)
	is.NoErr(err)
	is.Equal(got, plain)
}

func TestNormalizeStructured(t *testing.T) {
	is := is.New(t)
	a := opencdc.StructuredData{"i": 1, "f": float32(0.5), "n": map[string]any{"x": int32(2)}, "l": []any{int8(3)}, "s": "v"}
	b := opencdc.StructuredData{"i": int64(1), "f": 0.5, "n": map[string]any{"x": int64(2)}, "l": []any{int64(3)}, "s": "v"}
	is.Equal(normalizeStructured(a), normalizeStructured(b))
	c := opencdc.StructuredData{"i": int64(2), "f": 0.5, "n": map[string]any{"x": int64(2)}, "l": []any{int64(3)}, "s": "v"}
	is.True(!equalAny(normalizeStructured(a), normalizeStructured(c)))
}

func equalAny(a, b any) bool { return reflect.DeepEqual(a, b) }

type captureLog struct{ lines []string }

func (c *captureLog) Logf(format string, args ...any) {
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func TestAcceptanceReport_LogStart(t *testing.T) {
	is := is.New(t)
	rep := newAcceptanceReport(ConfigurableAcceptanceTestDriver{}, AcceptanceCapabilities{})
	var c captureLog
	rep.logStart(&c)
	is.Equal(len(c.lines), 1)
	is.True(strings.Contains(c.lines[0], "revision "+AcceptanceSuiteV2Revision))
	is.True(strings.Contains(c.lines[0], suiteSourceHash()))
	is.True(strings.Contains(c.lines[0], rep.CaseSetHash))
}

// A record that claims a schema the schema service does not have must not be
// compared as if it were plain data: decoding fails and the harness reports it.
func TestDecodeSchemaEncoded_UnknownSchema(t *testing.T) {
	is := is.New(t)
	rec := opencdc.Record{
		Metadata: opencdc.Metadata{},
		Payload:  opencdc.Change{After: opencdc.RawData("not avro")},
	}
	rec.Metadata.SetPayloadSchemaSubject("decode-schema-encoded-test-missing")
	rec.Metadata.SetPayloadSchemaVersion(7)
	_, err := decodeSchemaEncoded(context.Background(), rec)
	is.True(err != nil)
}
