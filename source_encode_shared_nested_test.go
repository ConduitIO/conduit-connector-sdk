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

	"github.com/conduitio/conduit-commons/opencdc"
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-sdk/schema"
)

type auditOneRecordSource struct {
	UnimplementedSource
	rec opencdc.Record
}

func (s *auditOneRecordSource) Read(context.Context) (opencdc.Record, error) { return s.rec, nil }

// Regression test for conduit-connector-sdk#420 (conduit-commons#342).
//
// The source encode middleware hands the record's StructuredData to
// Serde.Marshal. In conduit-commons <= v0.11.0 that hoisted union values in
// place, so a nested slice shared by Before and After was encoded from the
// hoisted values the second time.
func TestAudit_SourceEncode_SharedNestedSlice(t *testing.T) {
	ctx := context.Background()
	sch, err := schema.Create(ctx, cschema.TypeAvro, "audit-shared", []byte(
		`{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":["null","string",{"type":"map","values":"string"}]}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	shared := []any{"bar"}
	rec := opencdc.Record{
		Metadata: opencdc.Metadata{},
		Payload:  opencdc.Change{Before: opencdc.StructuredData{"a": shared}, After: opencdc.StructuredData{"a": shared}},
	}
	cschema.AttachPayloadSchemaToRecord(rec, sch)
	src := (SourceWithEncoding{}).Wrap(&auditOneRecordSource{rec: rec})
	got, err := src.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var after opencdc.StructuredData
	if err := sch.Unmarshal(got.Payload.After.Bytes(), &after); err != nil {
		t.Fatal(err)
	}
	t.Logf("after=%v; connector's own slice now %#v", after, shared)
	if after["a"].([]any)[0] != "bar" {
		t.Errorf("BUG: after.a[0] written as %#v instead of \"bar\"", after["a"].([]any)[0])
	}
}
