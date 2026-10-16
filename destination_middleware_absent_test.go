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

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema/avro"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// TestDestinationWithSchemaExtraction_Write_EmptyRawDataIsAbsent pins that a
// payload.before or payload.after holding a non-nil, zero-length RawData is
// treated as absent. Conduit does not send that shape today (its
// sanitizeRecord is a no-op), but nothing in the protocol rules it out, and
// decoding those zero bytes with the payload schema produced an invented
// all-null record such as {"id": nil, "name": nil} (invariant 6). Keys are
// covered separately: an empty key is a real value and is still decoded.
func TestDestinationWithSchemaExtraction_Write_EmptyRawDataIsAbsent(t *testing.T) {
	ctx := context.Background()

	value := opencdc.StructuredData{"id": int64(1), "name": "alice"}
	srd, err := avro.SerdeForType(value)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := schema.Create(ctx, schema.TypeAvro, t.Name(), []byte(srd.String()))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := sch.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	updatedValue := opencdc.StructuredData{"id": int64(1), "name": "bob"}
	encodedUpdated, err := sch.Marshal(updatedValue)
	if err != nil {
		t.Fatal(err)
	}

	schemaMetadata := func() opencdc.Metadata {
		return opencdc.Metadata{
			opencdc.MetadataKeySchemaSubject:     sch.Subject,
			opencdc.MetadataKeySchemaVersion:     strconv.Itoa(sch.Version),
			opencdc.MetadataPayloadSchemaSubject: sch.Subject,
			opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
		}
	}

	testCases := []struct {
		name string
		in   opencdc.Record
		want opencdc.Record
	}{{
		name: "create with empty raw before: before not decoded",
		in: opencdc.Record{
			Operation: opencdc.OperationCreate,
			Metadata:  schemaMetadata(),
			Key:       opencdc.RawData(encoded),
			Payload: opencdc.Change{
				Before: opencdc.RawData{},
				After:  opencdc.RawData(encoded),
			},
		},
		want: opencdc.Record{
			Operation: opencdc.OperationCreate,
			Metadata:  schemaMetadata(),
			Key:       value,
			Payload: opencdc.Change{
				Before: opencdc.RawData{},
				After:  value,
			},
		},
	}, {
		name: "nil-slice raw data is absent too",
		in: opencdc.Record{
			Operation: opencdc.OperationSnapshot,
			Metadata:  schemaMetadata(),
			Key:       opencdc.RawData(encoded),
			Payload: opencdc.Change{
				Before: opencdc.RawData(nil),
				After:  opencdc.RawData(encoded),
			},
		},
		want: opencdc.Record{
			Operation: opencdc.OperationSnapshot,
			Metadata:  schemaMetadata(),
			Key:       value,
			Payload: opencdc.Change{
				Before: opencdc.RawData(nil),
				After:  value,
			},
		},
	}, {
		name: "delete with empty raw after: after not decoded",
		in: opencdc.Record{
			Operation: opencdc.OperationDelete,
			Metadata:  schemaMetadata(),
			Key:       opencdc.RawData(encoded),
			Payload: opencdc.Change{
				Before: opencdc.RawData(encoded),
				After:  opencdc.RawData{},
			},
		},
		want: opencdc.Record{
			Operation: opencdc.OperationDelete,
			Metadata:  schemaMetadata(),
			Key:       value,
			Payload: opencdc.Change{
				Before: value,
				After:  opencdc.RawData{},
			},
		},
	}, {
		name: "update with a real before still decodes",
		in: opencdc.Record{
			Operation: opencdc.OperationUpdate,
			Metadata:  schemaMetadata(),
			Key:       opencdc.RawData(encoded),
			Payload: opencdc.Change{
				Before: opencdc.RawData(encoded),
				After:  opencdc.RawData(encodedUpdated),
			},
		},
		want: opencdc.Record{
			Operation: opencdc.OperationUpdate,
			Metadata:  schemaMetadata(),
			Key:       value,
			Payload: opencdc.Change{
				Before: value,
				After:  updatedValue,
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctrl := gomock.NewController(t)
			dst := NewMockDestination(ctrl)
			d := (&DestinationWithSchemaExtraction{
				PayloadEnabled: lang.Ptr(true),
				KeyEnabled:     lang.Ptr(true),
			}).Wrap(dst)

			var got opencdc.Record
			dst.EXPECT().Write(ctx, gomock.Any()).DoAndReturn(
				func(_ context.Context, recs []opencdc.Record) (int, error) {
					got = recs[0]
					return len(recs), nil
				})

			n, err := d.Write(ctx, []opencdc.Record{tc.in})
			is.NoErr(err)
			is.Equal(n, 1)
			// cmp tells RawData{} from RawData(nil), so the test also
			// pins that the middleware leaves an absent part as received.
			if diff := cmp.Diff(tc.want.Key, got.Key); diff != "" {
				t.Errorf("key mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.want.Payload, got.Payload); diff != "" {
				t.Errorf("payload mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestDestinationWithSchemaExtraction_Write_EmptyKeyStillDecodes pins that a
// zero-length key is decoded, not skipped. Zero bytes is the valid encoding
// of a zero-field record and of a "null" schema; postgres emits
// StructuredData{} keys with a zero-field key schema for relations without a
// key. Leaving such a key as RawData{} would change the bytes a destination
// keys on, e.g. a Kafka message key going from "{}" to empty, which moves
// the record to another partition (invariant 4).
func TestDestinationWithSchemaExtraction_Write_EmptyKeyStillDecodes(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name    string
		schema  string
		wantKey opencdc.Data
	}{{
		name:    "zero-field record key schema",
		schema:  `{"type":"record","name":"key","fields":[]}`,
		wantKey: opencdc.StructuredData{},
	}, {
		name:    "null key schema",
		schema:  `"null"`,
		wantKey: opencdc.StructuredData(nil),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			sch, err := schema.Create(ctx, schema.TypeAvro, t.Name(), []byte(tc.schema))
			is.NoErr(err)
			// the premise of the test: this key encodes to zero bytes
			b, err := sch.Marshal(opencdc.StructuredData{})
			is.NoErr(err)
			is.Equal(len(b), 0)

			ctrl := gomock.NewController(t)
			dst := NewMockDestination(ctrl)
			d := (&DestinationWithSchemaExtraction{
				PayloadEnabled: lang.Ptr(true),
				KeyEnabled:     lang.Ptr(true),
			}).Wrap(dst)

			var got opencdc.Record
			dst.EXPECT().Write(ctx, gomock.Any()).DoAndReturn(
				func(_ context.Context, recs []opencdc.Record) (int, error) {
					got = recs[0]
					return len(recs), nil
				})

			_, err = d.Write(ctx, []opencdc.Record{{
				Operation: opencdc.OperationCreate,
				Metadata: opencdc.Metadata{
					opencdc.MetadataKeySchemaSubject: sch.Subject,
					opencdc.MetadataKeySchemaVersion: strconv.Itoa(sch.Version),
				},
				Key: opencdc.RawData(b),
			}})
			is.NoErr(err)
			if diff := cmp.Diff(tc.wantKey, got.Key); diff != "" {
				t.Errorf("key mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
