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

package kafkaconnect

const (
	DebeziumOpCreate DebeziumOp = "c"
	DebeziumOpUpdate DebeziumOp = "u"
	DebeziumOpDelete DebeziumOp = "d"
	DebeziumOpRead   DebeziumOp = "r" // snapshot
)

type DebeziumOp string

type DebeziumPayload struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source any            `json:"source"`
	Op     DebeziumOp     `json:"op"`
	// TimestampMillis is what we call ReadAt in Conduit.
	TimestampMillis int64                `json:"ts_ms,omitempty"`
	Transaction     *DebeziumTransaction `json:"transaction"`
}

type DebeziumTransaction struct {
	ID                  string `json:"id"`
	TotalOrder          int64  `json:"total_order"`
	DataCollectionOrder int64  `json:"data_collection_order"`
}

func (p DebeziumPayload) ToEnvelope() Envelope {
	s := Schema{
		Type: TypeStruct,
		Fields: []Schema{
			p.beforeSchema(),
			p.afterSchema(),
			p.sourceSchema(), // we use a custom source schema, we put our metadata here
			{
				Field: "op",
				Type:  TypeString,
			}, {
				Field:    "ts_ms",
				Type:     TypeInt64,
				Optional: true,
			}, {
				Field:    "transaction",
				Type:     TypeStruct,
				Optional: true,
				Fields: []Schema{{
					Field: "id",
					Type:  TypeString,
				}, {
					Field: "total_order",
					Type:  TypeInt64,
				}, {
					Field: "data_collection_order",
					Type:  TypeInt64,
				}},
			},
		},
	}
	return Envelope{
		Schema:  s,
		Payload: p,
	}
}

func (p DebeziumPayload) beforeSchema() Schema {
	var s *Schema
	if p.Before == nil {
		// if before is nil take schema from after (e.g. it's an insert)
		s = Reflect(p.After)
	} else {
		s = Reflect(p.Before)
	}
	if s == nil {
		// both before and after are nil (strange?!), let's say they are empty structs
		s = &Schema{
			Type: TypeStruct,
		}
	}
	s.Field = "before"
	s.Optional = true // before is always optional
	return *s
}
func (p DebeziumPayload) afterSchema() Schema {
	var s *Schema
	if p.After == nil {
		// if after is nil take schema from before (e.g. it's an delete)
		s = Reflect(p.Before)
	} else {
		s = Reflect(p.After)
	}
	if s == nil {
		// both before and after are nil (strange?!), let's say they are empty structs
		s = &Schema{
			Type: TypeStruct,
		}
	}
	s.Field = "after"
	s.Optional = true // after is always optional
	return *s
}
func (p DebeziumPayload) sourceSchema() Schema {
	s := Reflect(p.Source)
	if s == nil {
		// source is nil
		s = &Schema{
			Type:     TypeStruct,
			Optional: true,
		}
	}
	s.Field = "source"
	return *s
}
