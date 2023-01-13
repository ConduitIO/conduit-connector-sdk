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

import (
	"testing"

	"encoding/json"

	"github.com/matryer/is"
)

func TestSchema(t *testing.T) {
	schemaNode := Schema{
		Type:     TypeStruct,
		Name:     "my.struct.name",
		Optional: false,
		Fields: []Schema{{
			Field:    "before",
			Type:     TypeStruct,
			Name:     "my.before.value",
			Optional: true,
			Fields: []Schema{{
				Field:    "id",
				Type:     TypeInt32,
				Optional: false,
			}, {
				Field:    "data",
				Type:     TypeInt64,
				Optional: true,
			}},
		}, {
			Field:    "after",
			Type:     TypeStruct,
			Name:     "my.after.value",
			Optional: true,
			Fields: []Schema{{
				Field:    "id",
				Type:     TypeInt32,
				Optional: false,
			}, {
				Field:    "data",
				Type:     TypeBytes,
				Optional: true,
			}},
		}, {
			Field:    "source",
			Type:     TypeStruct,
			Name:     "my.source.value",
			Optional: false,
			Fields: []Schema{{
				Type:     TypeString,
				Field:    "snapshot",
				Name:     "my.enum",
				Optional: true,
				Default:  "false",
				Version:  1,
				Parameters: map[string]string{
					"allowed": "true,last,false",
				},
			}, {
				Field:    "db",
				Type:     TypeString,
				Optional: false,
			}},
		}},
	}
	jsonStr := `
{
  "fields": [
    {
      "field": "before",
      "fields": [
        {
          "field": "id",
          "type": "int32"
        },
        {
          "field": "data",
          "optional": true,
          "type": "int64"
        }
      ],
      "name": "my.before.value",
      "optional": true,
      "type": "struct"
    },
    {
      "field": "after",
      "fields": [
        {
          "field": "id",
          "type": "int32"
        },
        {
          "field": "data",
          "optional": true,
          "type": "bytes"
        }
      ],
      "name": "my.after.value",
      "optional": true,
      "type": "struct"
    },
    {
      "field": "source",
      "fields": [
        {
          "default": "false",
          "field": "snapshot",
          "name": "my.enum",
          "optional": true,
          "parameters": {
            "allowed": "true,last,false"
          },
          "type": "string",
          "version": 1
        },
        {
          "field": "db",
          "type": "string"
        }
      ],
      "name": "my.source.value",
      "type": "struct"
    }
  ],
  "name": "my.struct.name",
  "type": "struct"
}`
	t.Run("unmarshal", func(t *testing.T) {
		is := is.New(t)
		var got Schema
		err := json.Unmarshal([]byte(jsonStr), &got)
		is.NoErr(err)
		is.Equal(got, schemaNode)
	})
	t.Run("marshal", func(t *testing.T) {
		is := is.New(t)
		got, err := json.Marshal(schemaNode)
		is.NoErr(err)

		equalJSON(is, got, []byte(jsonStr))
	})

}

func equalJSON(is *is.I, s1, s2 []byte) {
	var o1 interface{}
	var o2 interface{}

	err := json.Unmarshal(s1, &o1)
	is.NoErr(err)
	err = json.Unmarshal(s2, &o2)
	is.NoErr(err)

	is.Equal(o1, o2)
}
