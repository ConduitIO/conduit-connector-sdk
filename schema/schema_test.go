// Copyright © 2024 Meroxa, Inc.
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

package schema

import (
	"context"
	"testing"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestInMemoryService(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	want1 := schema.Instance{
		Subject: "test-subject",
		Version: 1,
		Type:    schema.TypeAvro,
		Bytes:   []byte("irrelevant 1"),
	}

	// Create first version
	created1, err := Create(ctx, want1.Type, want1.Subject, want1.Bytes)
	is.NoErr(err)
	is.Equal("", cmp.Diff(want1, created1))

	// Create second version
	want2 := schema.Instance{
		Subject: want1.Subject,
		Version: 2,
		Type:    want1.Type,
		Bytes:   []byte("irrelevant 2"),
	}
	got2, err := Create(ctx, want2.Type, want2.Subject, want2.Bytes)
	is.NoErr(err)
	is.Equal("", cmp.Diff(want2, got2))

	// Get first version
	getResp1, err := Get(ctx, want1.Subject, 1)
	is.NoErr(err)
	is.Equal("", cmp.Diff(want1, getResp1))

	// Get second version
	getResp2, err := Get(ctx, want2.Subject, 2)
	is.NoErr(err)
	is.Equal("", cmp.Diff(want2, getResp2))
}
