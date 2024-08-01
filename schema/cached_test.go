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
	"github.com/conduitio/conduit-connector-protocol/pconnutils"
	"github.com/conduitio/conduit-connector-protocol/pconnutils/mock"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestCachedSchemaService_Get(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	schemaServiceMock := mock.NewSchemaService(ctrl)

	req1 := pconnutils.GetSchemaRequest{
		Subject: "test",
		Version: 1,
	}
	resp1 := pconnutils.GetSchemaResponse{
		Schema: schema.Schema{
			ID:      1,
			Subject: "test",
			Version: 1,
			Type:    schema.TypeAvro,
			Bytes:   []byte("int"),
		},
	}

	req2 := pconnutils.GetSchemaRequest{
		Subject: "test",
		Version: 2,
	}
	resp2 := pconnutils.GetSchemaResponse{
		Schema: schema.Schema{
			ID:      2,
			Subject: "test",
			Version: 2,
			Type:    schema.TypeAvro,
			Bytes:   []byte("string"),
		},
	}

	// The underlying mock should be called only once per unique request
	schemaServiceMock.EXPECT().GetSchema(ctx, req1).Return(resp1, nil).Times(1)
	schemaServiceMock.EXPECT().GetSchema(ctx, req2).Return(resp2, nil).Times(1)

	schemaService := newCachedSchemaService(schemaServiceMock)

	for i := 0; i < 10; i++ {
		gotResp1, err := schemaService.GetSchema(ctx, req1)
		is.NoErr(err)
		is.Equal(resp1, gotResp1)

		gotResp2, err := schemaService.GetSchema(ctx, req2)
		is.NoErr(err)
		is.Equal(resp2, gotResp2)
	}
}

func TestCachedSchemaService_Create(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	schemaServiceMock := mock.NewSchemaService(ctrl)

	req1 := pconnutils.CreateSchemaRequest{
		Subject: "test",
		Type:    schema.TypeAvro,
		Bytes:   []byte("int"),
	}
	resp1 := pconnutils.CreateSchemaResponse{
		Schema: schema.Schema{
			ID:      1,
			Subject: "test",
			Version: 1,
			Type:    schema.TypeAvro,
			Bytes:   []byte("int"),
		},
	}

	req2 := pconnutils.CreateSchemaRequest{
		Subject: "test",
		Type:    schema.TypeAvro,
		Bytes:   []byte("string"),
	}
	resp2 := pconnutils.CreateSchemaResponse{
		Schema: schema.Schema{
			ID:      2,
			Subject: "test",
			Version: 2,
			Type:    schema.TypeAvro,
			Bytes:   []byte("string"),
		},
	}

	// The underlying mock should be called only once per unique request
	schemaServiceMock.EXPECT().CreateSchema(ctx, req1).Return(resp1, nil).Times(1)
	schemaServiceMock.EXPECT().CreateSchema(ctx, req2).Return(resp2, nil).Times(1)

	schemaService := newCachedSchemaService(schemaServiceMock)

	for i := 0; i < 10; i++ {
		gotResp1, err := schemaService.CreateSchema(ctx, req1)
		is.NoErr(err)
		is.Equal(resp1, gotResp1)

		gotResp2, err := schemaService.CreateSchema(ctx, req2)
		is.NoErr(err)
		is.Equal(resp2, gotResp2)
	}
}
