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

package sdk

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestSourceWithSchema_Parameters(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	s := SourceWithSchema{}.Wrap(src)

	want := config.Parameters{
		"foo": {
			Default:     "bar",
			Description: "baz",
		},
	}

	src.EXPECT().Parameters().Return(want)
	got := s.Parameters()

	is.Equal(got["foo"], want["foo"])
	is.Equal(len(got), 6) // expected middleware to inject 5 parameters
}

func TestSourceWithSchema_Configure(t *testing.T) {
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)
	ctx := context.Background()

	connectorID := uuid.NewString()
	ctx = internal.Enrich(ctx, pconnector.PluginConfig{ConnectorID: connectorID})
	boolPtr := func(b bool) *bool { return &b }

	testCases := []struct {
		name       string
		middleware SourceWithSchema
		have       config.Config

		wantErr            error
		wantSchemaType     schema.Type
		wantPayloadSubject string
		wantKeySubject     string
	}{{
		name:       "empty config",
		middleware: SourceWithSchema{},
		have:       config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: connectorID + "-payload",
		wantKeySubject:     connectorID + "-key",
	}, {
		name:       "invalid schema type",
		middleware: SourceWithSchema{},
		have: config.Config{
			configSourceSchemaType: "foo",
		},
		wantErr: schema.ErrUnsupportedType,
	}, {
		name: "disabled by default",
		middleware: SourceWithSchema{
			DefaultPayloadEncode: boolPtr(false),
			DefaultKeyEncode:     boolPtr(false),
		},
		have: config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "",
		wantKeySubject:     "",
	}, {
		name:       "disabled by config",
		middleware: SourceWithSchema{},
		have: config.Config{
			configSourceSchemaPayloadEncode: "false",
			configSourceSchemaKeyEncode:     "false",
		},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "",
		wantKeySubject:     "",
	}, {
		name: "static default payload subject",
		middleware: SourceWithSchema{
			DefaultPayloadSubject: "foo",
			DefaultKeySubject:     "bar",
		},
		have: config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "foo",
		wantKeySubject:     "bar",
	}, {
		name:       "payload subject by config",
		middleware: SourceWithSchema{},
		have: config.Config{
			configSourceSchemaPayloadSubject: "foo",
			configSourceSchemaKeySubject:     "bar",
		},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "foo",
		wantKeySubject:     "bar",
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			s := tt.middleware.Wrap(src).(*sourceWithSchema)

			src.EXPECT().Configure(ctx, tt.have).Return(nil)

			err := s.Configure(ctx, tt.have)
			if tt.wantErr != nil {
				is.True(errors.Is(err, tt.wantErr))
				return
			}

			is.NoErr(err)

			is.Equal(s.schemaType, tt.wantSchemaType)
			is.Equal(s.payloadSubject, tt.wantPayloadSubject)
			is.Equal(s.keySubject, tt.wantKeySubject)
		})
	}
}
