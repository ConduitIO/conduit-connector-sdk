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
	"maps"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-sdk/internal"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// -- SourceWithSchemaExtraction -----------------------------------------------

func TestSourceWithSchemaExtractionConfig_Apply(t *testing.T) {
	is := is.New(t)

	wantCfg := SourceWithSchemaExtractionConfig{
		PayloadEnabled: ptr(true),
		KeyEnabled:     ptr(true),
		PayloadSubject: ptr("foo"),
		KeySubject:     ptr("bar"),
	}

	have := &SourceWithSchemaExtraction{}
	wantCfg.Apply(have)

	is.Equal(have.Config, wantCfg)
}

func TestSourceWithSchemaExtraction_Parameters(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	s := (&SourceWithSchemaExtraction{}).Wrap(src)

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

func TestSourceWithSchemaExtraction_Configure(t *testing.T) {
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)
	ctx := context.Background()

	connectorID := uuid.NewString()
	ctx = internal.Enrich(ctx, pconnector.PluginConfig{ConnectorID: connectorID})

	testCases := []struct {
		name       string
		middleware SourceWithSchemaExtraction
		have       config.Config

		wantErr            error
		wantSchemaType     schema.Type
		wantPayloadSubject string
		wantKeySubject     string
	}{{
		name:       "empty config",
		middleware: SourceWithSchemaExtraction{},
		have:       config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "payload",
		wantKeySubject:     "key",
	}, {
		name:       "invalid schema type",
		middleware: SourceWithSchemaExtraction{},
		have: config.Config{
			configSourceSchemaExtractionType: "foo",
		},
		wantErr: schema.ErrUnsupportedType,
	}, {
		name: "disabled by default",
		middleware: SourceWithSchemaExtraction{
			Config: SourceWithSchemaExtractionConfig{
				PayloadEnabled: ptr(false),
				KeyEnabled:     ptr(false),
			},
		},
		have: config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "",
		wantKeySubject:     "",
	}, {
		name:       "disabled by config",
		middleware: SourceWithSchemaExtraction{},
		have: config.Config{
			configSourceSchemaExtractionPayloadEnabled: "false",
			configSourceSchemaExtractionKeyEnabled:     "false",
		},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "",
		wantKeySubject:     "",
	}, {
		name: "static default payload subject",
		middleware: SourceWithSchemaExtraction{
			Config: SourceWithSchemaExtractionConfig{
				PayloadSubject: ptr("foo"),
				KeySubject:     ptr("bar"),
			},
		},
		have: config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "foo",
		wantKeySubject:     "bar",
	}, {
		name:       "payload subject by config",
		middleware: SourceWithSchemaExtraction{},
		have: config.Config{
			configSourceSchemaExtractionPayloadSubject: "foo",
			configSourceSchemaExtractionKeySubject:     "bar",
		},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "foo",
		wantKeySubject:     "bar",
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			s := tt.middleware.Wrap(src).(*sourceWithSchemaExtraction)

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

func TestSourceWithSchemaExtraction_Read(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)
	ctx := context.Background()

	s := (&SourceWithSchemaExtraction{}).Wrap(src)

	src.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
	err := s.Configure(ctx, config.Config{})
	is.NoErr(err)

	testStructuredData := opencdc.StructuredData{
		"foo":   "bar",
		"int":   1,
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}

	testCases := []struct {
		name               string
		record             opencdc.Record
		wantKeySubject     string
		wantPayloadSubject string
	}{{
		name: "no key, no payload",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw key",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured key",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw payload before",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured payload before",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw payload after",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured payload after",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all structured",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all raw",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "key raw payload structured",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: nil,
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "key structured payload raw",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all structured with collection",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "foo",
			},
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "foo.key",
		wantPayloadSubject: "foo.payload",
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			src.EXPECT().Read(ctx).Return(tc.record, nil)

			var wantKey, wantPayloadBefore, wantPayloadAfter opencdc.Data
			if tc.record.Key != nil {
				wantKey = tc.record.Key.Clone()
			}
			if tc.record.Payload.Before != nil {
				wantPayloadBefore = tc.record.Payload.Before.Clone()
			}
			if tc.record.Payload.After != nil {
				wantPayloadAfter = tc.record.Payload.After.Clone()
			}

			got, err := s.Read(ctx)
			is.NoErr(err)

			gotKey := got.Key
			gotPayloadBefore := got.Payload.Before
			gotPayloadAfter := got.Payload.After

			if _, ok := wantKey.(opencdc.StructuredData); ok {
				subject, err := got.Metadata.GetKeySchemaSubject()
				is.NoErr(err)
				version, err := got.Metadata.GetKeySchemaVersion()
				is.NoErr(err)

				is.Equal(subject, tc.wantKeySubject)
				is.Equal(version, 1)

				sch, err := sdkschema.Get(ctx, subject, version)
				is.NoErr(err)

				var sd opencdc.StructuredData
				err = sch.Unmarshal(gotKey.Bytes(), &sd)
				is.NoErr(err)

				gotKey = sd
			} else {
				_, err := got.Metadata.GetKeySchemaSubject()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
				_, err = got.Metadata.GetKeySchemaVersion()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
			}

			_, isPayloadBeforeStructured := wantPayloadBefore.(opencdc.StructuredData)
			_, isPayloadAfterStructured := wantPayloadAfter.(opencdc.StructuredData)
			if isPayloadBeforeStructured || isPayloadAfterStructured {
				subject, err := got.Metadata.GetPayloadSchemaSubject()
				is.NoErr(err)
				version, err := got.Metadata.GetPayloadSchemaVersion()
				is.NoErr(err)

				is.Equal(subject, tc.wantPayloadSubject)
				is.Equal(version, 1)

				sch, err := sdkschema.Get(ctx, subject, version)
				is.NoErr(err)

				if isPayloadBeforeStructured {
					var sd opencdc.StructuredData
					err = sch.Unmarshal(gotPayloadBefore.Bytes(), &sd)
					is.NoErr(err)
					gotPayloadBefore = sd
				}
				if isPayloadAfterStructured {
					var sd opencdc.StructuredData
					err = sch.Unmarshal(gotPayloadAfter.Bytes(), &sd)
					is.NoErr(err)
					gotPayloadAfter = sd
				}
			} else {
				_, err := got.Metadata.GetPayloadSchemaSubject()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
				_, err = got.Metadata.GetPayloadSchemaVersion()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
			}

			is.Equal(gotKey, wantKey)
			is.Equal(gotPayloadBefore, wantPayloadBefore)
			is.Equal(gotPayloadAfter, wantPayloadAfter)
		})
	}
}

// -- SourceWithSchemaContext --------------------------------------------------

func TestSourceWithSchemaContext_Parameters(t *testing.T) {
	testCases := []struct {
		name       string
		mwCfg      SourceWithSchemaContextConfig
		wantParams config.Parameters
	}{
		{
			name:  "default middleware config",
			mwCfg: SourceWithSchemaContextConfig{},
			wantParams: config.Parameters{
				"sdk.schema.context.enabled": {
					Default: "true",
					Description: "Specifies whether to use a schema context name. If set to false, no schema context name " +
						"will be used, and schemas will be saved with the subject name specified in the connector " +
						"(not safe because of name conflicts).",
					Type: config.ParameterTypeBool,
				},
				"sdk.schema.context.name": {
					Default: "",
					Description: "Schema context name to be used. Used as a prefix for all schema subject names. " +
						"Defaults to the connector ID.",
					Type: config.ParameterTypeString,
				},
			},
		},
		{
			name: "custom middleware config",
			mwCfg: SourceWithSchemaContextConfig{
				Enabled: ptr(false),
				Name:    ptr("foobar"),
			},
			wantParams: config.Parameters{
				"sdk.schema.context.enabled": {
					Default: "false",
					Description: "Specifies whether to use a schema context name. If set to false, no schema context name " +
						"will be used, and schemas will be saved with the subject name specified in the connector " +
						"(not safe because of name conflicts).",
					Type: config.ParameterTypeBool,
				},
				"sdk.schema.context.name": {
					Default:     "foobar",
					Description: "Schema context name to be used. Used as a prefix for all schema subject names.",
					Type:        config.ParameterTypeString,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctrl := gomock.NewController(t)
			src := NewMockSource(ctrl)

			s := (&SourceWithSchemaContext{
				Config: tc.mwCfg,
			}).Wrap(src)

			connectorParams := config.Parameters{
				"foo": {
					Default:     "bar",
					Description: "baz",
				},
			}

			src.EXPECT().Parameters().Return(connectorParams)
			got := s.Parameters()

			want := config.Parameters{}
			maps.Copy(want, connectorParams)
			maps.Copy(want, tc.wantParams)

			is.Equal("", cmp.Diff(want, got))
		})
	}
}

func TestSourceWithSchemaContext_Configure(t *testing.T) {
	connID := "test-connector-id"

	testCases := []struct {
		name            string
		middlewareCfg   SourceWithSchemaContextConfig
		connectorCfg    config.Config
		wantContextName string
	}{
		{
			name:            "default middleware config, no user config",
			middlewareCfg:   SourceWithSchemaContextConfig{},
			connectorCfg:    config.Config{},
			wantContextName: connID,
		},
		{
			name: "custom context in middleware, no user config",
			middlewareCfg: SourceWithSchemaContextConfig{
				Enabled: ptr(true),
				Name:    ptr("foobar"),
			},
			connectorCfg:    config.Config{},
			wantContextName: "foobar",
		},
		{
			name: "middleware config: use context false, no user config",
			middlewareCfg: SourceWithSchemaContextConfig{
				Enabled: ptr(false),
				Name:    ptr("foobar"),
			},
			connectorCfg:    config.Config{},
			wantContextName: "",
		},
		{
			name: "user config overrides use context",
			middlewareCfg: SourceWithSchemaContextConfig{
				Enabled: ptr(false),
				Name:    ptr("foobar"),
			},
			connectorCfg: config.Config{
				"sdk.schema.context.enabled": "true",
			},
			wantContextName: "foobar",
		},
		{
			name: "user config overrides context name, non-empty",
			middlewareCfg: SourceWithSchemaContextConfig{
				Enabled: ptr(true),
				Name:    ptr("foobar"),
			},
			connectorCfg: config.Config{
				"sdk.schema.context.use":  "true",
				"sdk.schema.context.name": "user-context-name",
			},
			wantContextName: "user-context-name",
		},
		{
			name: "user config overrides context name, empty",
			middlewareCfg: SourceWithSchemaContextConfig{
				Enabled: ptr(true),
				Name:    ptr("foobar"),
			},
			connectorCfg: config.Config{
				"sdk.schema.context.use":  "true",
				"sdk.schema.context.name": "",
			},
			wantContextName: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := internal.ContextWithConnectorID(context.Background(), connID)

			s := NewMockSource(gomock.NewController(t))
			mw := &SourceWithSchemaContext{}

			tc.middlewareCfg.Apply(mw)
			underTest := mw.Wrap(s)

			s.EXPECT().
				Configure(gomock.Any(), tc.connectorCfg).
				DoAndReturn(func(ctx context.Context, c config.Config) error {
					gotContextName := sdkschema.GetSchemaContextName(ctx)
					is.Equal(tc.wantContextName, gotContextName)
					return nil
				})

			err := underTest.Configure(ctx, tc.connectorCfg)
			is.NoErr(err)
		})
	}
}

func TestSourceWithSchemaContext_ContextValue(t *testing.T) {
	is := is.New(t)
	connID := "test-connector-id"
	connectorCfg := config.Config{
		"sdk.schema.context.use":  "true",
		"sdk.schema.context.name": "user-context-name",
	}
	wantContextName := "user-context-name"
	ctx := internal.ContextWithConnectorID(context.Background(), connID)

	s := NewMockSource(gomock.NewController(t))
	underTest := (&SourceWithSchemaContext{}).Wrap(s)

	s.EXPECT().
		Configure(gomock.Any(), connectorCfg).
		DoAndReturn(func(ctx context.Context, _ config.Config) error {
			is.Equal(wantContextName, sdkschema.GetSchemaContextName(ctx))
			return nil
		})
	s.EXPECT().
		Open(gomock.Any(), opencdc.Position{}).
		DoAndReturn(func(ctx context.Context, _ opencdc.Position) error {
			is.Equal(wantContextName, sdkschema.GetSchemaContextName(ctx))
			return nil
		})

	err := underTest.Configure(ctx, connectorCfg)
	is.NoErr(err)

	err = underTest.Open(ctx, opencdc.Position{})
	is.NoErr(err)
}
