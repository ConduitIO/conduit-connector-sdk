// Copyright © 2022 Meroxa, Inc.
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
	"io"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	cpluginv1mock "github.com/conduitio/conduit-connector-protocol/cpluginv1/mock"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestDestinationPluginAdapter_Start_OpenContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst).(*destinationPluginAdapter)

	var gotCtx context.Context
	dst.EXPECT().Open(gomock.Any()).
		DoAndReturn(func(ctx context.Context) error {
			gotCtx = ctx // assign to gotCtx so it can be inspected
			return ctx.Err()
		})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := dstPlugin.Start(ctx, cpluginv1.DestinationStartRequest{})
	is.NoErr(err)
	is.NoErr(gotCtx.Err()) // expected context to be open

	// even if we cancel the context afterwards, the context in Open should stay open
	cancel()
	is.NoErr(gotCtx.Err()) // expected context to be open
}

func TestDestinationPluginAdapter_Start_ClosedContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst).(*destinationPluginAdapter)

	var gotCtx context.Context
	dst.EXPECT().Open(gomock.Any()).
		DoAndReturn(func(ctx context.Context) error {
			gotCtx = ctx // assign to gotCtx so it can be inspected
			select {
			case <-ctx.Done():
				return ctx.Err() // that's expected
			case <-time.After(time.Millisecond * 10):
				is.Fail() // didn't see context getting closed in time
				return nil
			}
		})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := dstPlugin.Start(ctx, cpluginv1.DestinationStartRequest{})
	is.True(err != nil)
	is.Equal(err, ctx.Err())
	is.Equal(gotCtx.Err(), context.Canceled)
}

func TestDestinationPluginAdapter_Start_Logger(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst).(*destinationPluginAdapter)
	wantLogger := zerolog.New(zerolog.NewTestWriter(t))

	dst.EXPECT().Open(gomock.Any()).
		DoAndReturn(func(ctx context.Context) error {
			gotLogger := Logger(ctx)
			is.True(gotLogger != nil)
			is.Equal(*gotLogger, wantLogger)
			return nil
		})

	ctx := wantLogger.WithContext(context.Background())

	_, err := dstPlugin.Start(ctx, cpluginv1.DestinationStartRequest{})
	is.NoErr(err)
}

func TestDestinationPluginAdapter_Run_Success(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst).(*destinationPluginAdapter)

	want := Record{
		Position:  Position("foo"),
		Metadata:  map[string]string{"foo": "bar"},
		CreatedAt: time.Now().UTC(),
		Key:       RawData("bar"),
		Payload: StructuredData{
			"x": "y",
			"z": 3,
		},
	}

	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().WriteAsync(gomock.Any(), gomock.Any(), gomock.Any()).Return(ErrUnimplemented)
	dst.EXPECT().Write(gomock.Any(), want).Return(nil)

	stream, reqStream, respStream := NewDestinationRunStreamMock(ctrl)

	ctx := context.Background()
	_, err := dstPlugin.Start(ctx, cpluginv1.DestinationStartRequest{})
	is.NoErr(err)

	req := cpluginv1.DestinationRunRequest{
		Record: cpluginv1.Record{
			Position:  want.Position,
			Metadata:  want.Metadata,
			CreatedAt: want.CreatedAt,
			Key:       cpluginv1.RawData(want.Key.(RawData)),
			Payload:   cpluginv1.StructuredData(want.Payload.(StructuredData)),
		},
	}
	go func() {
		reqStream <- req
	}()
	go func() {
		resp := <-respStream
		is.Equal(resp, cpluginv1.DestinationRunResponse{
			AckPosition: want.Position,
			Error:       "",
		})
		close(reqStream)
		close(respStream)
	}()

	err = dstPlugin.Run(ctx, stream)
	is.NoErr(err)
}

func NewDestinationRunStreamMock(
	ctrl *gomock.Controller,
) (
	*cpluginv1mock.DestinationRunStream,
	chan cpluginv1.DestinationRunRequest,
	chan cpluginv1.DestinationRunResponse,
) {
	stream := cpluginv1mock.NewDestinationRunStream(ctrl)

	reqStream := make(chan cpluginv1.DestinationRunRequest)
	respStream := make(chan cpluginv1.DestinationRunResponse)

	stream.EXPECT().Send(gomock.Any()).
		DoAndReturn(func(resp cpluginv1.DestinationRunResponse) (err error) {
			defer func() {
				if r := recover(); r != nil {
					var ok bool
					err, ok = r.(error)
					if !ok {
						err = fmt.Errorf("%+v", r)
					}
				}
			}()
			respStream <- resp
			return nil
		}).AnyTimes()

	stream.EXPECT().Recv().
		DoAndReturn(func() (cpluginv1.DestinationRunRequest, error) {
			req, ok := <-reqStream
			if !ok {
				return cpluginv1.DestinationRunRequest{}, io.EOF
			}
			return req, nil
		}).AnyTimes()

	return stream, reqStream, respStream
}
