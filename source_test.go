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

func TestSourcePluginAdapter_Start_OpenContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	var gotCtx context.Context
	src.EXPECT().Open(gomock.Any(), Position(nil)).
		DoAndReturn(func(ctx context.Context, _ Position) error {
			gotCtx = ctx // assign to gotCtx so it can be inspected
			return ctx.Err()
		})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{})
	is.NoErr(err)
	is.NoErr(gotCtx.Err()) // expected context to be open

	// even if we cancel the context afterwards, the context in Open should stay open
	cancel()
	is.NoErr(gotCtx.Err()) // expected context to be open
}

func TestSourcePluginAdapter_Start_ClosedContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	var gotCtx context.Context
	src.EXPECT().Open(gomock.Any(), Position(nil)).
		DoAndReturn(func(ctx context.Context, _ Position) error {
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
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{})
	is.True(err != nil)
	is.Equal(err, ctx.Err())
	is.Equal(gotCtx.Err(), context.Canceled)
}

func TestSourcePluginAdapter_Start_Logger(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)
	wantLogger := zerolog.New(zerolog.NewTestWriter(t))

	src.EXPECT().Open(gomock.Any(), Position(nil)).
		DoAndReturn(func(ctx context.Context, _ Position) error {
			gotLogger := Logger(ctx)
			is.True(gotLogger != nil)
			is.Equal(*gotLogger, wantLogger)
			return nil
		})

	ctx := wantLogger.WithContext(context.Background())

	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{})
	is.NoErr(err)
}

func TestSourcePluginAdapter_Run(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

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
	wantLast := want
	wantLast.Position = Position("bar")

	recordCount := 5

	src.EXPECT().Open(gomock.Any(), nil).Return(nil)

	// first produce "normal" records, then produce last record, then return ErrBackoffRetry
	r1 := src.EXPECT().Read(gomock.Any()).Return(want, nil).Times(recordCount - 1)
	r2 := src.EXPECT().Read(gomock.Any()).Return(wantLast, nil).After(r1)
	src.EXPECT().Read(gomock.Any()).Return(Record{}, ErrBackoffRetry).After(r2)

	stream, reqStream, respStream := newSourceRunStreamMock(ctrl)

	ctx := context.Background()
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{Position: nil})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := srcPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	for i := 0; i < recordCount-1; i++ {
		resp := <-respStream
		is.Equal(resp, cpluginv1.SourceRunResponse{
			Record: cpluginv1.Record{
				Position:  want.Position,
				Metadata:  want.Metadata,
				CreatedAt: want.CreatedAt,
				Key:       cpluginv1.RawData(want.Key.(RawData)),
				Payload:   cpluginv1.StructuredData(want.Payload.(StructuredData)),
			},
		})
	}

	// fetch last record
	resp := <-respStream
	is.Equal(resp, cpluginv1.SourceRunResponse{
		Record: cpluginv1.Record{
			Position:  wantLast.Position,
			Metadata:  wantLast.Metadata,
			CreatedAt: wantLast.CreatedAt,
			Key:       cpluginv1.RawData(wantLast.Key.(RawData)),
			Payload:   cpluginv1.StructuredData(wantLast.Payload.(StructuredData)),
		},
	})

	stopResp, err := srcPlugin.Stop(ctx, cpluginv1.SourceStopRequest{})
	is.NoErr(err)

	is.Equal([]byte(wantLast.Position), stopResp.LastPosition) // unexpected last position

	// after stop the source should stop producing new records, but it will
	// wait for acks coming back, let's send back all acks but last one
	src.EXPECT().Ack(gomock.Any(), want.Position).Times(recordCount - 1)

	for i := 0; i < recordCount-1; i++ {
		reqStream <- cpluginv1.SourceRunRequest{AckPosition: want.Position}
	}

	// close stream
	close(reqStream)
	close(respStream)

	// wait for Run to exit
	<-runDone
}

func TestSourcePluginAdapter_Teardown(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	src.EXPECT().Open(gomock.Any(), nil).Return(nil)
	r1 := src.EXPECT().Read(gomock.Any()).Return(Record{}, nil)
	src.EXPECT().Read(gomock.Any()).Return(Record{}, ErrBackoffRetry).After(r1)

	stream, reqStream, respStream := newSourceRunStreamMock(ctrl)

	ctx := context.Background()
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{Position: nil})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := srcPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	// fetch one record from stream to ensure Run started
	<-respStream

	// stop the record producing goroutine
	_, err = srcPlugin.Stop(ctx, cpluginv1.SourceStopRequest{})
	is.NoErr(err)

	// teardown should block until the stream is closed and all acks were received
	teardownDone := make(chan struct{})
	go func() {
		defer close(teardownDone)
		_, err := srcPlugin.Teardown(ctx, cpluginv1.SourceTeardownRequest{})
		is.NoErr(err)
	}()

	select {
	case <-time.After(time.Millisecond * 10):
		// all good
	case <-teardownDone:
		is.Fail() // teardown should block until stream is closed
	}

	// close stream and unblock teardown
	src.EXPECT().Teardown(gomock.Any()).Return(nil)
	close(reqStream)
	close(respStream)

	// wait for Teardown and Run to exit
	<-teardownDone
	<-runDone
}

func newSourceRunStreamMock(
	ctrl *gomock.Controller,
) (
	*cpluginv1mock.SourceRunStream,
	chan cpluginv1.SourceRunRequest,
	chan cpluginv1.SourceRunResponse,
) {
	stream := cpluginv1mock.NewSourceRunStream(ctrl)

	reqStream := make(chan cpluginv1.SourceRunRequest)
	respStream := make(chan cpluginv1.SourceRunResponse)

	stream.EXPECT().Send(gomock.Any()).
		DoAndReturn(func(resp cpluginv1.SourceRunResponse) (err error) {
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
		DoAndReturn(func() (cpluginv1.SourceRunRequest, error) {
			req, ok := <-reqStream
			if !ok {
				return cpluginv1.SourceRunRequest{}, io.EOF
			}
			return req, nil
		}).AnyTimes()

	return stream, reqStream, respStream
}
