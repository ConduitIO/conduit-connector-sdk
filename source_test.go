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
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	cpluginv1mock "github.com/conduitio/conduit-connector-protocol/cpluginv1/mock"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/conduitio/conduit-connector-sdk/internal/cchan"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestSourcePluginAdapter_Start_OpenContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)
	srcPlugin.state.Set(internal.StateConfigured) // Open expects state Configured

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
	srcPlugin.state.Set(internal.StateConfigured) // Open expects state Configured

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
	srcPlugin.state.Set(internal.StateConfigured) // Open expects state Configured
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
	srcPlugin.state.Set(internal.StateConfigured) // Open expects state Configured

	want := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  map[string]string{"foo": "bar"},
		Key:       RawData("bar"),
		Payload: Change{
			Before: nil, // create has no before
			After: StructuredData{
				"x": "y",
				"z": 3,
			},
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
				Operation: cpluginv1.Operation(want.Operation),
				Metadata:  want.Metadata,
				Key:       cpluginv1.RawData(want.Key.(RawData)),
				Payload: cpluginv1.Change{
					Before: nil, // create has no before
					After:  cpluginv1.StructuredData(want.Payload.After.(StructuredData)),
				},
			},
		})
	}

	// fetch last record
	resp := <-respStream
	is.Equal(resp, cpluginv1.SourceRunResponse{
		Record: cpluginv1.Record{
			Position:  wantLast.Position,
			Operation: cpluginv1.Operation(want.Operation),
			Metadata:  want.Metadata,
			Key:       cpluginv1.RawData(want.Key.(RawData)),
			Payload: cpluginv1.Change{
				Before: nil, // create has no before
				After:  cpluginv1.StructuredData(want.Payload.After.(StructuredData)),
			},
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

func TestSourcePluginAdapter_Run_Stuck(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	teardownTimeout = time.Second
	stopTimeout = time.Second
	defer func() {
		teardownTimeout = time.Minute // reset
		stopTimeout = time.Minute
	}()

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)
	srcPlugin.state.Set(internal.StateConfigured) // Open expects state Configured

	want := Record{
		Position: Position("foo"),
	}

	src.EXPECT().Open(gomock.Any(), nil).Return(nil)

	// first produce "normal" records, then produce last record, then return ErrBackoffRetry
	r1 := src.EXPECT().Read(gomock.Any()).Return(want, nil)
	src.EXPECT().Read(gomock.Any()).DoAndReturn(func(ctx context.Context) {
		<-make(chan struct{}) // block forever and ever
	}).After(r1)

	stream, _, respStream := newSourceRunStreamMock(ctrl)

	ctx := context.Background()
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{Position: nil})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := srcPlugin.Run(ctx, stream)
		is.Equal("forceful teardown", err.Error())
	}()

	resp := <-respStream
	is.Equal(resp, cpluginv1.SourceRunResponse{
		Record: cpluginv1.Record{
			Position: want.Position,
		},
	})

	// after this the connector starts blocking, we try to trigger a stop
	stopResp, err := srcPlugin.Stop(ctx, cpluginv1.SourceStopRequest{})
	is.True(errors.Is(err, context.DeadlineExceeded))
	is.Equal(nil, stopResp.LastPosition) // unexpected last position

	// the connector is still blocking, teardown should detach the goroutine
	src.EXPECT().Teardown(gomock.Any()).Return(nil)
	_, err = srcPlugin.Teardown(ctx, cpluginv1.SourceTeardownRequest{})
	is.True(errors.Is(err, context.DeadlineExceeded))

	// wait for Run to exit, teardown killed it
	<-runDone
}

func TestSourcePluginAdapter_Stop_WaitsForRun(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	stopTimeout = time.Second
	defer func() {
		stopTimeout = time.Minute // reset
	}()

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)
	srcPlugin.state.Set(internal.StateConfigured) // Open expects state Configured

	want := Record{
		Position: Position("foo"),
	}

	src.EXPECT().Open(gomock.Any(), nil).Return(nil)

	// produce one record, then return ErrBackoffRetry
	r1 := src.EXPECT().Read(gomock.Any()).Return(want, nil)
	src.EXPECT().Read(gomock.Any()).Return(Record{}, ErrBackoffRetry).After(r1)

	stream, reqStream, respStream := newSourceRunStreamMock(ctrl)

	// Start connector now
	ctx := context.Background()
	_, err := srcPlugin.Start(ctx, cpluginv1.SourceStartRequest{Position: nil})
	is.NoErr(err)

	// Run was not triggered yet, but we try to stop
	stopResp, err := srcPlugin.Stop(ctx, cpluginv1.SourceStopRequest{})
	is.True(errors.Is(err, context.DeadlineExceeded))
	is.Equal(nil, stopResp.LastPosition) // unexpected last position

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := srcPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	// Stop should still be blocked because there is a pending record that was not read yet
	stopResp, err = srcPlugin.Stop(ctx, cpluginv1.SourceStopRequest{})
	is.True(errors.Is(err, context.DeadlineExceeded))
	is.Equal(nil, stopResp.LastPosition) // unexpected last position

	// fetch produced record
	resp := <-respStream
	is.Equal(resp, cpluginv1.SourceRunResponse{
		Record: cpluginv1.Record{
			Position: want.Position,
		},
	})

	// after this the connector can be stopped
	stopResp, err = srcPlugin.Stop(ctx, cpluginv1.SourceStopRequest{})
	is.NoErr(err)
	is.Equal([]byte(want.Position), stopResp.LastPosition) // unexpected last position

	// close stream
	close(reqStream)
	close(respStream)

	// wait for Run to exit
	_, _, err = cchan.ChanOut[struct{}](runDone).RecvTimeout(ctx, time.Second)
	is.NoErr(err)
}

func TestSourcePluginAdapter_Teardown(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)
	srcPlugin.state.Set(internal.StateConfigured) // Open expects state Configured

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

func TestSourcePluginAdapter_LifecycleOnCreated(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	want := map[string]string{"foo": "bar"}
	src.EXPECT().LifecycleOnCreated(ctx, want).Return(nil)

	req := cpluginv1.SourceLifecycleOnCreatedRequest{Config: want}
	_, err := srcPlugin.LifecycleOnCreated(ctx, req)
	is.NoErr(err)
}

func TestSourcePluginAdapter_LifecycleOnUpdated(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	wantBefore := map[string]string{"foo": "bar"}
	wantAfter := map[string]string{"foo": "baz"}
	src.EXPECT().LifecycleOnUpdated(ctx, wantBefore, wantAfter).Return(nil)

	req := cpluginv1.SourceLifecycleOnUpdatedRequest{
		ConfigBefore: wantBefore,
		ConfigAfter:  wantAfter,
	}
	_, err := srcPlugin.LifecycleOnUpdated(ctx, req)
	is.NoErr(err)
}

func TestSourcePluginAdapter_LifecycleOnDeleted(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	src := NewMockSource(ctrl)

	srcPlugin := NewSourcePlugin(src).(*sourcePluginAdapter)

	want := map[string]string{"foo": "bar"}
	src.EXPECT().LifecycleOnDeleted(ctx, want).Return(nil)

	req := cpluginv1.SourceLifecycleOnDeletedRequest{Config: want}
	_, err := srcPlugin.LifecycleOnDeleted(ctx, req)
	is.NoErr(err)
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
