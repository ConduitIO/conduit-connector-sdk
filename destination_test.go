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

func TestDestinationPluginAdapter_Run_WriteAsync(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst).(*destinationPluginAdapter)

	want := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  map[string]string{"foo": "bar"},
		Before:    Entity{ /* create has no before */ },
		After: Entity{
			Key: RawData("bar"),
			Payload: StructuredData{
				"x": "y",
				"z": 3,
			},
		},
	}

	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().WriteAsync(gomock.Any(), want, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ Record, ackFunc AckFunc) error {
			return ackFunc(nil)
		}).Times(2)

	stream, reqStream, respStream := newDestinationRunStreamMock(ctrl)

	ctx := context.Background()
	_, err := dstPlugin.Start(ctx, cpluginv1.DestinationStartRequest{})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := dstPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	for i := 0; i < 2; i++ {
		// write the same record twice to check if WriteAsync is skipped the
		// second time
		reqStream <- cpluginv1.DestinationRunRequest{
			Record: cpluginv1.Record{
				Position:  want.Position,
				Operation: cpluginv1.Operation(want.Operation),
				Metadata:  want.Metadata,
				Before:    cpluginv1.Entity{ /* create has no before */ },
				After: cpluginv1.Entity{
					Key:     cpluginv1.RawData(want.After.Key.(RawData)),
					Payload: cpluginv1.StructuredData(want.After.Payload.(StructuredData)),
				},
			},
		}
		resp := <-respStream
		is.Equal(resp, cpluginv1.DestinationRunResponse{
			AckPosition: want.Position,
			Error:       "",
		})
	}

	// close stream after 2 records
	close(reqStream)
	close(respStream)

	// wait for Run to exit
	<-runDone
}

func TestDestinationPluginAdapter_Run_WriteFallback(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst).(*destinationPluginAdapter)

	want := Record{
		Position:  Position("foo"),
		Operation: OperationCreate,
		Metadata:  map[string]string{"foo": "bar"},
		Before:    Entity{ /* create has no before */ },
		After: Entity{
			Key: RawData("bar"),
			Payload: StructuredData{
				"x": "y",
				"z": 3,
			},
		},
	}

	dst.EXPECT().Open(gomock.Any()).Return(nil)
	// WriteAsync returns ErrUnimplemented, the sdk is expected to fall back to
	// Write, all future calls should go directly to Write
	dst.EXPECT().WriteAsync(gomock.Any(), gomock.Any(), gomock.Any()).Return(ErrUnimplemented)
	dst.EXPECT().Write(gomock.Any(), want).Return(nil).Times(2)

	stream, reqStream, respStream := newDestinationRunStreamMock(ctrl)

	ctx := context.Background()
	_, err := dstPlugin.Start(ctx, cpluginv1.DestinationStartRequest{})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := dstPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	for i := 0; i < 2; i++ {
		// write the same record twice to check if WriteAsync is skipped the
		// second time
		reqStream <- cpluginv1.DestinationRunRequest{
			Record: cpluginv1.Record{
				Position:  want.Position,
				Operation: cpluginv1.Operation(want.Operation),
				Metadata:  want.Metadata,
				Before:    cpluginv1.Entity{ /* create has no before */ },
				After: cpluginv1.Entity{
					Key:     cpluginv1.RawData(want.After.Key.(RawData)),
					Payload: cpluginv1.StructuredData(want.After.Payload.(StructuredData)),
				},
			},
		}
		resp := <-respStream
		is.Equal(resp, cpluginv1.DestinationRunResponse{
			AckPosition: want.Position,
			Error:       "",
		})
	}

	// close stream after 2 records
	close(reqStream)
	close(respStream)

	// wait for Run to exit
	<-runDone
}

func TestDestinationPluginAdapter_Stop_AwaitLastRecord(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst).(*destinationPluginAdapter)

	lastRecord := Record{Position: Position("foo")}

	// ackFunc stores the ackFunc so it can be called at a later time
	var ackFunc AckFunc
	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().WriteAsync(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ Record, af AckFunc) error {
			if ackFunc != nil {
				// second time we actually call the previous ackFunc to simulate
				// an asynchronous write
				err := ackFunc(nil)
				is.NoErr(err)
			}
			ackFunc = af
			return nil
		}).Times(2)

	stream, reqStream, respStream := newDestinationRunStreamMock(ctrl)

	ctx := context.Background()
	_, err := dstPlugin.Start(ctx, cpluginv1.DestinationStartRequest{})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := dstPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	// first write a random record and receive the ack
	reqStream <- cpluginv1.DestinationRunRequest{}
	// don't receive ack at this point, we simulate an asynchronous write

	// initiate stop and signal what the last record will be
	// separate goroutine needed because Stop will block until last record
	// is received
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		_, err := dstPlugin.Stop(
			context.Background(),
			cpluginv1.DestinationStopRequest{LastPosition: lastRecord.Position},
		)
		is.NoErr(err)
	}()

	select {
	case <-stopDone:
		is.Fail() // stop returned before plugin received last record
	case <-time.After(time.Millisecond * 50):
		// continue
	}

	// only now we prepare the expectation to Flush, it shouldn't be called
	// before receiving the last record
	dst.EXPECT().Flush(gomock.Any()).DoAndReturn(func(context.Context) error {
		return ackFunc(nil)
	})

	// send last record
	reqStream <- cpluginv1.DestinationRunRequest{
		Record: cpluginv1.Record{Position: lastRecord.Position},
	}
	// receive ack from first record
	<-respStream

	select {
	case <-stopDone:
		is.Fail() // stop returned before all acks were sent back
	case <-time.After(time.Millisecond * 50):
		// continue
	}

	// flush simulates the write of the last record, let's receive the ack now
	<-respStream

	select {
	case <-stopDone:
		// continue
	case <-time.After(time.Millisecond * 50):
		is.Fail() // stop didn't return even though last record was processed
	}

	// close stream at the end
	close(reqStream)
	close(respStream)

	// wait for Run to exit
	<-runDone
}

func newDestinationRunStreamMock(
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
