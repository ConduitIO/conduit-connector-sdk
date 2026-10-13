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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"go.uber.org/mock/gomock"
)

func TestDestinationPluginAdapter_Start_OpenContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

	var gotCtx context.Context
	dst.EXPECT().Open(gomock.Any()).
		DoAndReturn(func(ctx context.Context) error {
			gotCtx = ctx // assign to gotCtx so it can be inspected
			return ctx.Err()
		})

	ctx, cancel := context.WithCancel(context.Background())
	_, err := dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)
	is.NoErr(gotCtx.Err()) // expected context to be open

	// even if we cancel the context afterwards, the context in Open should stay open
	cancel()
	is.NoErr(gotCtx.Err()) // expected context to be open
}

func TestDestinationPluginAdapter_Open_ClosedContext(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

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
	_, err := dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.True(err != nil)
	is.Equal(err, ctx.Err())
	is.Equal(gotCtx.Err(), context.Canceled)
}

func TestDestinationPluginAdapter_Open_Logger(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
	wantLogger := zerolog.New(zerolog.NewTestWriter(t))

	dst.EXPECT().Open(gomock.Any()).
		DoAndReturn(func(ctx context.Context) error {
			gotLogger := Logger(ctx)
			is.True(gotLogger != nil)
			is.Equal(*gotLogger, wantLogger)
			return nil
		})

	ctx := wantLogger.WithContext(context.Background())

	_, err := dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)
}

func TestDestinationPluginAdapter_Run_Write(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

	want := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  map[string]string{"foo": "bar"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: nil, // create has no before
			After: opencdc.StructuredData{
				"x": "y",
				"z": 3,
			},
		},
	}

	dst.EXPECT().Config().Return(nil) // TODO return something
	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().Write(gomock.Any(), []opencdc.Record{want}).Return(1, nil).Times(10)

	ctx := context.Background()
	stream := NewInMemoryDestinationRunStream(ctx)

	_, err := dstPlugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: config.Config{}})
	is.NoErr(err)
	_, err = dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := dstPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	// write 10 records
	clientStream := stream.Client()
	for i := 0; i < 10; i++ {
		err = clientStream.Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{want}})
		is.NoErr(err)
		resp, err := clientStream.Recv()
		is.NoErr(err)
		is.Equal(resp, pconnector.DestinationRunResponse{
			Acks: []pconnector.DestinationRunResponseAck{{
				Position: want.Position,
				Error:    "",
			}},
		})
	}

	// close stream
	stream.Close(io.EOF)

	// wait for Run to exit
	<-runDone
}

func TestDestinationPluginAdapter_Run_WriteBatch_Success(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dst.EXPECT().Config().Return(lang.Ptr(struct{ DestinationWithBatch }{})).AnyTimes()

	dstPlugin := NewDestinationPlugin(
		DestinationWithMiddleware(dst),
		pconnector.PluginConfig{},
		config.Parameters{
			"sdk.batch.delay": config.Parameter{Type: config.ParameterTypeDuration},
			"sdk.batch.size":  config.Parameter{Type: config.ParameterTypeInt},
		},
	).(*destinationPluginAdapter)

	want := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  map[string]string{"foo": "bar"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: nil, // create has no before
			After: opencdc.StructuredData{
				"x": "y",
				"z": 3,
			},
		},
	}

	batchConfig := config.Config{
		"sdk.batch.delay": "0s",
		"sdk.batch.size":  "5",
	}

	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().Write(gomock.Any(), []opencdc.Record{want, want, want, want, want}).Return(5, nil)

	ctx := context.Background()
	stream := NewInMemoryDestinationRunStream(ctx)

	_, err := dstPlugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: batchConfig})
	is.NoErr(err)
	_, err = dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := dstPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	// write 5 records
	clientStream := stream.Client()
	for i := 0; i < 5; i++ {
		err = clientStream.Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{want}})
		is.NoErr(err)
	}

	resp, err := clientStream.Recv()
	is.NoErr(err)
	is.Equal(resp, pconnector.DestinationRunResponse{
		Acks: []pconnector.DestinationRunResponseAck{
			{Position: want.Position, Error: ""},
			{Position: want.Position, Error: ""},
			{Position: want.Position, Error: ""},
			{Position: want.Position, Error: ""},
			{Position: want.Position, Error: ""},
		},
	})

	// close stream
	stream.Close(io.EOF)

	// wait for Run to exit
	<-runDone
}

func TestDestinationPluginAdapter_Run_WriteBatch_Partial(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dst.EXPECT().Config().Return(lang.Ptr(struct{ DestinationWithBatch }{})).AnyTimes()

	dstPlugin := NewDestinationPlugin(
		DestinationWithMiddleware(dst),
		pconnector.PluginConfig{},
		config.Parameters{
			"sdk.batch.delay": config.Parameter{Type: config.ParameterTypeDuration},
			"sdk.batch.size":  config.Parameter{Type: config.ParameterTypeInt},
		},
	).(*destinationPluginAdapter)

	want := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata:  map[string]string{"foo": "bar"},
		Key:       opencdc.RawData("bar"),
		Payload: opencdc.Change{
			Before: nil, // create has no before
			After: opencdc.StructuredData{
				"x": "y",
				"z": 3,
			},
		},
	}

	batchConfig := config.Config{
		"sdk.batch.delay": "0s",
		"sdk.batch.size":  "5",
	}
	wantErr := errors.New("write error")

	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().Write(gomock.Any(), []opencdc.Record{want, want, want, want, want}).Return(3, wantErr) // only 3 records are written

	ctx := context.Background()
	stream := NewInMemoryDestinationRunStream(ctx)

	_, err := dstPlugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: batchConfig})
	is.NoErr(err)
	_, err = dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := dstPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	// write 5 records
	clientStream := stream.Client()
	for i := 0; i < 5; i++ {
		err = clientStream.Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{want}})
		is.NoErr(err)
	}

	resp, err := clientStream.Recv()
	is.NoErr(err)
	is.Equal(resp, pconnector.DestinationRunResponse{
		Acks: []pconnector.DestinationRunResponseAck{
			{Position: want.Position, Error: ""},
			{Position: want.Position, Error: ""},
			{Position: want.Position, Error: ""},
		},
	})

	resp, err = clientStream.Recv()
	is.NoErr(err)
	is.Equal(resp, pconnector.DestinationRunResponse{
		Acks: []pconnector.DestinationRunResponseAck{
			{Position: want.Position, Error: wantErr.Error()},
			{Position: want.Position, Error: wantErr.Error()},
		},
	})

	// close stream
	stream.Close(io.EOF)

	// wait for Run to exit
	<-runDone
}

func TestDestinationPluginAdapter_Stop_AwaitLastRecord(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

	lastRecord := opencdc.Record{Position: opencdc.Position("foo")}

	// ackFunc stores the ackFunc so it can be called at a later time
	dst.EXPECT().Config().Return(nil) // TODO return config
	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().Write(gomock.Any(), gomock.Any()).Return(1, nil)

	ctx := context.Background()
	stream := NewInMemoryDestinationRunStream(ctx)

	_, err := dstPlugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: config.Config{}})
	is.NoErr(err)
	_, err = dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		err := dstPlugin.Run(ctx, stream)
		is.NoErr(err)
	}()

	// initiate stop and signal what the last record will be
	// separate goroutine needed because Stop will block until last record
	// is received
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		_, err := dstPlugin.Stop(
			context.Background(),
			pconnector.DestinationStopRequest{LastPosition: lastRecord.Position},
		)
		is.NoErr(err)
	}()

	select {
	case <-stopDone:
		is.Fail() // stop returned before plugin received last record
	case <-time.After(time.Millisecond * 50):
		// continue
	}

	// send last record
	clientStream := stream.Client()
	err = clientStream.Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{lastRecord}})
	is.NoErr(err)

	// stop should still block since acknowledgment wasn't sent back yet
	select {
	case <-stopDone:
		is.Fail() // stop returned before all acks were sent back
	case <-time.After(time.Millisecond * 50):
		// continue
	}

	// let's receive the ack now
	_, err = clientStream.Recv()
	is.NoErr(err)

	select {
	case <-stopDone:
		// continue
	case <-time.After(time.Millisecond * 50):
		is.Fail() // stop didn't return even though last record was processed
	}

	// close stream at the end
	stream.Close(io.EOF)

	// wait for Run to exit
	<-runDone
}

func TestDestinationPluginAdapter_LifecycleOnCreated(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

	wantCtx := internal.Enrich(ctx, pconnector.PluginConfig{})
	want := config.Config{"foo": "bar"}
	dst.EXPECT().LifecycleOnCreated(wantCtx, want).Return(nil)

	req := pconnector.DestinationLifecycleOnCreatedRequest{Config: want}
	_, err := dstPlugin.LifecycleOnCreated(ctx, req)
	is.NoErr(err)
}

func TestDestinationPluginAdapter_LifecycleOnUpdated(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

	wantCtx := internal.Enrich(ctx, pconnector.PluginConfig{})
	wantBefore := config.Config{"foo": "bar"}
	wantAfter := config.Config{"foo": "baz"}
	dst.EXPECT().LifecycleOnUpdated(wantCtx, wantBefore, wantAfter).Return(nil)

	req := pconnector.DestinationLifecycleOnUpdatedRequest{
		ConfigBefore: wantBefore,
		ConfigAfter:  wantAfter,
	}
	_, err := dstPlugin.LifecycleOnUpdated(ctx, req)
	is.NoErr(err)
}

func TestDestinationPluginAdapter_LifecycleOnDeleted(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	dst := NewMockDestination(ctrl)

	dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

	wantCtx := internal.Enrich(ctx, pconnector.PluginConfig{})
	want := config.Config{"foo": "bar"}
	dst.EXPECT().LifecycleOnDeleted(wantCtx, want).Return(nil)

	req := pconnector.DestinationLifecycleOnDeletedRequest{Config: want}
	_, err := dstPlugin.LifecycleOnDeleted(ctx, req)
	is.NoErr(err)
}

// teardownFixture is a destination adapter whose first Write blocks inside the
// connector until released, and which records whether the connector was
// called after (or during) Teardown.
type teardownFixture struct {
	plugin       *destinationPluginAdapter
	client       pconnector.DestinationRunStreamClient
	rec          opencdc.Record
	inWrite      chan struct{}
	releaseWrite chan struct{}
	writeDone    atomic.Bool
	tornDown     atomic.Bool
	writesAfter  atomic.Int32
}

func newTeardownFixture(t *testing.T, batch bool) *teardownFixture {
	t.Helper()
	is := is.New(t)
	f := &teardownFixture{
		inWrite:      make(chan struct{}),
		releaseWrite: make(chan struct{}),
		rec: opencdc.Record{
			Position:  opencdc.Position("foo"),
			Operation: opencdc.OperationCreate,
			Key:       opencdc.RawData("bar"),
			Payload:   opencdc.Change{After: opencdc.RawData("x")},
		},
	}
	dst := NewMockDestination(gomock.NewController(t))
	f.plugin = NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

	var writeCalls atomic.Int32
	dst.EXPECT().Config().Return(nil)
	dst.EXPECT().Open(gomock.Any()).Return(nil)
	dst.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, recs []opencdc.Record) (int, error) {
			if f.tornDown.Load() {
				f.writesAfter.Add(1)
			}
			if writeCalls.Add(1) == 1 {
				close(f.inWrite)
				<-f.releaseWrite // the "connection" is busy until released
				f.writeDone.Store(true)
			}
			return len(recs), nil
		}).AnyTimes()
	dst.EXPECT().Teardown(gomock.Any()).DoAndReturn(func(context.Context) error {
		// the contract: no Write is running when Teardown is called
		if !f.writeDone.Load() {
			t.Error("Teardown was called while Write was still running")
		}
		f.tornDown.Store(true)
		return nil
	})

	ctx := context.Background()
	_, err := f.plugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: config.Config{}})
	is.NoErr(err)
	_, err = f.plugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)
	if batch {
		// the batch strategy: writes happen on its own goroutine
		f.plugin.configureWriteStrategy((&destinationWithBatch{}).setBatchConfig(ctx, DestinationWithBatch{BatchSize: 2, BatchDelay: time.Hour}))
	}

	stream := NewInMemoryDestinationRunStream(ctx)
	go func() { _ = f.plugin.Run(ctx, stream) }()
	f.client = stream.Client()
	go func() { // the in-memory stream is unbuffered: keep draining acks
		for {
			if _, err := f.client.Recv(); err != nil {
				return
			}
		}
	}()
	return f
}

// TestDestinationPluginAdapter_Teardown_WaitsForWrite: Teardown can arrive
// without a prior Stop while Run is inside the connector's Write. The connector
// is promised that no other call happens once Teardown starts, so Teardown must
// wait for the running Write and later writes must not reach the connector.
func TestDestinationPluginAdapter_Teardown_WaitsForWrite(t *testing.T) {
	for _, tc := range []struct {
		name  string
		batch bool
	}{{"single", false}, {"batch", true}} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			f := newTeardownFixture(t, tc.batch)

			recs := []opencdc.Record{f.rec}
			if tc.batch {
				recs = []opencdc.Record{f.rec, f.rec}
			}
			is.NoErr(f.client.Send(pconnector.DestinationRunRequest{Records: recs}))
			<-f.inWrite

			tdDone := make(chan error, 1)
			go func() {
				_, err := f.plugin.Teardown(context.Background(), pconnector.DestinationTeardownRequest{})
				tdDone <- err
			}()

			select {
			case <-tdDone:
				t.Fatal("Teardown returned while Write was still running")
			case <-time.After(300 * time.Millisecond):
			}

			close(f.releaseWrite)
			select {
			case err := <-tdDone:
				is.NoErr(err)
			case <-time.After(10 * time.Second):
				t.Fatal("Teardown did not return after Write returned")
			}

			// a batch that arrives after Teardown must not reach the connector
			_ = f.client.Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{f.rec, f.rec}})
			time.Sleep(500 * time.Millisecond)
			is.Equal(f.writesAfter.Load(), int32(0))
		})
	}
}
