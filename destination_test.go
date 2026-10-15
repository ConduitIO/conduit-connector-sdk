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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
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

// TestDestinationPluginAdapter_Stop_BatchDrained is the regression test for
// ConduitIO/conduit#3092: with batching, Stop after a drain blocked in Flush
// until its 1 minute timeout whenever the last record had filled a batch,
// because Flush waited for the result of a flush that never happened. Conduit's
// graceful shutdown then took a minute and failed with "context deadline
// exceeded".
//
// Stop gets a generous deadline instead of the default one, so a regression
// fails in seconds with that error instead of passing slowly; nothing here
// waits on time.
func TestDestinationPluginAdapter_Stop_BatchDrained(t *testing.T) {
	recs := func(from, to int) []opencdc.Record {
		var out []opencdc.Record
		for i := from; i <= to; i++ {
			out = append(out, opencdc.Record{
				Position: opencdc.Position(strconv.Itoa(i)),
				Payload:  opencdc.Change{After: opencdc.RawData("x")},
			})
		}
		return out
	}
	for _, tc := range []struct {
		name  string
		batch DestinationWithBatch
		// sends are the Run requests, in order
		sends [][]opencdc.Record
		// acksBeforeStop are received before Stop is called (the drain)
		acksBeforeStop int
		// acksDuringStop are flushed by Stop itself
		acksDuringStop int
	}{{
		// the #3092 case: the last record fills the batch, Write flushes it
		// synchronously and consumes the result
		name:           "last record fills the batch",
		batch:          DestinationWithBatch{BatchSize: 5},
		sends:          [][]opencdc.Record{recs(1, 1), recs(2, 2), recs(3, 3), recs(4, 4), recs(5, 5)},
		acksBeforeStop: 5,
	}, {
		name:           "one request fills the batch",
		batch:          DestinationWithBatch{BatchSize: 5, BatchDelay: time.Hour},
		sends:          [][]opencdc.Record{recs(1, 5)},
		acksBeforeStop: 5,
	}, {
		name:           "several full batches",
		batch:          DestinationWithBatch{BatchSize: 2},
		sends:          [][]opencdc.Record{recs(1, 1), recs(2, 2), recs(3, 3), recs(4, 4)},
		acksBeforeStop: 4,
	}, {
		// the delay timer flushed the last batch; its result is still buffered
		// and must be collected without waiting
		name:           "delay timer flushed the last batch",
		batch:          DestinationWithBatch{BatchSize: 100, BatchDelay: time.Millisecond},
		sends:          [][]opencdc.Record{recs(1, 1), recs(2, 2), recs(3, 3)},
		acksBeforeStop: 3,
	}, {
		// unchanged behavior: cached records are written by Stop
		name:           "records still cached",
		batch:          DestinationWithBatch{BatchSize: 100, BatchDelay: time.Hour},
		sends:          [][]opencdc.Record{recs(1, 1), recs(2, 2), recs(3, 3)},
		acksDuringStop: 3,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			dst := NewMockDestination(gomock.NewController(t))
			plugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

			dst.EXPECT().Config().Return(nil)
			dst.EXPECT().Open(gomock.Any()).Return(nil)
			dst.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, recs []opencdc.Record) (int, error) {
					return len(recs), nil
				}).AnyTimes()

			ctx := context.Background()
			_, err := plugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: config.Config{}})
			is.NoErr(err)
			_, err = plugin.Open(ctx, pconnector.DestinationOpenRequest{})
			is.NoErr(err)
			plugin.configureWriteStrategy((&destinationWithBatch{}).setBatchConfig(ctx, tc.batch))

			stream := NewInMemoryDestinationRunStream(ctx)
			runDone := make(chan error, 1)
			go func() { runDone <- plugin.Run(ctx, stream) }()
			client := stream.Client()

			acks := make(chan pconnector.DestinationRunResponseAck, 1000)
			go func() { // the in-memory stream is unbuffered: keep draining acks
				for {
					resp, err := client.Recv()
					if err != nil {
						return
					}
					for _, a := range resp.Acks {
						acks <- a
					}
				}
			}()
			recvAcks := func(n int) {
				t.Helper()
				for range n {
					a := <-acks
					is.Equal(a.Error, "")
				}
			}

			var last opencdc.Position
			for _, s := range tc.sends {
				is.NoErr(client.Send(pconnector.DestinationRunRequest{Records: s}))
				last = s[len(s)-1].Position
			}
			recvAcks(tc.acksBeforeStop) // drained: everything sent so far is acked

			stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_, err = plugin.Stop(stopCtx, pconnector.DestinationStopRequest{LastPosition: last})
			is.NoErr(err) // before the fix: "context deadline exceeded" when a batch was full
			recvAcks(tc.acksDuringStop)

			select {
			case a := <-acks:
				t.Fatalf("unexpected extra ack %q", a.Position)
			default:
			}

			stream.Close(io.EOF)
			is.NoErr(<-runDone)
		})
	}
}

// recordingRunStream is a DestinationRunStream fake for adapter tests. Recv
// serves the requests put on reqs (io.EOF once reqs is closed) and calls
// onRecv first, if set. Send records the acks before it returns, so they can
// be inspected as soon as the adapter call that sent them returns; sendHook,
// if set, runs first and can fail the send.
type recordingRunStream struct {
	reqs     chan pconnector.DestinationRunRequest
	onRecv   func()
	sendHook func([]pconnector.DestinationRunResponseAck) error

	mu   sync.Mutex
	acks []pconnector.DestinationRunResponseAck
}

func newRecordingRunStream(buffer int) *recordingRunStream {
	return &recordingRunStream{reqs: make(chan pconnector.DestinationRunRequest, buffer)}
}

func (s *recordingRunStream) Client() pconnector.DestinationRunStreamClient { return nil }
func (s *recordingRunStream) Server() pconnector.DestinationRunStreamServer { return s }

func (s *recordingRunStream) Recv() (pconnector.DestinationRunRequest, error) {
	if s.onRecv != nil {
		s.onRecv()
	}
	req, ok := <-s.reqs
	if !ok {
		return pconnector.DestinationRunRequest{}, io.EOF
	}
	return req, nil
}

func (s *recordingRunStream) Send(resp pconnector.DestinationRunResponse) error {
	if s.sendHook != nil {
		if err := s.sendHook(resp.Acks); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acks = append(s.acks, resp.Acks...)
	return nil
}

func (s *recordingRunStream) ackSnapshot() []pconnector.DestinationRunResponseAck {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pconnector.DestinationRunResponseAck(nil), s.acks...)
}

// recordingDestination is a connector that writes every record successfully
// and records the positions it was asked to write.
type recordingDestination struct {
	UnimplementedDestination

	mu      sync.Mutex
	written []opencdc.Position
}

func (d *recordingDestination) Config() DestinationConfig      { return nil }
func (d *recordingDestination) Open(context.Context) error     { return nil }
func (d *recordingDestination) Teardown(context.Context) error { return nil }

func (d *recordingDestination) Write(_ context.Context, recs []opencdc.Record) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range recs {
		d.written = append(d.written, r.Position)
	}
	return len(recs), nil
}

func (d *recordingDestination) snapshot() []opencdc.Position {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]opencdc.Position(nil), d.written...)
}

// newBatchingAdapter returns an opened adapter for d with the given batching.
func newBatchingAdapter(t *testing.T, d Destination, batch DestinationWithBatch) *destinationPluginAdapter {
	t.Helper()
	is := is.New(t)
	ctx := context.Background()
	plugin := NewDestinationPlugin(d, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
	_, err := plugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: config.Config{}})
	is.NoErr(err)
	_, err = plugin.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)
	plugin.configureWriteStrategy((&destinationWithBatch{}).setBatchConfig(ctx, batch))
	return plugin
}

// TestDestinationPluginAdapter_Stop_BatchDrained_TimerFlushError covers the
// error path of the nothing-cached branch of writeStrategyBatch.Flush: the
// delay timer flushed the last batch and failed to send its acks, and that
// failure, left pending on the results channel, must be returned by Stop.
//
// The ordering is pinned with channels, not time: the ack Send blocks until
// Run is back in Recv (so Write has returned without seeing a result), then
// fails; Stop's Flush can only look at the results channel after the timer
// flush has released the batcher lock, i.e. after the result is pending.
func TestDestinationPluginAdapter_Stop_BatchDrained_TimerFlushError(t *testing.T) {
	is := is.New(t)
	wantErr := errors.New("ack stream broken")

	dst := &recordingDestination{}
	plugin := newBatchingAdapter(t, dst,
		DestinationWithBatch{BatchSize: 100, BatchDelay: time.Millisecond})

	stream := newRecordingRunStream(1)
	recvCalls := make(chan struct{}, 10)
	stream.onRecv = func() { recvCalls <- struct{}{} }
	sendStarted := make(chan struct{})
	failSend := make(chan struct{})
	stream.sendHook = func([]pconnector.DestinationRunResponseAck) error {
		close(sendStarted)
		<-failSend
		return wantErr
	}

	ctx := context.Background()
	runDone := make(chan error, 1)
	go func() { runDone <- plugin.Run(ctx, stream) }()

	<-recvCalls // Run waits for the first request
	rec := opencdc.Record{Position: opencdc.Position("1"), Payload: opencdc.Change{After: opencdc.RawData("x")}}
	stream.reqs <- pconnector.DestinationRunRequest{Records: []opencdc.Record{rec}}
	<-sendStarted // the delay timer is flushing the batch, holding the batcher lock
	<-recvCalls   // Write returned (no result yet) and Run is back in Recv
	close(failSend)

	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := plugin.Stop(stopCtx, pconnector.DestinationStopRequest{LastPosition: rec.Position})
	is.True(errors.Is(err, wantErr))                           // the timer flush's failure is reported by Stop
	is.Equal(dst.snapshot(), []opencdc.Position{rec.Position}) // written, but its ack never got through
	is.Equal(len(stream.ackSnapshot()), 0)

	close(stream.reqs)
	is.NoErr(<-runDone)
}

// TestDestinationPluginAdapter_Stop_BatchRandomized drives the batching
// adapter through Run and Stop with random batch sizes, delays and request
// shapes, Stop right after the last request. It catches ConduitIO/
// conduit-connector-sdk#413: a delay-timer flush landing just before a request
// that alone fills a batch wedged the adapter for good.
//
// For each iteration, when Stop returns: it took under a second and returned
// no error, every record was written and acked once and in order, and nothing
// is written afterwards. Set SDK_BATCH_RANDOM_SEED to replay a failing seed.
func TestDestinationPluginAdapter_Stop_BatchRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	if v := os.Getenv("SDK_BATCH_RANDOM_SEED"); v != "" {
		var err error
		if seed, err = strconv.ParseUint(v, 10, 64); err != nil {
			t.Fatalf("SDK_BATCH_RANDOM_SEED: %v", err)
		}
	}
	t.Logf("seed %d", seed)
	rnd := rand.New(rand.NewPCG(seed, 0))

	for iter := range 300 {
		in := newRandomBatchInput(rnd)
		in.desc = fmt.Sprintf("seed %d, iteration %d: %s", seed, iter, in.desc)
		runRandomBatchIteration(t, in)
	}
}

type randomBatchInput struct {
	batch DestinationWithBatch
	reqs  []pconnector.DestinationRunRequest
	want  []opencdc.Position // every record's position, in order
	desc  string
}

// newRandomBatchInput draws batch size 1-7, delay 0-300µs and 1-20 requests of
// 1-3 records. Half the delays are under 10µs: #413 needs the timer to fire
// inside a window of a few instructions, which only short delays hit often.
func newRandomBatchInput(rnd *rand.Rand) randomBatchInput {
	delay := time.Duration(rnd.IntN(301)) * time.Microsecond
	if rnd.IntN(2) == 0 {
		delay = time.Duration(rnd.IntN(11)) * time.Microsecond
	}
	in := randomBatchInput{batch: DestinationWithBatch{BatchSize: 1 + rnd.IntN(7), BatchDelay: delay}}
	n := 1 + rnd.IntN(20)
	in.reqs = make([]pconnector.DestinationRunRequest, 0, n)
	for range n {
		recs := make([]opencdc.Record, 1+rnd.IntN(3))
		for i := range recs {
			pos := opencdc.Position(strconv.Itoa(len(in.want)))
			in.want = append(in.want, pos)
			recs[i] = opencdc.Record{Position: pos, Payload: opencdc.Change{After: opencdc.RawData("x")}}
		}
		in.reqs = append(in.reqs, pconnector.DestinationRunRequest{Records: recs})
	}
	in.desc = fmt.Sprintf("batch size %d, delay %s, %d requests, %d records",
		in.batch.BatchSize, in.batch.BatchDelay, len(in.reqs), len(in.want))
	return in
}

func runRandomBatchIteration(t *testing.T, in randomBatchInput) {
	t.Helper()
	posEqual := func(a, b opencdc.Position) bool { return bytes.Equal(a, b) }

	dst := &recordingDestination{}
	plugin := newBatchingAdapter(t, dst, in.batch)
	// buffered, so a wedged Run cannot block the test; the Stop bound reports it
	stream := newRecordingRunStream(len(in.reqs))
	ctx := context.Background()
	runDone := make(chan error, 1)
	go func() { runDone <- plugin.Run(ctx, stream) }()
	for _, r := range in.reqs {
		stream.reqs <- r
	}

	// Stop's context bounds only its position watch: with #413 the wedged
	// Write holds the batcher lock and Stop's flush waits for it forever, so
	// Stop runs on its own goroutine with a hard bound.
	stopCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	start := time.Now()
	stopDone := make(chan error, 1)
	go func() {
		_, err := plugin.Stop(stopCtx, pconnector.DestinationStopRequest{LastPosition: in.want[len(in.want)-1]})
		stopDone <- err
	}()
	var err error
	select {
	case err = <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: Stop did not return (wedged, #413)", in.desc)
	}
	if elapsed := time.Since(start); err != nil || elapsed >= time.Second {
		t.Fatalf("%s: Stop returned %v after %s", in.desc, err, elapsed)
	}

	// when Stop returns, everything is written and acked, in order
	if written := dst.snapshot(); !slices.EqualFunc(written, in.want, posEqual) {
		t.Fatalf("%s: written when Stop returned %q, want %q", in.desc, written, in.want)
	}
	acks := stream.ackSnapshot()
	acked := make([]opencdc.Position, 0, len(acks))
	for _, a := range acks {
		if a.Error != "" {
			t.Fatalf("%s: record %s nacked: %s", in.desc, a.Position, a.Error)
		}
		acked = append(acked, a.Position)
	}
	if !slices.EqualFunc(acked, in.want, posEqual) {
		t.Fatalf("%s: acked when Stop returned %q, want %q", in.desc, acked, in.want)
	}

	close(stream.reqs)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("%s: Run: %v", in.desc, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: Run did not return after the stream closed", in.desc)
	}
	if n := len(dst.snapshot()); n != len(in.want) {
		t.Fatalf("%s: %d records written after Stop returned", in.desc, n-len(in.want))
	}
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
	// allowTeardownDuringWrite is set by the test that exercises the timeout
	// path, where Teardown is documented to run while a Write is in flight.
	allowTeardownDuringWrite atomic.Bool
	writesAfter              atomic.Int32

	acksMu sync.Mutex
	acks   []pconnector.DestinationRunResponseAck // every ack/nack seen on the stream
}

func (f *teardownFixture) ackSnapshot() []pconnector.DestinationRunResponseAck {
	f.acksMu.Lock()
	defer f.acksMu.Unlock()
	return append([]pconnector.DestinationRunResponseAck(nil), f.acks...)
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
		if !f.writeDone.Load() && !f.allowTeardownDuringWrite.Load() {
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
			resp, err := f.client.Recv()
			if err != nil {
				return
			}
			f.acksMu.Lock()
			f.acks = append(f.acks, resp.Acks...)
			f.acksMu.Unlock()
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

			// The Write that Teardown waited for was acked without an error.
			// The late batch was neither acked nor nacked: a nack could make
			// Conduit send it to the DLQ.
			for _, ack := range f.ackSnapshot() {
				is.Equal(ack.Error, "")
			}
			is.Equal(len(f.ackSnapshot()), len(recs))
		})
	}
}

// TestDestinationPluginAdapter_Teardown_AbandonedWrite covers the timeout path:
// the Write does not return within destinationTeardownTimeout, so Teardown goes
// on and calls the connector's Teardown while the Write is still running. The
// Write must not be acked when it finally returns, and nothing must leak.
func TestDestinationPluginAdapter_Teardown_AbandonedWrite(t *testing.T) {
	old := destinationTeardownTimeout
	destinationTeardownTimeout = 100 * time.Millisecond
	t.Cleanup(func() { destinationTeardownTimeout = old })

	is := is.New(t)
	f := newTeardownFixture(t, false)
	f.allowTeardownDuringWrite.Store(true)
	is.NoErr(f.client.Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{f.rec}}))
	<-f.inWrite

	start := time.Now()
	_, err := f.plugin.Teardown(context.Background(), pconnector.DestinationTeardownRequest{})
	is.True(err != nil) // the abandonment is reported
	is.True(errors.Is(err, context.DeadlineExceeded))
	is.True(time.Since(start) < 5*time.Second)
	is.True(f.tornDown.Load()) // the connector's Teardown was called

	// The abandoned Write returns, successfully. Nothing is acked.
	close(f.releaseWrite)
	time.Sleep(300 * time.Millisecond)
	is.Equal(len(f.ackSnapshot()), 0)

	// Giving up on the Write left no goroutine parked in the wait.
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, frame := range []string{"closeAndWait", "csync.Run", "sync.(*WaitGroup).Wait"} {
		if strings.Contains(string(buf), frame) {
			t.Fatalf("a goroutine is still parked in %s after Teardown gave up:\n%s", frame, buf)
		}
	}
}
