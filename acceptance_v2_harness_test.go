// Copyright © 2026 Meroxa, Inc.
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
	"sync/atomic"
	"testing"

	"github.com/conduitio/conduit-connector-protocol/pconnector"
)

// These tests cover how the v2 harness ends a source it crashes. They drive
// v2Src directly with a fake Run instead of a connector, so that the order in
// which Run sees the cancelled context and the closed stream is fixed by the
// test rather than by the scheduler (#406).

// fakeSrc returns a v2Src whose Run is run. With closeOnly, the in-memory
// stream ignores the cancellation, so the only thing that can end a blocked
// Recv or Send is the harness closing the stream: that is the side of the race
// #406 lost.
func fakeSrc(t *testing.T, closeOnly bool, run func(context.Context, pconnector.SourceRunStream) error) *v2Src {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	streamCtx := ctx
	if closeOnly {
		streamCtx = context.WithoutCancel(ctx)
	}
	s := &v2Src{
		t:       t,
		stream:  v2SrcStream{newV2Stream[pconnector.SourceRunRequest, pconnector.SourceRunResponse](streamCtx, 1, 1)},
		cancel:  cancel,
		ctx:     context.Background(),
		runDone: make(chan error, 1),
	}
	go func() { s.runDone <- run(ctx, s.stream) }()
	return s
}

// TestAcceptanceV2_CrashExpectsItsOwnStreamClose is the regression test for
// #406: a source whose Run returns a wrapped end of stream because the harness
// closed the stream during crash must not be reported as failing.
func TestAcceptanceV2_CrashExpectsItsOwnStreamClose(t *testing.T) {
	// The fake does what the SDK's runRead does when its Send loses the race
	// against the closed stream.
	s := fakeSrc(t, true, func(_ context.Context, stream pconnector.SourceRunStream) error {
		_, err := stream.Server().Recv()
		return fmt.Errorf("read stream error: %w", err)
	})
	if err := s.kill(); err != nil {
		t.Fatalf("crash reported the harness's own stream close as a source failure: %v", err)
	}
}

// TestAcceptanceV2_CrashCancelsBeforeClosing checks the order: Run must be
// able to see the cancellation before the closed stream, like a process that
// dies, so the end it reports is the cancellation.
func TestAcceptanceV2_CrashCancelsBeforeClosing(t *testing.T) {
	s := fakeSrc(t, false, func(ctx context.Context, _ pconnector.SourceRunStream) error {
		<-ctx.Done()
		return ctx.Err()
	})
	var closedFirst atomic.Bool
	cancel := s.cancel
	s.cancel = func() {
		select {
		case <-s.stream.stop:
			closedFirst.Store(true)
		default:
		}
		cancel()
	}
	if err := s.kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if closedFirst.Load() {
		t.Fatal("crash closed the stream before it cancelled the context of Run")
	}
}

// TestAcceptanceV2_CrashReportsRealErrors makes sure the fix for #406 did not
// make crash blind: an error the harness did not cause is still reported,
// including an io.EOF that comes from the connector instead of the stream.
func TestAcceptanceV2_CrashReportsRealErrors(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{name: "nil", err: nil},
		{name: "context canceled", err: fmt.Errorf("read plugin error: %w", context.Canceled)},
		{name: "context canceled as text", err: errors.New("ack stream error: rpc error: context canceled")},
		{name: "stream closed by the harness", err: fmt.Errorf("read stream error: %w", errV2StreamClosed)},
		{name: "connector returned io.EOF", err: fmt.Errorf("read plugin error: %w", io.EOF), wantErr: true},
		{name: "connector failed", err: errors.New("read plugin error: boom"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := fakeSrc(t, false, func(ctx context.Context, _ pconnector.SourceRunStream) error {
				<-ctx.Done()
				return tc.err
			})
			err := s.kill()
			if tc.wantErr && err == nil {
				t.Fatalf("crash did not report %q", tc.err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("crash reported an expected end: %v", err)
			}
		})
	}
}

// TestAcceptanceV2_StreamCloseIsDistinguishable checks that the plugin side of
// the in-memory stream reports a close as errV2StreamClosed, which still
// matches io.EOF: the SDK adapters treat io.EOF as the normal end of a stream.
func TestAcceptanceV2_StreamCloseIsDistinguishable(t *testing.T) {
	st := v2SrcStream{newV2Stream[pconnector.SourceRunRequest, pconnector.SourceRunResponse](context.Background(), 0, 0)}
	st.Close(context.Canceled)

	_, recvErr := st.Server().Recv()
	sendErr := st.Server().Send(pconnector.SourceRunResponse{})
	for name, err := range map[string]error{"Recv": recvErr, "Send": sendErr} {
		if !errors.Is(err, errV2StreamClosed) {
			t.Errorf("%s after Close: got %v, want errV2StreamClosed", name, err)
		}
		if !errors.Is(err, io.EOF) {
			t.Errorf("%s after Close: %v does not match io.EOF", name, err)
		}
	}
	if _, err := st.Client().Recv(); !errors.Is(err, context.Canceled) {
		t.Errorf("host Recv after Close: got %v, want the close reason", err)
	}
}
