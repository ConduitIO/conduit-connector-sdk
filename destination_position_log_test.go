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
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"go.uber.org/mock/gomock"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A position can embed record key values, so the adapter logs and reports a
// hash of it, never the position.
func TestDestinationPluginAdapter_PositionsAreNotLogged(t *testing.T) {
	const secret = "customer-SECRET-4711"
	pos := opencdc.Position(`{"last_read":"` + secret + `"}`)

	t.Run("write error", func(t *testing.T) {
		is := is.New(t)
		ctrl := gomock.NewController(t)
		dst := NewMockDestination(ctrl)
		dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)

		rec := opencdc.Record{Position: pos, Operation: opencdc.OperationCreate, Payload: opencdc.Change{After: opencdc.RawData("x")}}
		dst.EXPECT().Config().Return(nil)
		dst.EXPECT().Open(gomock.Any()).Return(nil)
		dst.EXPECT().Write(gomock.Any(), gomock.Any()).Return(0, io.ErrUnexpectedEOF)

		var logs lockedBuffer
		ctx := zerolog.New(&logs).WithContext(context.Background())
		stream := NewInMemoryDestinationRunStream(ctx)

		_, err := dstPlugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: config.Config{}})
		is.NoErr(err)
		_, err = dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
		is.NoErr(err)

		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			_ = dstPlugin.Run(ctx, stream)
		}()
		client := stream.Client()
		is.NoErr(client.Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{rec}}))
		_, err = client.Recv()
		is.NoErr(err)
		stream.Close(io.EOF)
		<-runDone

		out := logs.String()
		is.True(strings.Contains(out, "error writing record"))
		is.True(strings.Contains(out, positionHash(pos)))
		is.True(!strings.Contains(out, secret))
	})

	t.Run("last position not encountered on stop", func(t *testing.T) {
		is := is.New(t)
		ctrl := gomock.NewController(t)
		dst := NewMockDestination(ctrl)
		dstPlugin := NewDestinationPlugin(dst, pconnector.PluginConfig{}, nil).(*destinationPluginAdapter)
		dst.EXPECT().Config().Return(nil)
		dst.EXPECT().Open(gomock.Any()).Return(nil)

		prev := stopTimeout
		stopTimeout = 50 * time.Millisecond
		t.Cleanup(func() { stopTimeout = prev })

		var logs lockedBuffer
		ctx := zerolog.New(&logs).WithContext(context.Background())
		_, err := dstPlugin.Configure(ctx, pconnector.DestinationConfigureRequest{Config: config.Config{}})
		is.NoErr(err)
		_, err = dstPlugin.Open(ctx, pconnector.DestinationOpenRequest{})
		is.NoErr(err)

		_, _ = dstPlugin.Stop(ctx, pconnector.DestinationStopRequest{LastPosition: pos})

		out := logs.String()
		is.True(strings.Contains(out, "did not encounter expected last position"))
		is.True(!strings.Contains(out, secret))
	})
}
