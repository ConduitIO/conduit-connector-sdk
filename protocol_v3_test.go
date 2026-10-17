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
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-protocol/pconnector/client"
	"github.com/conduitio/conduit-connector-protocol/pconnector/server"
	v3 "github.com/conduitio/conduit-connector-protocol/pconnector/v3"
	clientv3 "github.com/conduitio/conduit-connector-protocol/pconnector/v3/client"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/matryer/is"
)

// The values below are exactly the ones the v1/v2 structpb wire format cannot
// carry: an integer beyond 2^53 (rounds to a float64), the maximum uint64
// (cannot be represented as a float64 at all), raw bytes (become a base64
// string) and a json.Number decimal (parsed through a float64). Protocol v3's
// opencdc.v2 wire format keeps them lossless; see ADR
// 20261015-lossless-structured-values-protocol-v3.md and conduit#3100.
const (
	twoTo53PlusOne = int64(9007199254740993)      // 2^53 + 1
	maxUint64      = uint64(18446744073709551615) // ^uint64(0), does not fit in a float64
)

var losslessBytes = []byte{0x00, 0x01, 0x02, 0xfe, 0xff}

func losslessStructuredData() opencdc.StructuredData {
	return opencdc.StructuredData{
		"int64":   twoTo53PlusOne,
		"uint64":  maxUint64,
		"bytes":   losslessBytes,
		"decimal": json.Number("12345678901234567890.12345678901234567890"),
	}
}

func losslessRecord() opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position("protocol-v3-position"),
		Operation: opencdc.OperationUpdate,
		Metadata:  opencdc.Metadata{"foo": "bar"},
		Key:       opencdc.RawData("key"),
		Payload: opencdc.Change{
			After: losslessStructuredData(),
		},
	}
}

func assertLosslessAfter(t *testing.T, after opencdc.Data) {
	t.Helper()
	is := is.New(t)

	sd, ok := after.(opencdc.StructuredData)
	is.True(ok) // after must still be structured data

	is.Equal(twoTo53PlusOne, sd["int64"]) // exact int64, not a rounded float64
	is.Equal(maxUint64, sd["uint64"])     // exact uint64
	gotBytes, ok := sd["bytes"].([]byte)  // []byte, not a base64 string
	is.True(ok)
	is.True(bytes.Equal(losslessBytes, gotBytes))
	is.Equal(json.Number("12345678901234567890.12345678901234567890"), sd["decimal"])
}

// losslessSource returns a single record carrying the lossless value kinds and
// then blocks until its context is cancelled. It exercises the full SDK source
// dispatch (NewSourcePlugin -> Configure -> Open -> Run).
type losslessSource struct {
	UnimplementedSource
	record opencdc.Record
	first  atomic.Bool
}

func (s *losslessSource) Config() SourceConfig { return UnimplementedSourceConfig{} }

func (s *losslessSource) Open(context.Context, opencdc.Position) error { return nil }

func (s *losslessSource) Read(ctx context.Context) (opencdc.Record, error) {
	if s.first.CompareAndSwap(false, true) {
		return s.record, nil
	}
	<-ctx.Done()
	return opencdc.Record{}, ctx.Err()
}

func (s *losslessSource) Ack(context.Context, opencdc.Position) error { return nil }

func (s *losslessSource) Teardown(context.Context) error { return nil }

// reattachPluginsOption is a client.Option that points go-plugin at the plugin
// set for the version the server already negotiated. Version negotiation is
// skipped when reattaching, so go-plugin does not populate ClientConfig.Plugins
// from VersionedPlugins[version] and Dispense would otherwise fail with
// "unknown plugin type". See https://github.com/hashicorp/go-plugin/issues/310.
type reattachPluginsOption struct {
	version int
}

func (o reattachPluginsOption) ApplyOption(cfg *plugin.ClientConfig) error {
	cfg.Plugins = cfg.VersionedPlugins[o.version]
	return nil
}

// startServer starts the SDK's plugin server (the same server.Serve entrypoint
// Serve uses) in go-plugin's test mode and returns the reattach config once the
// handshake has negotiated a protocol version. clientVersions is passed exactly
// as go-plugin's client would: the PLUGIN_PROTOCOL_VERSIONS env var. The
// returned cleanup function stops the server and waits for it to exit.
func startServer(t *testing.T, clientVersions string, src Source) (*plugin.ReattachConfig, func()) {
	t.Helper()
	t.Setenv("PLUGIN_PROTOCOL_VERSIONS", clientVersions)

	ctx, cancel := context.WithCancel(context.Background())
	reattachCh := make(chan *plugin.ReattachConfig, 1)
	closeCh := make(chan struct{})

	spec := Specification{Name: "protocol-v3-test", Version: "v0.0.0"}
	cfg := pconnector.PluginConfig{}

	go func() {
		_ = server.Serve(
			func() pconnector.SpecifierPlugin { return NewSpecifierPlugin(spec) },
			func() pconnector.SourcePlugin { return NewSourcePlugin(src, cfg, nil) },
			func() pconnector.DestinationPlugin { return NewDestinationPlugin(nil, cfg, nil) },
			server.WithDebug(ctx, reattachCh, closeCh),
		)
	}()

	select {
	case rc := <-reattachCh:
		return rc, func() {
			cancel()
			select {
			case <-closeCh:
			case <-time.After(10 * time.Second):
				t.Error("timed out waiting for the SDK plugin server to shut down")
			}
		}
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("timed out waiting for the SDK plugin server to negotiate a version")
		return nil, func() {}
	}
}

// TestServeNegotiatesProtocolV3 asserts the SDK server negotiates v3 with a v3
// client and still falls back to v2 for an old client that only offers {1, 2}.
// The version set itself lives in conduit-connector-protocol's server.Serve,
// which the SDK delegates to; this test pins that the SDK's entrypoint picks up
// the {1, 2, 3} set after the protocol bump.
func TestServeNegotiatesProtocolV3(t *testing.T) {
	tests := []struct {
		name           string
		clientVersions string
		want           int
	}{
		{
			name:           "v3 client selects v3",
			clientVersions: "3,2,1",
			want:           v3.Version,
		},
		{
			name:           "old v2 client falls back to v2",
			clientVersions: "2,1",
			want:           2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			rc, cleanup := startServer(t, tt.clientVersions, nil)
			defer cleanup()
			is.Equal(tt.want, rc.ProtocolVersion)
		})
	}
}

// TestProtocolV3LosslessRoundTrip runs a connector source through the SDK's
// plugin server against a v3 client and asserts the lossless value kinds
// survive the full gRPC round trip exactly. The record is produced by the SDK
// source dispatch, encoded to opencdc.v2 by the v3 server handler, transported
// over gRPC and decoded back by the v3 client handler.
func TestProtocolV3LosslessRoundTrip(t *testing.T) {
	is := is.New(t)

	src := &losslessSource{record: losslessRecord()}
	rc, stopServer := startServer(t, "3,2,1", src)
	t.Cleanup(stopServer)
	is.Equal(v3.Version, rc.ProtocolVersion)

	pluginClient, err := client.New(
		hclog.NewNullLogger(),
		"",
		client.WithReattachConfig(rc),
		reattachPluginsOption{version: rc.ProtocolVersion},
	)
	is.NoErr(err)
	t.Cleanup(pluginClient.Kill)

	rpcClient, err := pluginClient.Client()
	is.NoErr(err)
	t.Cleanup(func() { _ = rpcClient.Close() })

	raw, err := rpcClient.Dispense("source")
	is.NoErr(err)
	srcClient, ok := raw.(*clientv3.SourcePluginClient)
	is.True(ok)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	_, err = srcClient.Configure(ctx, pconnector.SourceConfigureRequest{Config: config.Config{}})
	is.NoErr(err)
	_, err = srcClient.Open(ctx, pconnector.SourceOpenRequest{})
	is.NoErr(err)

	stream := srcClient.NewStream()
	if err := srcClient.Run(ctx, stream); err != nil {
		t.Fatalf("source Run failed: %v", err)
	}

	resp, err := stream.Client().Recv()
	is.NoErr(err)
	is.Equal(1, len(resp.Records))
	assertLosslessAfter(t, resp.Records[0].Payload.After)
}
