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
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/conduitio/conduit-connector-sdk/internal"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

const testDeadline = 5 * time.Second

// scriptSource is a Source whose reads are scripted by the test.
type scriptSource struct {
	UnimplementedSource
	cfg   *scriptSourceConfig
	readN func(ctx context.Context, n int) ([]opencdc.Record, error)
	read  func(ctx context.Context) (opencdc.Record, error)
}

type scriptSourceConfig struct {
	UnimplementedSourceConfig
	SourceWithBatch
}

// Config returns the same pointer every time, as a real connector does: the
// middleware and Configure must see one config.
func (s *scriptSource) Config() SourceConfig {
	if s.cfg == nil {
		s.cfg = &scriptSourceConfig{}
	}
	return s.cfg
}

func (*scriptSource) Open(context.Context, opencdc.Position) error { return nil }

func (s *scriptSource) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if s.readN == nil {
		return nil, ErrUnimplemented
	}
	return s.readN(ctx, n)
}

func (s *scriptSource) Read(ctx context.Context) (opencdc.Record, error) {
	if s.read == nil {
		return opencdc.Record{}, ErrUnimplemented
	}
	return s.read(ctx)
}

func mkRecs(from, to int) []opencdc.Record {
	recs := make([]opencdc.Record, 0, to-from)
	for i := from; i < to; i++ {
		recs = append(recs, opencdc.Record{Position: opencdc.Position(fmt.Sprintf("p%04d", i))})
	}
	return recs
}

func batchPositions(recs []opencdc.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Position)
	}
	return out
}

func wantPositions(from, to int) []string { return batchPositions(mkRecs(from, to)) }

// openBatch opens src behind the batch middleware with already-resolved
// settings. The returned cancel stops the reader goroutines, as Stop does.
func openBatch(t *testing.T, src Source, st batchSettings) (*sourceWithBatch, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mw := (&SourceWithBatch{resolved: &st}).Wrap(src).(*sourceWithBatch)
	if err := mw.Open(ctx, nil); err != nil {
		t.Fatal(err)
	}
	return mw, cancel
}

// withDeadline fails the test if fn does not return in time. A collector that
// spins or blocks forever fails here instead of hanging the suite.
func withDeadline(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(testDeadline):
		t.Fatalf("%s: did not finish within %s", name, testDeadline)
	}
}

// -- resolveBatch ---------------------------------------------------------

func TestResolveBatch(t *testing.T) {
	const ms10 = 10 * time.Millisecond
	tests := []struct {
		name     string
		size     int
		delay    time.Duration
		in       batchInputs
		want     batchSettings
		wantCode string
	}{
		{"defaults are adaptive drain", 100, 0, batchInputs{}, batchSettings{size: 100, mode: batchModeAdaptive}, ""},
		{"opt-out", 0, 0, batchInputs{sizeSet: true, delaySet: true}, batchSettings{mode: batchModePassthrough}, ""},
		{"pinned 0/0 (connector default)", 0, 0, batchInputs{}, batchSettings{mode: batchModePassthrough}, ""},
		{"window", 100, ms10, batchInputs{sizeSet: true, delaySet: true}, batchSettings{size: 100, delay: ms10, mode: batchModeWindow}, ""},
		{"delay only, defaulted size", 100, ms10, batchInputs{delaySet: true}, batchSettings{size: 100, delay: ms10, mode: batchModeWindow}, ""},
		{"explicit size 0, delay>0", 0, ms10, batchInputs{sizeSet: true, delaySet: true}, batchSettings{size: defaultMaxBatchSize, delay: ms10, mode: batchModeWindow}, CodeBatchSizeDefaulted},
		{"pinned size 0, user delay", 0, ms10, batchInputs{delaySet: true}, batchSettings{size: defaultMaxBatchSize, delay: ms10, mode: batchModeWindow}, CodeBatchSizeDefaulted},
		{"explicit size, delay unset", 5, 0, batchInputs{sizeSet: true}, batchSettings{size: 5, mode: batchModeUntilFull}, CodeBatchDrainSemanticsPending},
		{"explicit size equal to default", 100, 0, batchInputs{sizeSet: true}, batchSettings{size: 100, mode: batchModeUntilFull}, CodeBatchDrainSemanticsPending},
		{"explicit size and delay 0", 5, 0, batchInputs{sizeSet: true, delaySet: true}, batchSettings{size: 5, mode: batchModeUntilFull}, CodeBatchDrainSemanticsPending},
		{"explicit delay 0 only (adaptive default)", 100, 0, batchInputs{delaySet: true}, batchSettings{size: 100, mode: batchModeAdaptive}, ""},
		{"window default: delay 0 only", 100, 0, batchInputs{delaySet: true, defaultDelay: ms10}, batchSettings{mode: batchModePassthrough}, CodeBatchUnboundedWait},
		{"window default: size and delay 0", 5, 0, batchInputs{sizeSet: true, delaySet: true, defaultDelay: ms10}, batchSettings{size: 5, mode: batchModeUntilFull}, CodeBatchUnboundedWait},
		{"window default: nothing set", 100, ms10, batchInputs{defaultDelay: ms10}, batchSettings{size: 100, delay: ms10, mode: batchModeWindow}, ""},
		{"negative delay is 0", 100, -time.Second, batchInputs{}, batchSettings{size: 100, mode: batchModeAdaptive}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got, ws := resolveBatch(tc.size, tc.delay, tc.in)
			is.Equal(got, tc.want)
			if tc.wantCode == "" {
				is.Equal(len(ws), 0)
				return
			}
			is.Equal(len(ws), 1)
			is.Equal(ws[0].code, tc.wantCode)
			is.True(ws[0].path != "" && ws[0].fix != "" && ws[0].msg != "")
		})
	}
}

// -- Configure: explicit versus defaulted ----------------------------------

func batchParams(size, delay string) config.Parameters {
	return config.Parameters{
		"sdk.batch.size":  {Type: config.ParameterTypeInt, Default: size},
		"sdk.batch.delay": {Type: config.ParameterTypeDuration, Default: delay},
	}
}

type logLine struct {
	Level      string `json:"level"`
	Code       string `json:"code"`
	ConfigPath string `json:"config_path"`
	Fix        string `json:"fix"`
}

// parseBatchConfig runs Configure's parsing and returns the resolved settings
// and the logged lines that carry a code.
func parseBatchConfig(t *testing.T, cfg config.Config, params config.Parameters) (batchSettings, []logLine) {
	t.Helper()
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())

	target := (&scriptSource{}).Config()
	if err := Util.ParseConfig(ctx, cfg, target, params); err != nil {
		t.Fatal(err)
	}
	sw := findSourceWithBatch(target)
	if sw == nil || sw.resolved == nil {
		t.Fatal("batch settings were not resolved by ParseConfig")
	}

	var coded []logLine
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if l == "" {
			continue
		}
		var ll logLine
		if err := json.Unmarshal([]byte(l), &ll); err != nil {
			t.Fatalf("bad log line %q: %v", l, err)
		}
		if ll.Code != "" {
			coded = append(coded, ll)
		}
	}
	return *sw.resolved, coded
}

func TestParseConfig_BatchExplicitVersusDefaulted(t *testing.T) {
	const ms10 = 10 * time.Millisecond
	tests := []struct {
		name     string
		cfg      config.Config
		specSize string
		want     batchSettings
		wantCode string
	}{
		{"nothing set", config.Config{}, "100", batchSettings{size: 100, mode: batchModeAdaptive}, ""},
		{"empty values count as unset", config.Config{"sdk.batch.size": " ", "sdk.batch.delay": ""}, "100", batchSettings{size: 100, mode: batchModeAdaptive}, ""},
		// The point of detecting this before ApplyDefaults: 100 is also the
		// default, and an explicit 100 must still be an explicit choice.
		{"explicit sdk.batch.size equal to the default is explicit", config.Config{"sdk.batch.size": "100"}, "100", batchSettings{size: 100, mode: batchModeUntilFull}, CodeBatchDrainSemanticsPending},
		{"explicit size 5", config.Config{"sdk.batch.size": "5"}, "100", batchSettings{size: 5, mode: batchModeUntilFull}, CodeBatchDrainSemanticsPending},
		{"explicit size with whitespace in key and value", config.Config{" sdk.batch.size ": " 100 "}, "100", batchSettings{size: 100, mode: batchModeUntilFull}, CodeBatchDrainSemanticsPending},
		{"explicit delay only", config.Config{"sdk.batch.delay": "10ms"}, "100", batchSettings{size: 100, delay: ms10, mode: batchModeWindow}, ""},
		{"explicit delay 0 only", config.Config{"sdk.batch.delay": "0"}, "100", batchSettings{size: 100, mode: batchModeAdaptive}, ""},
		{"size 0 with delay", config.Config{"sdk.batch.size": "0", "sdk.batch.delay": "10ms"}, "100", batchSettings{size: defaultMaxBatchSize, delay: ms10, mode: batchModeWindow}, CodeBatchSizeDefaulted},
		{"opt-out", config.Config{"sdk.batch.size": "0", "sdk.batch.delay": "0"}, "100", batchSettings{mode: batchModePassthrough}, ""},
		{"connector pinned 0/0", config.Config{}, "0", batchSettings{mode: batchModePassthrough}, ""},
		{"connector pinned 0/0, user sets size", config.Config{"sdk.batch.size": "50"}, "0", batchSettings{size: 50, mode: batchModeUntilFull}, CodeBatchDrainSemanticsPending},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got, coded := parseBatchConfig(t, tc.cfg, batchParams(tc.specSize, "0"))
			is.Equal(got, tc.want)
			if tc.wantCode == "" {
				is.Equal(len(coded), 0)
				return
			}
			is.Equal(len(coded), 1)
			is.Equal(coded[0].Level, "warn")
			is.Equal(coded[0].Code, tc.wantCode)
			is.True(coded[0].ConfigPath != "")
			is.True(coded[0].Fix != "")
		})
	}
}

func TestParseConfig_BatchFixedWindowSelectedByEnv(t *testing.T) {
	const ms10 = 10 * time.Millisecond
	t.Setenv(EnvBatchDefaultDelay, "10ms")

	t.Run("nothing set is the fixed window", func(t *testing.T) {
		is := is.New(t)
		got, coded := parseBatchConfig(t, config.Config{}, batchParams("100", "0"))
		is.Equal(got, batchSettings{size: 100, delay: ms10, mode: batchModeWindow})
		is.Equal(len(coded), 0)
	})
	t.Run("user delay wins over the env", func(t *testing.T) {
		is := is.New(t)
		got, _ := parseBatchConfig(t, config.Config{"sdk.batch.delay": "3ms"}, batchParams("100", "0"))
		is.Equal(got, batchSettings{size: 100, delay: 3 * time.Millisecond, mode: batchModeWindow})
	})
	t.Run("delay 0 alone would wait forever, stays unbatched", func(t *testing.T) {
		is := is.New(t)
		got, coded := parseBatchConfig(t, config.Config{"sdk.batch.delay": "0"}, batchParams("100", "0"))
		is.Equal(got, batchSettings{mode: batchModePassthrough})
		is.Equal(len(coded), 1)
		is.Equal(coded[0].Code, CodeBatchUnboundedWait)
	})
	t.Run("explicit size and delay 0 is unbounded wait", func(t *testing.T) {
		is := is.New(t)
		got, coded := parseBatchConfig(t, config.Config{"sdk.batch.size": "5", "sdk.batch.delay": "0"}, batchParams("100", "0"))
		is.Equal(got, batchSettings{size: 5, mode: batchModeUntilFull})
		is.Equal(len(coded), 1)
		is.Equal(coded[0].Code, CodeBatchUnboundedWait)
	})
	t.Run("explicit size alone gets the window", func(t *testing.T) {
		is := is.New(t)
		got, coded := parseBatchConfig(t, config.Config{"sdk.batch.size": "5"}, batchParams("100", "0"))
		is.Equal(got, batchSettings{size: 5, delay: ms10, mode: batchModeWindow})
		is.Equal(len(coded), 0)
	})
	t.Run("pinned 0/0 connectors ignore the env", func(t *testing.T) {
		is := is.New(t)
		got, coded := parseBatchConfig(t, config.Config{}, batchParams("0", "0"))
		is.Equal(got, batchSettings{mode: batchModePassthrough})
		is.Equal(len(coded), 0)
	})
	t.Run("invalid env is ignored", func(t *testing.T) {
		is := is.New(t)
		t.Setenv(EnvBatchDefaultDelay, "soon")
		got, _ := parseBatchConfig(t, config.Config{}, batchParams("100", "0"))
		is.Equal(got, batchSettings{size: 100, mode: batchModeAdaptive})
	})
}

func TestParseConfig_BatchSpecDefaultWindow(t *testing.T) {
	// A spec that carries the fixed-window default (100/10ms), as generated
	// when the tags are switched, behaves like the env override.
	is := is.New(t)
	got, coded := parseBatchConfig(t, config.Config{"sdk.batch.delay": "0"}, batchParams("100", "10ms"))
	is.Equal(got, batchSettings{mode: batchModePassthrough})
	is.Equal(len(coded), 1)
	is.Equal(coded[0].Code, CodeBatchUnboundedWait)
}

func TestCodeFromMessage(t *testing.T) {
	tests := []struct {
		msg  string
		want string
		ok   bool
	}{
		{CodeBatchUnboundedWait + ": set a delay", CodeBatchUnboundedWait, true},
		{"failed to parse configuration: config invalid: " + CodeBatchSizeDefaulted + ": fix it", CodeBatchSizeDefaulted, true},
		{"x (" + CodeBatchDrainSemanticsPending + ")", CodeBatchDrainSemanticsPending, true},
		{"ends the sentence with " + CodeBatchUnboundedWait + ".", CodeBatchUnboundedWait, true},
		{CodeBatchUnboundedWait, CodeBatchUnboundedWait, true},
		{"x" + CodeBatchUnboundedWait + ": no", "", false},
		{CodeBatchUnboundedWait + "_more: no", "", false},
		{CodeBatchUnboundedWait + ".more: no", "", false},
		{"sdk.batch.size is invalid", "", false},
		{"", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.msg, func(t *testing.T) {
			is := is.New(t)
			got, ok := CodeFromMessage(tc.msg)
			is.Equal(ok, tc.ok)
			is.Equal(got, tc.want)
		})
	}
}

// -- collector -------------------------------------------------------------

func adaptive(size int) batchSettings {
	return batchSettings{size: size, mode: batchModeAdaptive}
}

// readSource serves records from a channel on the Read path.
func readSource(ch chan opencdc.Record) *scriptSource {
	return &scriptSource{read: func(ctx context.Context) (opencdc.Record, error) {
		select {
		case r := <-ch:
			return r, nil
		case <-ctx.Done():
			return opencdc.Record{}, ctx.Err()
		}
	}}
}

func TestSourceWithBatch_Adaptive_FullQueueYieldsSize(t *testing.T) {
	is := is.New(t)
	ch := make(chan opencdc.Record, 1000)
	for _, r := range mkRecs(0, 250) {
		ch <- r
	}
	s, _ := openBatch(t, readSource(ch), adaptive(100))

	// Wait until the read-ahead is full, i.e. the engine is the bottleneck.
	withDeadline(t, "read-ahead fill", func() {
		for len(s.readCh) < 100 {
			time.Sleep(time.Millisecond)
		}
	})

	var got []opencdc.Record
	withDeadline(t, "collect", func() {
		for len(got) < 250 {
			b, err := s.ReadN(context.Background(), 1)
			is.NoErr(err)
			is.True(len(b) >= 1 && len(b) <= 100)
			got = append(got, b...)
			if len(got) == len(b) {
				is.Equal(len(b), 100) // first batch from a full queue is a full batch
			}
		}
	})
	is.Equal(batchPositions(got), wantPositions(0, 250))
}

func TestSourceWithBatch_Adaptive_EmptyQueueYieldsOneWithoutTimer(t *testing.T) {
	is := is.New(t)
	ch := make(chan opencdc.Record, 1)
	s, _ := openBatch(t, readSource(ch), adaptive(100))

	for i := range 3 {
		ch <- mkRecs(i, i+1)[0]
		var b []opencdc.Record
		withDeadline(t, "sparse collect", func() {
			var err error
			b, err = s.ReadN(context.Background(), 1)
			is.NoErr(err)
		})
		is.Equal(batchPositions(b), wantPositions(i, i+1)) // one record, returned at once
	}
	is.Equal(s.delay, time.Duration(0))
}

func TestSourceWithBatch_Adaptive_ReadNConnector(t *testing.T) {
	is := is.New(t)
	var calls atomic.Int32
	release := make(chan struct{})
	src := &scriptSource{readN: func(ctx context.Context, n int) ([]opencdc.Record, error) {
		is.Equal(n, 100) // the SDK asks for the batch size, not 1
		switch calls.Add(1) {
		case 1:
			return mkRecs(0, 30), nil
		case 2:
			return mkRecs(30, 40), nil
		default:
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, ctx.Err()
		}
	}}
	s, _ := openBatch(t, src, adaptive(100))

	// Let both results queue up: result 1 in the collector's hands is
	// not possible before we ask, so we expect 30 then (queued) 10 or 40 in one.
	var got []string
	withDeadline(t, "collect", func() {
		for len(got) < 40 {
			b, err := s.ReadN(context.Background(), 1)
			is.NoErr(err)
			is.True(len(b) >= 1)
			got = append(got, batchPositions(b)...)
		}
	})
	is.Equal(got, wantPositions(0, 40))
}

func TestSourceWithBatch_HardCapWithCarryForConnectorIgnoringN(t *testing.T) {
	for _, st := range []batchSettings{
		adaptive(100),
		{size: 100, delay: time.Hour, mode: batchModeWindow},
		{size: 100, mode: batchModeUntilFull},
	} {
		t.Run(st.mode.String(), func(t *testing.T) {
			is := is.New(t)
			var calls atomic.Int32
			src := &scriptSource{readN: func(ctx context.Context, _ int) ([]opencdc.Record, error) {
				if calls.Add(1) == 1 {
					return mkRecs(0, 250), nil // ignores n
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			s, _ := openBatch(t, src, st)

			withDeadline(t, "collect", func() {
				b1, err := s.ReadN(context.Background(), 1)
				is.NoErr(err)
				is.Equal(batchPositions(b1), wantPositions(0, 100))
				b2, err := s.ReadN(context.Background(), 1)
				is.NoErr(err)
				is.Equal(batchPositions(b2), wantPositions(100, 200))
			})
			// 50 carried records are all that is left: the third call returns
			// them without waiting for the connector (adaptive), or tops up and
			// is bounded by the timer/ctx (other modes), so use a cancelable ctx.
			if st.mode == batchModeAdaptive {
				withDeadline(t, "carry", func() {
					b3, err := s.ReadN(context.Background(), 1)
					is.NoErr(err)
					is.Equal(batchPositions(b3), wantPositions(200, 250))
				})
			}
		})
	}
}

func TestSourceWithBatch_FlushThenFail_ReadN(t *testing.T) {
	wantErr := errors.New("poison record")

	t.Run("error after k less than size records delivers k, then the error", func(t *testing.T) {
		is := is.New(t)
		var calls atomic.Int32
		src := &scriptSource{readN: func(context.Context, int) ([]opencdc.Record, error) {
			if calls.Add(1) == 1 {
				return mkRecs(0, 7), nil
			}
			return nil, wantErr
		}}
		s, _ := openBatch(t, src, batchSettings{size: 100, delay: time.Hour, mode: batchModeWindow})

		withDeadline(t, "collect", func() {
			b, err := s.ReadN(context.Background(), 1)
			is.NoErr(err)
			is.Equal(batchPositions(b), wantPositions(0, 7))
			for range 3 { // sticky
				b, err = s.ReadN(context.Background(), 1)
				is.True(errors.Is(err, wantErr))
				is.Equal(len(b), 0)
			}
		})
	})

	t.Run("carry larger than size comes out in order, then the stashed error", func(t *testing.T) {
		is := is.New(t)
		var calls atomic.Int32
		src := &scriptSource{readN: func(context.Context, int) ([]opencdc.Record, error) {
			if calls.Add(1) == 1 {
				return mkRecs(0, 250), nil
			}
			return nil, wantErr
		}}
		s, _ := openBatch(t, src, batchSettings{size: 100, delay: time.Hour, mode: batchModeWindow})

		withDeadline(t, "collect", func() {
			var got []string
			for {
				b, err := s.ReadN(context.Background(), 1)
				if err != nil {
					is.True(errors.Is(err, wantErr))
					is.Equal(len(b), 0)
					break
				}
				is.True(len(b) > 0 && len(b) <= 100) // never empty with a nil error
				got = append(got, batchPositions(b)...)
			}
			is.Equal(got, wantPositions(0, 250))
			_, err := s.ReadN(context.Background(), 1)
			is.True(errors.Is(err, wantErr))
		})
	})
}

func TestSourceWithBatch_FlushThenFail_Read(t *testing.T) {
	is := is.New(t)
	wantErr := errors.New("poison record")
	var n atomic.Int32
	src := &scriptSource{read: func(context.Context) (opencdc.Record, error) {
		i := int(n.Add(1)) - 1
		if i < 3 {
			return mkRecs(i, i+1)[0], nil
		}
		return opencdc.Record{}, wantErr
	}}
	s, _ := openBatch(t, src, adaptive(100))

	withDeadline(t, "collect", func() {
		var got []string
		for {
			b, err := s.ReadN(context.Background(), 1)
			if err != nil {
				is.True(errors.Is(err, wantErr))
				break
			}
			is.True(len(b) > 0)
			got = append(got, batchPositions(b)...)
		}
		is.Equal(got, wantPositions(0, 3))
		for range 3 {
			b, err := s.ReadN(context.Background(), 1)
			is.True(errors.Is(err, wantErr))
			is.Equal(len(b), 0)
		}
	})
}

func TestSourceWithBatch_ReadUnimplementedFallsBack(t *testing.T) {
	is := is.New(t)
	ch := make(chan opencdc.Record, 10)
	for _, r := range mkRecs(0, 5) {
		ch <- r
	}
	s, _ := openBatch(t, readSource(ch), batchSettings{size: 5, delay: time.Hour, mode: batchModeWindow})
	withDeadline(t, "collect", func() {
		b, err := s.ReadN(context.Background(), 1)
		is.NoErr(err)
		is.Equal(batchPositions(b), wantPositions(0, 5))
	})
	is.True(s.useRead)
}

func TestSourceWithBatch_CancellationDiscardsPartialBatch(t *testing.T) {
	for _, st := range []batchSettings{
		{size: 100, delay: time.Hour, mode: batchModeWindow},
		{size: 100, mode: batchModeUntilFull},
	} {
		t.Run(st.mode.String(), func(t *testing.T) {
			is := is.New(t)
			ch := make(chan opencdc.Record, 10)
			for _, r := range mkRecs(0, 3) {
				ch <- r
			}
			s, _ := openBatch(t, readSource(ch), st)

			ctx, cancel := context.WithCancel(context.Background())
			type result struct {
				recs []opencdc.Record
				err  error
			}
			res := make(chan result, 1)
			go func() {
				b, err := s.ReadN(ctx, 1)
				res <- result{b, err}
			}()

			// the 3 records are collected, the batch is neither full nor expired
			withDeadline(t, "records read", func() {
				for len(s.readCh) > 0 {
					time.Sleep(time.Millisecond)
				}
			})
			time.Sleep(20 * time.Millisecond)
			cancel()

			select {
			case r := <-res:
				is.True(errors.Is(r.err, context.Canceled))
				is.Equal(len(r.recs), 0) // nothing collected is sent
			case <-time.After(testDeadline):
				t.Fatal("collect did not return after cancellation")
			}
			is.Equal(len(s.carry), 0)
		})
	}
}

func TestSourceWithBatch_ConnectorCancelErrorDiscardsPartialBatch(t *testing.T) {
	// A connector that reports the stop as an error after delivering records
	// is a cancellation, not a poison record: nothing is flushed.
	is := is.New(t)
	var calls atomic.Int32
	src := &scriptSource{readN: func(context.Context, int) ([]opencdc.Record, error) {
		if calls.Add(1) == 1 {
			return mkRecs(0, 3), nil
		}
		return nil, context.Canceled
	}}
	s, _ := openBatch(t, src, batchSettings{size: 100, delay: time.Hour, mode: batchModeWindow})
	withDeadline(t, "collect", func() {
		b, err := s.ReadN(context.Background(), 1)
		is.True(errors.Is(err, context.Canceled))
		is.Equal(len(b), 0)
	})
}

func TestSourceWithBatch_ClosedReaderNeverReturnsEmptyWithNilError(t *testing.T) {
	is := is.New(t)
	src := &scriptSource{readN: func(ctx context.Context, _ int) ([]opencdc.Record, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	s, cancelOpen := openBatch(t, src, adaptive(10))
	cancelOpen() // the reader goroutine exits and closes its channel

	for range 3 {
		withDeadline(t, "collect", func() {
			b, err := s.ReadN(context.Background(), 1)
			is.True(err != nil)
			is.Equal(len(b), 0)
		})
	}
}

func TestSourceWithBatch_Passthrough(t *testing.T) {
	is := is.New(t)
	var gotN []int
	src := &scriptSource{readN: func(_ context.Context, n int) ([]opencdc.Record, error) {
		gotN = append(gotN, n)
		return mkRecs(0, 1), nil
	}}
	s, _ := openBatch(t, src, batchSettings{mode: batchModePassthrough})

	is.True(s.readCh != nil)
	for range 3 {
		b, err := s.ReadN(context.Background(), 1)
		is.NoErr(err)
		is.Equal(len(b), 1)
	}
	// Exactly the caller's reads, with the caller's n: no read-ahead goroutine.
	is.Equal(gotN, []int{1, 1, 1})
}

func TestSourceWithBatch_ReaderGoroutinesExitWithFullChannel(t *testing.T) {
	// A reader that fails while its channel is full and nobody collects used to
	// block forever on the error send.
	t.Run("Read", func(t *testing.T) {
		is := is.New(t)
		var n atomic.Int32
		src := &scriptSource{read: func(context.Context) (opencdc.Record, error) {
			if n.Add(1) > 3 {
				return opencdc.Record{}, errors.New("boom")
			}
			return mkRecs(0, 1)[0], nil
		}}
		ctx, cancel := context.WithCancel(context.Background())
		mw := (&SourceWithBatch{resolved: &batchSettings{size: 1, mode: batchModeAdaptive}}).Wrap(src).(*sourceWithBatch)
		is.NoErr(mw.Open(ctx, nil))       // ReadN is unimplemented, so Open's reader falls back to Read
		time.Sleep(20 * time.Millisecond) // channel is full, runRead blocked sending
		cancel()
		withDeadline(t, "runRead exit", func() {
			for range mw.readCh {
			}
		})
	})
	t.Run("ReadN", func(t *testing.T) {
		is := is.New(t)
		var n atomic.Int32
		src := &scriptSource{readN: func(context.Context, int) ([]opencdc.Record, error) {
			if n.Add(1) > 1 {
				return nil, errors.New("boom")
			}
			return mkRecs(0, 1), nil
		}}
		ctx, cancel := context.WithCancel(context.Background())
		mw := (&SourceWithBatch{resolved: &batchSettings{size: 1, mode: batchModeAdaptive}}).Wrap(src).(*sourceWithBatch)
		is.NoErr(mw.Open(ctx, nil))
		time.Sleep(20 * time.Millisecond) // one result queued, the error send is blocked
		cancel()
		withDeadline(t, "runReadN exit", func() {
			for range mw.readNCh {
			}
		})
	})
}

// -- through the adapter -------------------------------------------------------

// runThroughAdapter configures src with cfg, opens and runs it behind the real
// plugin adapter, and returns the client side of the stream and Run's result.
func runThroughAdapter(t *testing.T, src *scriptSource, cfg config.Config) (*InMemorySourceRunStream, <-chan error, *sourcePluginAdapter) {
	t.Helper()
	ctx := context.Background()
	wrapped := SourceWithMiddleware(src)
	a := NewSourcePlugin(wrapped, pconnector.PluginConfig{}, batchParams("100", "0")).(*sourcePluginAdapter)
	a.state.Set(internal.StateInitial)

	if _, err := a.Configure(ctx, pconnector.SourceConfigureRequest{Config: cfg}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Open(ctx, pconnector.SourceOpenRequest{}); err != nil {
		t.Fatal(err)
	}
	stream := NewInMemorySourceRunStream(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(ctx, stream) }()
	t.Cleanup(func() { stream.Close(io.EOF) })
	return stream, runDone, a
}

func TestSourcePluginAdapter_Run_BatchedSourceFailsMidBatch(t *testing.T) {
	// Deadline test for a spinning runRead: after the connector fails, the
	// adapter must deliver the good records and then return the error. If the
	// collector returned an empty slice with a nil error, runRead would
	// `continue` forever, Run would never return and the deadline would fail.
	wantErr := errors.New("poison record")
	var calls atomic.Int32
	src := &scriptSource{readN: func(context.Context, int) ([]opencdc.Record, error) {
		if calls.Add(1) == 1 {
			return mkRecs(0, 250), nil // ignores n; hard cap and carry
		}
		return nil, wantErr
	}}
	stream, runDone, _ := runThroughAdapter(t, src, config.Config{"sdk.batch.size": "100", "sdk.batch.delay": "1h"})

	var got []string
	withDeadline(t, "recv", func() {
		for len(got) < 250 {
			resp, err := stream.Client().Recv()
			if err != nil {
				t.Errorf("recv: %v", err)
				return
			}
			if len(resp.Records) == 0 || len(resp.Records) > 100 {
				t.Errorf("batch of %d records", len(resp.Records))
			}
			got = append(got, batchPositions(resp.Records)...)
		}
	})
	if strings.Join(got, ",") != strings.Join(wantPositions(0, 250), ",") {
		t.Fatalf("delivered %d records, not the 250 in order", len(got))
	}

	select {
	case err := <-runDone:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Run returned %v, want %v", err, wantErr)
		}
	case <-time.After(testDeadline):
		t.Fatal("Run did not return the connector error: runRead is spinning")
	}
}

func TestSourcePluginAdapter_Stop_BatchedSourceDiscardsPartialBatch(t *testing.T) {
	is := is.New(t)
	ch := make(chan opencdc.Record, 10)
	for _, r := range mkRecs(0, 3) {
		ch <- r
	}
	src := readSource(ch)
	// a window that cannot expire during the test
	stream, runDone, a := runThroughAdapter(t, src, config.Config{"sdk.batch.size": "100", "sdk.batch.delay": "1h"})

	time.Sleep(50 * time.Millisecond) // the 3 records are in the partial batch
	var stopResp pconnector.SourceStopResponse
	var stopErr error
	withDeadline(t, "stop", func() {
		stopResp, stopErr = a.Stop(context.Background(), pconnector.SourceStopRequest{})
	})
	is.NoErr(stopErr)
	is.Equal(len(stopResp.LastPosition), 0) // nothing was sent, so nothing to wait for
	stream.Close(io.EOF)                    // the engine closes the stream after Stop

	select {
	case err := <-runDone:
		is.NoErr(err)
	case <-time.After(testDeadline):
		t.Fatal("Run did not return after Stop")
	}
}

func TestSourceWithBatch_ReadDoesNotRaceWithReader(t *testing.T) {
	// Acceptance harnesses call Read directly. With batching on, that must not
	// start a second reader against the connector: every record comes out once,
	// in order.
	is := is.New(t)
	var n atomic.Int32
	src := &scriptSource{read: func(ctx context.Context) (opencdc.Record, error) {
		i := int(n.Add(1)) - 1
		if i >= 50 {
			<-ctx.Done()
			return opencdc.Record{}, ctx.Err()
		}
		return mkRecs(i, i+1)[0], nil
	}}
	s, _ := openBatch(t, src, adaptive(8))

	withDeadline(t, "read", func() {
		var got []opencdc.Record
		for len(got) < 50 {
			rec, err := s.Read(context.Background())
			is.NoErr(err)
			got = append(got, rec)
		}
		is.Equal(batchPositions(got), wantPositions(0, 50))
	})
}
