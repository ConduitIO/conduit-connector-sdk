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
	"os"

	"github.com/rs/zerolog"
)

// Logger returns an instance of a logger that can be used for leveled and
// structured logging in a plugin. The logger will respect the log level
// configured in Conduit.
func Logger(ctx context.Context) *zerolog.Logger {
	return zerolog.Ctx(ctx)
}

// initStandaloneModeLogger will create a default context logger that can be
// used by the plugin in standalone mode. Should not be called in builtin mode.
func initStandaloneModeLogger(level zerolog.Level) {
	// adjust field names to have parity with hclog, go-plugin uses hclog to
	// parse log messages
	zerolog.LevelFieldName = "@level"
	zerolog.CallerFieldName = "@caller"
	zerolog.TimestampFieldName = "@timestamp"
	zerolog.MessageFieldName = "@message"

	logger := zerolog.New(os.Stderr)
	logger = logger.Level(level)

	zerolog.DefaultContextLogger = &logger
}
