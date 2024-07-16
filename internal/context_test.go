// Copyright © 2024 Meroxa, Inc.
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

package internal

import (
	"context"
	"testing"

	"github.com/conduitio/conduit-connector-protocol/pconduit"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	"github.com/matryer/is"
)

func TestContextUtils_Enrich(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	cfg := pconnector.PluginConfig{
		Token:       "test-token",
		ConnectorID: "test-connector-id",
		LogLevel:    "trace",
	}

	got := Enrich(ctx, cfg)
	is.Equal(cfg.Token, pconduit.ConnectorTokenFromContext(got))
	is.Equal(cfg.ConnectorID, ConnectorIDFromContext(got))
	is.Equal(cfg.LogLevel, LogLevelFromContext(got))
}
