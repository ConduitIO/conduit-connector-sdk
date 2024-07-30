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

	"github.com/conduitio/conduit-commons/config"
)

// Util provides utilities for implementing connectors.
var Util = struct {
	// SourceUtil provides utility methods for implementing a source.
	Source SourceUtil
	// SourceUtil provides utility methods for implementing a destination.
	Destination DestinationUtil

	// ParseConfig sanitizes the configuration, applies defaults, validates it and
	// copies the values into the target object. It combines the functionality
	// provided by github.com/conduitio/conduit-commons/config.Config into a single
	// convenient function. It is intended to be used in the Configure method of a
	// connector to parse the configuration map.
	//
	// The configuration parameters should be provided through NewSource().Parameters()
	// or NewDestination().Parameters() so that parameters from SDK middlewares are
	// included too.
	//
	// The function does the following:
	//   - Removes leading and trailing spaces from all keys and values in the
	//     configuration.
	//   - Applies the default values defined in the parameter specifications to the
	//     configuration.
	//   - Validates the configuration by checking for unrecognized parameters, type
	//     validations, and value validations.
	//   - Copies configuration values into the target object. The target object must
	//     be a pointer to a struct.
	ParseConfig func(ctx context.Context, cfg config.Config, target any, params config.Parameters) error
}{
	ParseConfig: parseConfig,
}

func mergeParameters(p1 config.Parameters, p2 config.Parameters) config.Parameters {
	params := make(config.Parameters, len(p1)+len(p2))
	for k, v := range p1 {
		params[k] = v
	}
	for k, v := range p2 {
		_, ok := params[k]
		if ok {
			panic(fmt.Errorf("parameter %q declared twice", k))
		}
		params[k] = v
	}
	return params
}

func parseConfig(
	ctx context.Context,
	cfg config.Config,
	target any,
	params config.Parameters,
) error {
	logger := Logger(ctx)

	logger.Debug().Msg("sanitizing configuration and applying defaults")
	c := cfg.Sanitize().ApplyDefaults(params)

	logger.Debug().Msg("validating configuration according to the specifications")
	err := c.Validate(params)
	if err != nil {
		return fmt.Errorf("config invalid: %w", err)
	}

	logger.Debug().Type("target", target).Msg("decoding configuration into the target object")
	//nolint:wrapcheck // error is already wrapped by DecodeInto
	return c.DecodeInto(target)
}
