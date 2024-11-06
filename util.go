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
	"log/slog"
	"strings"

	"github.com/Masterminds/semver/v3"
	v1 "github.com/conduitio/conduit-connector-sdk/specgen/specgen/model/v1"
	"github.com/conduitio/evolviconf"
	"github.com/conduitio/evolviconf/evolviyaml"
	"github.com/conduitio/yaml/v3"
	slogzerolog "github.com/samber/slog-zerolog/v2"

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
	ParseConfig func(ctx context.Context, cfg config.Config, target any) error
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
	// params config.Parameters,
) error {
	logger := Logger(ctx)

	logger.Debug().Msg("sanitizing configuration and applying defaults")
	c := cfg.Sanitize() // .ApplyDefaults(params)

	logger.Debug().Msg("validating configuration according to the specifications")
	// err := c.Validate(params)
	// if err != nil {
	// 	return fmt.Errorf("config invalid: %w", err)
	// }

	logger.Debug().Type("target", target).Msg("decoding configuration into the target object")
	//nolint:wrapcheck // error is already wrapped by DecodeInto
	return c.DecodeInto(target)
}

func YAMLSpecification(rawYaml string) func() Specification {
	specs, err := ParseYAMLSpecification(context.Background(), rawYaml)
	if err != nil {
		panic("failed to parse YAML specification: " + err.Error())
	}
	return func() Specification { return specs }
}

func ParseYAMLSpecification(ctx context.Context, rawYaml string) (Specification, error) {
	logger := Logger(ctx)

	logger.Debug().Str("yaml", rawYaml).Msg("parsing YAML specification")

	parser := evolviconf.NewParser[Specification, *yaml.Decoder](
		evolviyaml.NewParser[Specification, v1.Specification](
			must[*semver.Constraints](semver.NewConstraint("^1")),
			v1.Changelog,
		),
	)
	reader := strings.NewReader(rawYaml)

	spec, warnings, err := parser.Parse(ctx, reader)
	if err != nil {
		return Specification{}, fmt.Errorf("failed to parse YAML specification: %w", err)
	}
	if len(warnings) > 0 {
		slogLogger := slog.New(slogzerolog.Option{Logger: logger}.NewZerologHandler())
		warnings.Log(ctx, slogLogger)
	}

	switch len(spec) {
	case 0:
		logger.Debug().Msg("no specification found in YAML")
		return Specification{}, fmt.Errorf("no specification found in YAML")
	case 1:
		logger.Debug().Any("specification", spec[0]).Msg("specification successfully parsed")
		return spec[0], nil
	default:
		logger.Warn().Any("specification", spec[0]).Msg("multiple specifications found in YAML, returning the first one")
		return spec[0], nil
	}
}

func must[T any](out T, err error) T {
	if err != nil {
		panic(err)
	}
	return out
}
