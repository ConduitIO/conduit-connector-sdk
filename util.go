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
	"fmt"

	"github.com/conduitio/conduit-commons/config"
)

// Util provides utilities for implementing connectors.
var Util = struct {
	// SourceUtil provides utility methods for implementing a source.
	Source SourceUtil
	// SourceUtil provides utility methods for implementing a destination.
	Destination DestinationUtil
	// ParseConfig provided to parse a config map into a struct
	// Under the hood, this function uses the library mitchellh/mapstructure, with the "mapstructure" tag renamed to "json",
	// so to rename a key, use the "json" tag and set a value directly. To embed structs, append ",squash" to your tag.
	// for more details and docs, check https://pkg.go.dev/github.com/mitchellh/mapstructure
	ParseConfig func(map[string]string, interface{}) error
}{
	ParseConfig: parseConfig,
}

func mergeParameters(p1 map[string]Parameter, p2 map[string]Parameter) map[string]Parameter {
	params := make(map[string]Parameter, len(p1)+len(p2))
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

func parseConfig(cfg map[string]string, v interface{}) error {
	return config.Config(cfg).DecodeInto(v)
}
