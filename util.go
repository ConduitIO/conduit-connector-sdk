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
	"reflect"
	"strings"

	"github.com/conduitio/conduit-commons/config"
	"github.com/mitchellh/mapstructure"
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
	ParseConfig func(map[string]string, any) error
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

func breakUpConfig(c map[string]string) map[string]any {
	const sep = "."

	brokenUp := make(map[string]any)
	for k, v := range c {
		// break up based on dot and put in maps in case target struct is broken up
		tokens := strings.Split(k, sep)
		remain := k
		current := brokenUp
		for _, t := range tokens {
			current[remain] = v // we don't care if we overwrite a map here, the string has precedence
			if _, ok := current[t]; !ok {
				current[t] = map[string]any{}
			}
			var ok bool
			current, ok = current[t].(map[string]any)
			if !ok {
				break // this key is a string, leave it as it is
			}
			_, remain, _ = strings.Cut(remain, sep)
		}
	}
	return brokenUp
}

func parseConfig(raw map[string]string, config any) error {
	dConfig := &mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           &config,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			emptyStringToZeroValueHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
		TagName: "json",
		Squash:  true,
	}

	decoder, err := mapstructure.NewDecoder(dConfig)
	if err != nil {
		return err
	}
	err = decoder.Decode(breakUpConfig(raw))
	return err
}

func emptyStringToZeroValueHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data any,
	) (any, error) {
		if f.Kind() != reflect.String || data != "" {
			return data, nil
		}
		return reflect.New(t).Elem().Interface(), nil
	}
}
