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

package specgen

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"reflect"
	"strings"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/paramgen/paramgen"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/yaml.v3"
)

func ParseSpecification(conn sdk.Connector) (sdk.Specification, error) {
	var spec sdk.Specification
	if conn.NewSpecification != nil {
		spec = conn.NewSpecification()
	}

	// Ignore source and destination parameters in the current specification,
	// we will generate them.
	spec.SourceParams = nil
	spec.DestinationParams = nil

	if conn.NewSource != nil {
		var err error
		spec.SourceParams, err = parseParameters(conn.NewSource().Config())
		if err != nil {
			return sdk.Specification{}, fmt.Errorf("failed to parse source config parameters: %w", err)
		}
	}
	// if conn.NewDestination != nil {
	// 	var err error
	// 	spec.DestinationParams, err = parseParameters(conn.NewDestination().Config())
	// 	if err != nil {
	// 		return sdk.Specification{}, fmt.Errorf("failed to parse source config parameters: %w", err)
	// 	}
	// }

	return spec, nil
}

func SpecificationToYaml(spec sdk.Specification, combineWithPath string) ([]byte, error) {
	if combineWithPath != "" {
		// TODO combine with yaml from that path
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	err := enc.Encode(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to encode specification to yaml: %w", err)
	}
	return buf.Bytes(), nil
}

func parseParameters(cfg any) (config.Parameters, error) {
	pkg, typName := getFullyQualifiedName(cfg)
	path, err := packageToPath(pkg)
	if err != nil {
		return nil, fmt.Errorf("failed to get path for package %q: %w", pkg, err)
	}

	params, _, err := paramgen.ParseParameters(path, typName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse parameters: %w", err)
	}

	overwriteDefaults(params, cfg)
	return params, nil
}

// overwriteDefaults traverses the parameters, fetches the corresponding field
// from the cfg struct and updates the default value if that field is set to
// anything other than the zero value. It ignores fields that are not found.
func overwriteDefaults(params config.Parameters, cfg any) {
	traverseFields(cfg, func(path string, field reflect.StructField, value reflect.Value) {
		param, ok := params[path]
		if !ok {
			// This shouldn't happen if the parameters were extracted from the
			// same type. If it does, we ignore it.
			return
		}
		if !value.IsZero() {
			for value.Kind() == reflect.Ptr {
				value = value.Elem()
			}

			if value.Kind() == reflect.Slice {
				param.Default = formatSlice(value)
			} else {
				param.Default = fmt.Sprintf("%v", value.Interface())
			}
			params[path] = param
		}
	})
}

func formatSlice(slice reflect.Value) string {
	// Create a slice to hold string representations of elements
	strElements := make([]string, slice.Len())

	// Convert each element to a string
	for i := 0; i < slice.Len(); i++ {
		strElements[i] = fmt.Sprintf("%v", slice.Index(i).Interface())
	}

	// Join the elements with comma
	return strings.Join(strElements, ",")
}

// getFullyQualifiedName returns the fully qualified package name and type name
// for a value.
func getFullyQualifiedName(t any) (string, string) {
	typ := reflect.TypeOf(t)
	for typ.Kind() == reflect.Ptr || typ.Kind() == reflect.Interface {
		typ = typ.Elem()
	}
	return typ.PkgPath(), typ.Name()
}

// packageToPath takes a package import path and returns the path to the directory
// of that package.
func packageToPath(pkg string) (string, error) {
	cmd := exec.Command("go", "list", "-f", "{{.Dir}}", pkg)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("error piping stdout of go list command: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("error piping stderr of go list command: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("error starting go list command: %w", err)
	}
	path, err := io.ReadAll(stdout)
	if err != nil {
		return "", fmt.Errorf("error reading stdout of go list command: %w", err)
	}
	errMsg, err := io.ReadAll(stderr)
	if err != nil {
		return "", fmt.Errorf("error reading stderr of go list command: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return "", fmt.Errorf("error running command %q (error message: %q): %w", cmd.String(), errMsg, err)
	}
	return strings.TrimRight(string(path), "\n"), nil
}
