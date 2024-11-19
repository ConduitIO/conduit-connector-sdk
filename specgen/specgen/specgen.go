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
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/paramgen/paramgen"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	sdk "github.com/conduitio/conduit-connector-sdk"
	v1 "github.com/conduitio/conduit-connector-sdk/specgen/specgen/model/v1"
	"gopkg.in/yaml.v3"
)

func ExtractSpecification(ctx context.Context, conn sdk.Connector) (pconnector.Specification, error) {
	var spec pconnector.Specification
	if conn.NewSpecification != nil {
		spec = conn.NewSpecification()
	}

	// Ignore source and destination parameters in the current specification,
	// we will generate them.
	spec.SourceParams = nil
	spec.DestinationParams = nil

	if conn.NewSource != nil {
		var err error
		spec.SourceParams, err = parseParameters(ctx, conn.NewSource().Config())
		if err != nil {
			return pconnector.Specification{}, fmt.Errorf("failed to parse source config parameters: %w", err)
		}
	}
	if conn.NewDestination != nil {
		var err error
		spec.DestinationParams, err = parseParameters(ctx, conn.NewDestination().Config())
		if err != nil {
			return pconnector.Specification{}, fmt.Errorf("failed to parse source config parameters: %w", err)
		}
	}

	return spec, nil
}

func SpecificationToYaml(spec pconnector.Specification) ([]byte, error) {
	return yamlMarshal(v1.Specification{}.FromConfig(spec))
}

// WriteAndCombine combines the user-provided, custom information from an existing YAML file
// with the connector spec.
// `path` is the path to the existing YAML file.
// `yamlBytes` contains the connector's YAML spec.
func WriteAndCombine(yamlBytes []byte, path string) error {
	// Read the existing YAML file.
	existingRaw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// If the file doesn't exist, just write the new YAML directly
			if err := os.WriteFile(path, yamlBytes, 0o600); err != nil {
				return fmt.Errorf("failed to write YAML to file: %w", err)
			}
			return nil
		}
		return fmt.Errorf("failed to read existing file: %w", err)
	}

	type specWithUnknowns struct {
		Version       string `yaml:"version"`
		Specification struct {
			v1.ConnectorSpecification `yaml:",inline"`
			UnknownFields             map[string]any `yaml:",inline"`
		} `yaml:"specification"`
		UnknownFields map[string]any `yaml:",inline"`
	}

	generatedYAML := specWithUnknowns{}
	err = yaml.Unmarshal(yamlBytes, &generatedYAML)
	if err != nil {
		// This shouldn't happen, since we produced the YAML in this program.
		return fmt.Errorf("failed to unmarshal generated specifications YAML: %w", err)
	}

	existingSpecs := specWithUnknowns{}
	// Unmarshal the existing YAML file and check for unknown fields.
	err = yaml.Unmarshal(existingRaw, &existingSpecs)
	if err != nil {
		// This shouldn't happen, since we produced the YAML in this program.
		return fmt.Errorf("failed to unmarshal existing specifications YAML: %w", err)
	}

	existingSpecs.Version = generatedYAML.Version
	existingSpecs.Specification.Source = generatedYAML.Specification.Source
	existingSpecs.Specification.Destination = generatedYAML.Specification.Destination
	if existingSpecs.Specification.Name == "" {
		existingSpecs.Specification.Name = generatedYAML.Specification.Name
	}
	if existingSpecs.Specification.Summary == "" {
		existingSpecs.Specification.Summary = generatedYAML.Specification.Summary
	}
	if existingSpecs.Specification.Description == "" {
		existingSpecs.Specification.Description = generatedYAML.Specification.Description
	}
	if existingSpecs.Specification.Version == "" {
		existingSpecs.Specification.Version = generatedYAML.Specification.Version
	}
	if existingSpecs.Specification.Author == "" {
		existingSpecs.Specification.Author = generatedYAML.Specification.Author
	}

	// Marshal the merged map back to YAML bytes
	mergedYAML, err := yamlMarshal(existingSpecs)
	if err != nil {
		return fmt.Errorf("failed to marshal merged YAML: %w", err)
	}

	// Write the merged YAML to the file
	if err := os.WriteFile(path, mergedYAML, 0o600); err != nil {
		return fmt.Errorf("failed to write merged YAML to file: %w", err)
	}

	return nil
}

func yamlMarshal(obj any) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	err := enc.Encode(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to yaml: %w", err)
	}
	return buf.Bytes(), nil
}

func parseParameters(ctx context.Context, cfg any) (config.Parameters, error) {
	pkg, typName := getFullyQualifiedName(cfg)
	path, err := packageToPath(ctx, "", pkg)
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
	traverseFields(cfg, func(path string, _ reflect.StructField, value reflect.Value) {
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
func packageToPath(ctx context.Context, dir, pkg string) (string, error) {
	out, err := Run(ctx, dir, "go", "list", "-f", "{{.Dir}}", pkg)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// Run runs the given command and returns the output.
func Run(ctx context.Context, dir string, command string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = dir

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("error running command %q (error message: %q): %w", cmd.String(), stderr.String(), err)
	}

	return stdout.Bytes(), nil
}
