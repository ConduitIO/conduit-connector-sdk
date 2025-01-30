// Copyright © 2025 Meroxa, Inc.
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
	"html/template"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

type Command struct {
	outputPath  string
	packagePath string
}

func NewCommand(outputPath, packagePath string) *Command {
	return &Command{
		outputPath:  outputPath,
		packagePath: packagePath,
	}
}

func (cmd *Command) Execute(ctx context.Context) error {
	specBytes, err := cmd.extractYAMLSpecification(ctx, cmd.packagePath)
	if err != nil {
		return fmt.Errorf("error: failed to extract specification: %w", err)
	}

	err = WriteAndCombine(specBytes, cmd.outputPath)
	if err != nil {
		return fmt.Errorf("error: failed to output file: %w", err)
	}

	return nil
}

// extractYAMLSpecification extracts the YAML specification from the connector
// found in `path`.
// `path` is the directory path to the package that contains the `Connector` variable.
func (cmd *Command) extractYAMLSpecification(ctx context.Context, path string) ([]byte, error) {
	// Get the import path of the package (that contains the connector code).
	// It's needed in extractYAMLProgram to get the `Connector` variable.
	out, err := Run(ctx, path, "go", "list", "-m")
	if err != nil {
		return nil, err
	}
	importPath := strings.TrimSpace(string(out))

	program, err := cmd.writeProgram(importPath)
	if err != nil {
		return nil, err
	}

	return cmd.runInDir(ctx, program, path)
}

// writeProgram writes the program that extracts the YAML specs
// from the connector that can be found at the connectorImportPath.
func (cmd *Command) writeProgram(connectorImportPath string) ([]byte, error) {
	var program bytes.Buffer
	data := reflectData{
		ImportPath: connectorImportPath,
	}
	if err := extractYAMLProgram.Execute(&program, &data); err != nil {
		return nil, err
	}
	return program.Bytes(), nil
}

// runInDir writes the given program into the given dir, runs it there, and
// returns the stdout output.
func (cmd *Command) runInDir(ctx context.Context, program []byte, dir string) ([]byte, error) {
	// We use MkdirTemp instead of CreateTemp so we can control the filename.
	tmpDir, err := os.MkdirTemp(dir, "specgen_")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			log.Printf("failed to remove temp directory: %s", err)
		}
	}()
	const progSource = "prog.go"
	progBinary := "prog.bin"
	if runtime.GOOS == "windows" {
		// Windows won't execute a program unless it has a ".exe" suffix.
		progBinary += ".exe"
	}

	if err := os.WriteFile(filepath.Join(tmpDir, progSource), program, 0o600); err != nil {
		return nil, err
	}

	// Build the program.
	buf := bytes.NewBuffer(nil)
	command := exec.Command("go", "build", "-o", progBinary, progSource)
	command.Dir = tmpDir
	command.Stdout = os.Stdout
	command.Stderr = io.MultiWriter(os.Stderr, buf)
	if err := command.Run(); err != nil {
		sErr := buf.String()
		if strings.Contains(sErr, `cannot find package "."`) &&
			strings.Contains(sErr, "github.com/conduitio/conduit-connector-sdk") {
			fmt.Fprint(os.Stderr, "Please upgrade to the latest version of the connector SDK (go get github.com/conduitio/conduit-connector-sdk@latest)\n")
			return nil, err
		}
		return nil, err
	}

	return Run(ctx, dir, filepath.Join(tmpDir, progBinary))
}

type reflectData struct {
	ImportPath string
}

// extractYAMLProgram extract the YAML specs from a connector and writes it
// to standard output. This program uses reflection to traverse the connector configuration.
var extractYAMLProgram = template.Must(template.New("program").Parse(`
package main

import (
	"context"
	"log"
	"os"

	pkg_ {{ printf "%q" .ImportPath }}
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/specgen"
)

func main() {
	spec, err := specgen.ExtractSpecification(context.Background(), pkg_.Connector)
	if err != nil {
		log.Fatalf("error: failed to extract specification: %v", err)
	}

	specYaml, err := specgen.SpecificationToYaml(spec)
	if err != nil {
		log.Fatalf("error: failed to convert specification to yaml: %v", err)
	}

	_, err = os.Stdout.Write(specYaml)
	if err != nil {
		log.Fatalf("error: failed to output yaml: %v", err)
	}
}
`))
