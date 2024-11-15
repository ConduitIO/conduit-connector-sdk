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

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-connector-sdk/specgen/specgen"
)

func main() {
	ctx := context.Background()

	log.SetFlags(0)
	log.SetPrefix("specgen: ")

	// parse the command arguments
	args := parseFlags()

	specBytes, err := extractYAMLSpecification(ctx, args.path)
	if err != nil {
		log.Fatalf("error: failed to extract specification: %v", err)
	}

	err = specgen.WriteAndCombine(specBytes, args.output)
	if err != nil {
		log.Fatalf("error: failed to output file: %v", err)
	}
}

type Args struct {
	output string
	path   string
}

func parseFlags() Args {
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var (
		output = flags.String("output", "connector.yaml", "name of the output file")
		path   = flags.String("path", ".", "directory path to the package that contains the Connector variable")
	)

	// flags is set up to exit on error, we can safely ignore the error
	_ = flags.Parse(os.Args[1:])

	if len(flags.Args()) > 0 {
		log.Println("error: unrecognized argument")
		fmt.Println()
		flags.Usage()
		os.Exit(1)
	}

	var args Args
	args.output = lang.ValOrZero(output)
	args.path = lang.ValOrZero(path)

	return args
}

func extractYAMLSpecification(ctx context.Context, path string) ([]byte, error) {
	// Get the import path of the package.
	out, err := specgen.Run(ctx, path, "go", "list", "-m")
	if err != nil {
		return nil, err
	}
	importPath := strings.TrimSpace(string(out))

	program, err := writeProgram(importPath)
	if err != nil {
		return nil, err
	}

	return runInDir(ctx, program, path)
}

func writeProgram(importPath string) ([]byte, error) {
	var program bytes.Buffer
	data := reflectData{
		ImportPath: importPath,
	}
	if err := reflectProgram.Execute(&program, &data); err != nil {
		return nil, err
	}
	return program.Bytes(), nil
}

// runInDir writes the given program into the given dir, runs it there, and
// returns the stdout output.
func runInDir(ctx context.Context, program []byte, dir string) ([]byte, error) {
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
	var progBinary = "prog.bin"
	if runtime.GOOS == "windows" {
		// Windows won't execute a program unless it has a ".exe" suffix.
		progBinary += ".exe"
	}

	if err := os.WriteFile(filepath.Join(tmpDir, progSource), program, 0600); err != nil {
		return nil, err
	}

	// Build the program.
	buf := bytes.NewBuffer(nil)
	cmd := exec.Command("go", "build", "-o", progBinary, progSource)
	cmd.Dir = tmpDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = io.MultiWriter(os.Stderr, buf)
	if err := cmd.Run(); err != nil {
		sErr := buf.String()
		if strings.Contains(sErr, `cannot find package "."`) &&
			strings.Contains(sErr, "github.com/conduitio/conduit-connector-sdk") {
			fmt.Fprint(os.Stderr, "Please upgrade to the latest version of the connector SDK (go get github.com/conduitio/conduit-connector-sdk@latest)\n")
			return nil, err
		}
		return nil, err
	}

	return specgen.Run(ctx, dir, filepath.Join(tmpDir, progBinary))
}

type reflectData struct {
	ImportPath string
}

// This program uses reflection to traverse the connector configuration and
// prints the extracted specification to standard output.
var reflectProgram = template.Must(template.New("program").Parse(`
package main

import (
	"context"
	"log"
	"os"

	pkg_ {{ printf "%q" .ImportPath }}
	"github.com/conduitio/conduit-connector-sdk/specgen/specgen"
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
