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
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/conduitio/conduit-connector-sdk/cmd/readmegen/util"
	"github.com/conduitio/yaml/v3"
)

var (
	specsPath  = flag.String("specs", "connector.yaml", "The path to the connector.yaml file")
	readmePath = flag.String("readme", "README.md", "The path to the readme file")
	write      = flag.Bool("w", false, "Overwrite readme file instead of printing to stdout")
)

func main() {
	if err := mainE(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func mainE() error {
	flag.Parse()

	specs, err := readSpecs(*specsPath)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	var out io.Writer = os.Stdout
	if *write {
		// Write to buffer and then flush to file
		out = buf
	}

	opts := util.GenerateOptions{
		Data:       specs,
		ReadmePath: *readmePath,
		Out:        out,
	}

	err = util.Generate(opts)
	if err != nil {
		return fmt.Errorf("failed to generate readme: %w", err)
	}

	if *write {
		err = os.WriteFile(*readmePath, buf.Bytes(), 0644)
		if err != nil {
			return fmt.Errorf("failed to write %s: %w", *readmePath, err)
		}
	}
	return nil
}

func readSpecs(path string) (map[string]any, error) {
	specsRaw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read specifications from %v: %w", path, err)
	}

	var data map[string]any
	err = yaml.Unmarshal(specsRaw, &data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal specifications: %w", err)
	}
	return data, nil
}
