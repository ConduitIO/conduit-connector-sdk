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

package readmegen

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/conduitio/yaml/v3"
)

type Command struct {
	specificationsFile string
	readmeFile         string
	write              bool
}

func NewCommand(specificationsFile, readmeFile string, write bool) *Command {
	return &Command{
		specificationsFile: specificationsFile,
		readmeFile:         readmeFile,
		write:              write,
	}
}

func (cmd *Command) Execute(_ context.Context) error {
	specs, err := cmd.readSpecs(cmd.specificationsFile)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	var out io.Writer = os.Stdout
	if cmd.write {
		// Write to buffer and then flush to file
		out = buf
	}

	opts := GenerateOptions{
		Data:       specs,
		ReadmePath: cmd.readmeFile,
		Out:        out,
	}

	err = Generate(opts)
	if err != nil {
		return fmt.Errorf("failed to generate readme: %w", err)
	}

	if cmd.write {
		err = os.WriteFile(cmd.readmeFile, buf.Bytes(), 0o644)
		if err != nil {
			return fmt.Errorf("failed to write %s: %w", cmd.readmeFile, err)
		}
	}
	return nil
}

func (*Command) readSpecs(path string) (map[string]any, error) {
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
