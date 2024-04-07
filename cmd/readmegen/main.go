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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"

	"github.com/conduitio/conduit-connector-sdk/cmd/readmegen/util"
	"golang.org/x/mod/modfile"
)

var (
	pkg               = flag.String("package", "", "The full package import path for the connector. By default, readmegen will try to detect the package import path based on the go.mod file it finds in the directory or any parent directory.")
	debugIntermediary = flag.Bool("debug-intermediary", false, "Print the intermediary generated program to stdout instead of writing it to a file")
	yaml              = flag.Bool("yaml", false, "Generate parameters in YAML format (default is a HTML table)")
)

func main() {
	flag.Parse()

	if *pkg == "" {
		var err error
		*pkg, err = detectPackage()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	err := generate()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func detectPackage() (string, error) {
	currentDir, err := filepath.Abs(".")
	if err != nil {
		return "", fmt.Errorf("could not detect absolute path to current directory: %w", err)
	}
	for {
		dat, err := os.ReadFile(filepath.Join(currentDir, "go.mod"))
		if os.IsNotExist(err) {
			if currentDir == filepath.Dir(currentDir) {
				// at the root
				break
			}
			currentDir = filepath.Dir(currentDir)
			continue
		} else if err != nil {
			return "", err
		}
		modulePath := modfile.ModulePath(dat)
		if modulePath == "" {
			break // no module path found
		}
		return modulePath, nil
	}
	return "", errors.New("could not detect package, make sure you are in the root of the connector directory or provide the -package flag manually")
}

func generate() (err error) {
	if *debugIntermediary {
		return executeTemplate(os.Stdout)
	}

	tmpDir, err := os.MkdirTemp(".", "readmegen_tmp")
	if err != nil {
		return fmt.Errorf("could not create temporary directory: %w", err)
	}
	defer func() {
		removeErr := os.RemoveAll(tmpDir)
		if removeErr != nil {
			removeErr = fmt.Errorf("could not remove temporary directory: %w", removeErr)
			err = errors.Join(err, removeErr)
		}
	}()

	f, err := os.Create(filepath.Join(tmpDir, "main.go"))
	if err != nil {
		return fmt.Errorf("could not create temporary file: %w", err)
	}
	defer f.Close()

	err = executeTemplate(f)
	if err != nil {
		return fmt.Errorf("could not execute template: %w", err)
	}
	f.Close()

	cmd := exec.Command("go", "run", "main.go")
	cmd.Dir = tmpDir
	cmd.Stdout = os.Stdout // pipe directly to stdout
	cmd.Stderr = os.Stderr // pipe directly to stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("intermediary program failed: %w", err)
	}

	return nil
}

func executeTemplate(out io.Writer) error {
	t, err := template.New("").Parse(tmpl)
	if err != nil {
		return fmt.Errorf("could not parse template: %w", err)
	}
	return t.Execute(out, data{
		ImportPath: *pkg,
		GenerateOptions: util.GenerateOptions{
			YAMLParameters: *yaml,
		},
	})
}

type data struct {
	ImportPath      string
	GenerateOptions util.GenerateOptions
}

const tmpl = `
package main

import (
	"fmt"
	"os"

	conn "{{ .ImportPath }}"
	"github.com/conduitio/conduit-connector-sdk/cmd/readmegen/util"
)

func main() {
	err := util.Generate(
		conn.Connector,
		util.GenerateOptions{
			YAMLParameters: {{ printf "%#v" .GenerateOptions.YAMLParameters }},
			Output:         os.Stdout,
		},
	)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}
}
`
