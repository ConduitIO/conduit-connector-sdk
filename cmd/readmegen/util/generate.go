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

package util

import (
	"embed"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

var (
	//go:embed templates/*
	templates embed.FS
)

type GenerateOptions struct {
	// Required fields
	Data       any       // The data to use in template generation
	ReadmePath string    // Path to the readme template file
	Out        io.Writer // Where to write the generated output

	// Optional fields
	FuncMap     template.FuncMap // Custom function map to merge with default functions
	TemplatesFS fs.FS            // Custom templates directory to merge with default templates
}

func (opts GenerateOptions) validate() error {
	// Validate required fields
	if opts.Data == nil {
		return fmt.Errorf("Data is required")
	}
	if opts.ReadmePath == "" {
		return fmt.Errorf("ReadmePath is required")
	}
	if opts.Out == nil {
		return fmt.Errorf("Out is required")
	}
	return nil
}

func Generate(opts GenerateOptions) error {
	if err := opts.validate(); err != nil {
		return fmt.Errorf("invalid options: %w", err)
	}

	readme, err := os.ReadFile(opts.ReadmePath)
	if err != nil {
		return fmt.Errorf("could not read readme file %v: %w", opts.ReadmePath, err)
	}
	readmeTmpl, err := Preprocess(string(readme))
	if err != nil {
		return fmt.Errorf("could not preprocess readme file %v: %w", opts.ReadmePath, err)
	}

	t := template.New("readme").Funcs(funcMap).Funcs(sprig.FuncMap())
	if opts.FuncMap != nil {
		t = t.Funcs(opts.FuncMap)
	}

	t = template.Must(t.ParseFS(templates, "templates/*.tmpl"))
	if opts.TemplatesFS != nil {
		t = template.Must(t.ParseFS(opts.TemplatesFS, "*.tmpl"))
	}

	t = template.Must(t.Parse(readmeTmpl))

	return t.Execute(opts.Out, opts.Data)
}

var funcMap = template.FuncMap{
	"formatCommentYAML": formatCommentYAML,
	"formatValueYAML":   formatValueYAML,
	"args":              args,
}

func args(kvs ...any) (map[string]any, error) {
	if len(kvs)%2 != 0 {
		return nil, errors.New("args requires even number of arguments")
	}
	m := make(map[string]any)
	for i := 0; i < len(kvs); i += 2 {
		s, ok := kvs[i].(string)
		if !ok {
			return nil, errors.New("even args must be strings")
		}
		m[s] = kvs[i+1]
	}
	return m, nil
}

// formatCommentYAML takes a markdown text and formats it as a comment in a YAML
// file. The comment is prefixed with the given indent level and "# ". The lines
// are wrapped at 80 characters.
func formatCommentYAML(text string, indent int) string {
	const (
		prefix     = "# "
		lineLen    = 80
		tmpNewLine = "〠"
	)

	// remove markdown new lines
	text = strings.ReplaceAll(text, "\n\n", tmpNewLine)
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.ReplaceAll(text, tmpNewLine, "\n")

	comment := formatMultiline(text, strings.Repeat(" ", indent)+prefix, lineLen)
	// remove first indent and last new line
	comment = comment[indent : len(comment)-1]
	return comment
}

func formatValueYAML(value string, indent int) string {
	switch {
	case value == "":
		return `""`
	case strings.Contains(value, "\n"):
		// specifically used in the javascript processor
		formattedValue := formatMultiline(value, strings.Repeat(" ", indent), 10000)
		return fmt.Sprintf("|\n%s", formattedValue)
	default:
		return fmt.Sprintf(`"%s"`, value)
	}
}

func formatMultiline(
	input string,
	prefix string,
	maxLineLen int,
) string {
	textLen := maxLineLen - len(prefix)

	// split the input into lines of length textLen
	lines := strings.Split(input, "\n")
	var formattedLines []string
	for _, line := range lines {
		if len(line) <= textLen {
			formattedLines = append(formattedLines, line)
			continue
		}

		// split the line into multiple lines, don't break words
		words := strings.Fields(line)
		var formattedLine string
		for _, word := range words {
			if len(formattedLine)+len(word) > textLen {
				formattedLines = append(formattedLines, formattedLine[1:])
				formattedLine = ""
			}
			formattedLine += " " + word
		}
		if formattedLine != "" {
			formattedLines = append(formattedLines, formattedLine[1:])
		}
	}

	// combine lines including indent and prefix
	var formatted string
	for _, line := range formattedLines {
		formatted += prefix + line + "\n"
	}

	return formatted
}
