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
	_ "embed"
	"errors"
	"io"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var (
	//go:embed readme.tmpl
	readmeTmpl string
	//go:embed parameters.table.tmpl
	parametersTableTmpl string
	//go:embed parameters.yaml.tmpl
	parametersYAMLTmpl string
)

type GenerateOptions struct {
	YAMLParameters bool
	Output         io.Writer
}

func Generate(conn sdk.Connector, opts GenerateOptions) error {
	templates := []string{readmeTmpl, parametersTableTmpl}
	if opts.YAMLParameters {
		// switch table template to YAML template
		templates[1] = parametersYAMLTmpl
	}
	t := template.Must(
		template.New("readme").
			Funcs(funcMap).
			Funcs(sprig.FuncMap()).
			Parse(strings.Join(templates, "\n")),
	)
	err := t.Execute(opts.Output, map[string]any{
		"specification":     conn.NewSpecification(),
		"sourceParams":      conn.NewSource().Parameters(),
		"destinationParams": conn.NewDestination().Parameters(),
	})
	return err
}

var funcMap = template.FuncMap{
	"formatCommentYAML": formatCommentYAML,
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
