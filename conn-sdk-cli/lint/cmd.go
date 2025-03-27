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

package lint

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/lint/common"
	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/lint/github"
)

type Command struct {
	fix     bool
	linters []common.Linter
}

func NewCommand(fix bool) *Command {
	return &Command{
		fix: fix,
		linters: []common.Linter{
			&github.RepoSettingsLinter{},
			&github.RulesetsLinter{},
			&github.WorkflowPermissionsLinter{},
		},
	}
}

func (cmd *Command) Execute(ctx context.Context) error {
	cfg, err := cmd.parseConfig()
	if err != nil {
		return err
	}

	var linterErrs linterErrors
	for _, linter := range cmd.linters {
		errs := linter.Lint(ctx, cfg)
		for _, err := range errs {
			linterErrs = append(linterErrs, newLinterError(linter, err))
		}
	}

	if cmd.fix {
		fixedIndices := make([]int, 0, len(linterErrs))
		for i, linterErr := range linterErrs {
			fixer, ok := linterErr.linter.(common.Fixer)
			if !ok {
				continue
			}
			err := fixer.Fix(ctx, cfg, linterErr.err)
			if err != nil {
				if !errors.Is(err, common.ErrNoFix) {
					slog.Error("Failed to fix lint error", "linter", fixer.Name(), "error", err)
				}
				continue
			}
			fixedIndices = append(fixedIndices, i)
		}
		slices.Reverse(fixedIndices)
		for _, i := range fixedIndices {
			linterErrs = append(linterErrs[:i], linterErrs[i+1:]...)
		}
	}

	if len(linterErrs) > 0 {
		// The if is needed to avoid returning a typed nil.
		return linterErrs
	}

	return nil
}

func (cmd *Command) parseConfig() (common.Config, error) {
	var cfg common.Config
	var err error

	cfg.Module, err = cmd.getModuleName()
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}

func (cmd *Command) getModuleName() (string, error) {
	gomod, err := os.Open("go.mod")
	if err != nil {
		return "", fmt.Errorf("failed to read go.mod (make sure the command is being executed in the root of the connector repository): %w", err)
	}
	defer gomod.Close()

	scanner := bufio.NewScanner(gomod)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "module ") {
			return strings.TrimPrefix(line, "module "), nil
		}
	}

	return "", fmt.Errorf("failed to find module name in go.mod, make sure the project contains a valid go.mod file")
}
