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

package github

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/lint/common"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-github/v70/github"
)

// WorkflowsLinter checks if the repository contains the necessary GitHub
// Actions workflows.
type WorkflowsLinter struct{}

var _ common.Linter = (*WorkflowsLinter)(nil)

func (l *WorkflowsLinter) Name() string {
	return "github-workflows"
}

const (
	connectorTemplateOwner = "conduitio"
	connectorTemplateRepo  = "conduit-connector-template"
	githubWorkflowsPath    = "./.github/workflows"
)

func (l *WorkflowsLinter) Lint(ctx context.Context, cfg common.Config) []error {
	// Check if the template workflows are already downloaded.
	templatePath := filepath.Join(os.TempDir(), "conn-sdk-cli", connectorTemplateOwner, connectorTemplateRepo, githubWorkflowsPath)

	if _, err := os.Stat(templatePath); os.IsNotExist(err) {
		client, _, _, err := githubClient(cfg.Module)
		if err != nil {
			return []error{err}
		}

		// Download the template workflows and store them in a temporary directory.
		err = l.downloadGithubDirectory(
			ctx,
			connectorTemplateOwner,
			connectorTemplateRepo,
			githubWorkflowsPath,
			templatePath,
			client)
		if err != nil {
			return []error{fmt.Errorf("failed to download template workflows: %w", err)}
		}
	}

	files, err := os.ReadDir(templatePath)
	if err != nil {
		return []error{fmt.Errorf("failed to read local directory: %w", err)}
	}

	var errs []error
	for _, file := range files {
		templateFilePath := filepath.Join(templatePath, file.Name())
		localFilePath := filepath.Join(githubWorkflowsPath, file.Name())

		if _, err := os.Stat(localFilePath); os.IsNotExist(err) {
			errs = append(errs, &workflowError{
				filename: file.Name(),
				err:      fmt.Errorf("workflow file %s is missing", file.Name()),
			})
			continue
		}

		templateContent, err := os.ReadFile(templateFilePath)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to read template workflow file %s: %w", file.Name(), err))
			continue
		}

		localContent, err := os.ReadFile(localFilePath)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to read local workflow file %s: %w", file.Name(), err))
			continue
		}

		templateContent = bytes.TrimSpace(templateContent)
		localContent = bytes.TrimSpace(localContent)
		diff := cmp.Diff(string(templateContent), string(localContent))
		if diff != "" {
			errs = append(errs, &workflowError{
				filename: file.Name(),
				err:      fmt.Errorf("workflow file %s contains unexpected content:\n%s", file.Name(), diff),
			})
		}
	}

	return errs
}

func (l *WorkflowsLinter) downloadGithubDirectory(
	ctx context.Context,
	owner, repo, dirPath, localPath string,
	client *github.Client,
) error {
	_, directoryContent, _, err := client.Repositories.GetContents(
		ctx,
		owner,
		repo,
		dirPath,
		&github.RepositoryContentGetOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to get folder contents: %w", err)
	}

	// Create local path
	if err := os.MkdirAll(localPath, 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %w", err)
	}

	// Download each file
	downloadFile := func(file *github.RepositoryContent) error {
		localFilePath := filepath.Join(localPath, file.GetName())

		reader, _, err := client.Repositories.DownloadContents(
			ctx,
			owner,
			repo,
			file.GetPath(),
			&github.RepositoryContentGetOptions{},
		)
		if err != nil {
			return err
		}
		defer reader.Close()

		// Create local file
		out, err := os.Create(localFilePath)
		if err != nil {
			return fmt.Errorf("failed to create local file %s: %w", localFilePath, err)
		}
		defer out.Close()

		// Copy downloaded content to local file
		_, err = io.Copy(out, reader)
		if err != nil {
			return fmt.Errorf("failed to write %s: %w", localFilePath, err)
		}
		return nil
	}

	for _, file := range directoryContent {
		if file.GetType() == "file" {
			if err := downloadFile(file); err != nil {
				return err
			}
		}
	}

	return nil
}

func (l *WorkflowsLinter) Fix(_ context.Context, _ common.Config, toFixErr error) error {
	var workflowErr *workflowError
	if !errors.As(toFixErr, &workflowErr) {
		return common.ErrNoFix
	}

	templateFilePath := filepath.Join(os.TempDir(), "conn-sdk-cli", connectorTemplateOwner, connectorTemplateRepo, githubWorkflowsPath, workflowErr.filename)
	localFilePath := filepath.Join(githubWorkflowsPath, workflowErr.filename)

	// Ensure the .github/workflows directory exists.
	if err := os.MkdirAll(filepath.Dir(localFilePath), 0755); err != nil {
		return fmt.Errorf("failed to create workflows directory: %w", err)
	}

	templateContent, err := os.ReadFile(templateFilePath)
	if err != nil {
		return fmt.Errorf("failed to read template workflow file %s: %w", workflowErr.filename, err)
	}

	// Overwrite the file contents with the template.
	err = os.WriteFile(localFilePath, templateContent, 0644)
	if err != nil {
		return fmt.Errorf("failed to write workflow file %s: %w", workflowErr.filename, err)
	}

	return nil
}

type workflowError struct {
	filename string
	err      error
}

func (e *workflowError) Error() string { return e.err.Error() }
