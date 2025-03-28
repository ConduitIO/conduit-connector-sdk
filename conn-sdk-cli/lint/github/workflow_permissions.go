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
	"context"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/lint/common"
	"github.com/google/go-github/v70/github"
)

// WorkflowPermissionsLinter checks that the GitHub Actions permissions are set
// correctly.
type WorkflowPermissionsLinter struct{}

var _ common.Linter = &WorkflowPermissionsLinter{}

func (l *WorkflowPermissionsLinter) Name() string {
	return "github-workflow-permissions"
}

func (l *WorkflowPermissionsLinter) Lint(ctx context.Context, cfg common.Config) []error {
	client, owner, repo, err := githubClient(cfg.Module)
	if err != nil {
		return []error{err}
	}

	perm, _, err := client.Repositories.GetDefaultWorkflowPermissions(ctx, owner, repo)
	if err != nil {
		return []error{err}
	}

	var errs []error
	if !perm.GetCanApprovePullRequestReviews() {
		errs = append(errs, errors.New("GitHub Actions should be able to create and approve pull requests"))
	}
	if perm.GetDefaultWorkflowPermissions() != "write" {
		errs = append(errs, fmt.Errorf("GitHub Actions should have default workflow permissions set to \"write\", got %q", perm.GetDefaultWorkflowPermissions()))
	}

	if len(errs) > 0 {
		return []error{&workflowPermissionsError{errs}}
	}

	return nil
}

func (l *WorkflowPermissionsLinter) Fix(ctx context.Context, cfg common.Config, toFixErr error) error {
	var linterError *workflowPermissionsError
	if !errors.As(toFixErr, &linterError) {
		return common.ErrNoFix
	}

	client, owner, repo, err := githubClient(cfg.Module)
	if err != nil {
		return err
	}

	_, _, err = client.Repositories.EditDefaultWorkflowPermissions(ctx, owner, repo, github.DefaultWorkflowPermissionRepository{
		DefaultWorkflowPermissions:   github.Ptr("write"),
		CanApprovePullRequestReviews: github.Ptr(true),
	})
	if err != nil {
		return fmt.Errorf("failed to fix GitHub Actions permissions: %w", err)
	}

	return nil
}

type workflowPermissionsError struct{ errs []error }

func (e *workflowPermissionsError) Error() string { return errors.Join(e.errs...).Error() }
