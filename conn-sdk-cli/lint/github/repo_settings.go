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

type RepoSettingsLinter struct{}

var _ common.Linter = &RepoSettingsLinter{}

func (l *RepoSettingsLinter) Name() string {
	return "github-repo-settings"
}

func (l *RepoSettingsLinter) Lint(ctx context.Context, cfg common.Config) []error {
	client := githubClient()

	owner, repo, err := ownerAndRepoFromModule(cfg.Module)
	if err != nil {
		return []error{err}
	}

	ghRepo, _, err := client.Repositories.Get(ctx, owner, repo)
	if err != nil {
		return []error{fmt.Errorf("could not get repository settings for %s: %w", cfg.Module, err)}
	}

	var errs []error

	// General Features
	if ghRepo.GetHasWiki() {
		errs = append(errs, fmt.Errorf("wikis should be disabled"))
	}
	if !ghRepo.GetHasIssues() {
		errs = append(errs, fmt.Errorf("issues should be enabled"))
	}
	if ghRepo.GetHasDiscussions() {
		errs = append(errs, fmt.Errorf("discussions should be disabled"))
	}
	if ghRepo.GetHasProjects() {
		errs = append(errs, fmt.Errorf("projects should be disabled"))
	}

	// Pull Requests
	if ghRepo.GetAllowMergeCommit() {
		errs = append(errs, fmt.Errorf("merge commits should be disabled"))
	}
	if !ghRepo.GetAllowSquashMerge() {
		errs = append(errs, fmt.Errorf("squash merging should be enabled"))
	}
	if ghRepo.GetAllowRebaseMerge() {
		errs = append(errs, fmt.Errorf("rebase merging should be disabled"))
	}
	if !ghRepo.GetAllowAutoMerge() {
		errs = append(errs, fmt.Errorf("auto-merge should be enabled"))
	}
	if !ghRepo.GetDeleteBranchOnMerge() {
		errs = append(errs, fmt.Errorf("head branches should be automatically deleted"))
	}
	if !ghRepo.GetAllowUpdateBranch() {
		errs = append(errs, fmt.Errorf("branches should always be suggested to update"))
	}

	if len(errs) > 0 {
		return []error{&repoSettingsLinterError{err: errors.Join(errs...)}}
	}

	return nil
}

func (l *RepoSettingsLinter) Fix(ctx context.Context, cfg common.Config, toFixErr error) (bool, error) {
	var linterError *repoSettingsLinterError
	if !errors.As(toFixErr, &linterError) {
		return false, nil
	}

	client := githubClient()
	owner, repo, err := ownerAndRepoFromModule(cfg.Module)
	if err != nil {
		return false, err
	}

	_, _, err = client.Repositories.Edit(ctx, owner, repo, &github.Repository{
		HasWiki:        github.Ptr(false),
		HasIssues:      github.Ptr(true),
		HasDiscussions: github.Ptr(false),
		HasProjects:    github.Ptr(false),

		AllowMergeCommit:    github.Ptr(false),
		AllowSquashMerge:    github.Ptr(true),
		AllowRebaseMerge:    github.Ptr(false),
		AllowAutoMerge:      github.Ptr(true),
		DeleteBranchOnMerge: github.Ptr(true),
		AllowUpdateBranch:   github.Ptr(true),
	})
	if err != nil {
		return false, fmt.Errorf("could not update repository settings for %s: %w", cfg.Module, err)
	}

	return true, nil
}

type repoSettingsLinterError struct{ err error }

func (e *repoSettingsLinterError) Error() string { return e.err.Error() }
