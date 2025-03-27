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
	client, owner, repo, err := githubClient(cfg.Module)
	if err != nil {
		return []error{err}
	}

	ghRepo, _, err := client.Repositories.Get(ctx, owner, repo)
	if err != nil {
		return []error{fmt.Errorf("could not get repository settings for %s: %w", cfg.Module, err)}
	}

	var errs []error
	if err := l.lintRepoSettings(ghRepo); err != nil {
		errs = append(errs, err)
	}
	if ghRepo.GetDefaultBranch() != defaultBranch {
		errs = append(errs, &defaultBranchError{
			have: ghRepo.GetDefaultBranch(),
			want: defaultBranch,
		})
	}

	return errs
}

func (l *RepoSettingsLinter) lintRepoSettings(ghRepo *github.Repository) error {
	var errs []error

	// General Features
	if ghRepo.GetHasWiki() {
		errs = append(errs, errors.New("wikis should be disabled"))
	}
	if !ghRepo.GetHasIssues() {
		errs = append(errs, errors.New("issues should be enabled"))
	}
	if ghRepo.GetHasDiscussions() {
		errs = append(errs, errors.New("discussions should be disabled"))
	}
	if ghRepo.GetHasProjects() {
		errs = append(errs, errors.New("projects should be disabled"))
	}

	// Pull Requests
	if ghRepo.GetAllowMergeCommit() {
		errs = append(errs, errors.New("merge commits should be disabled"))
	}
	if !ghRepo.GetAllowSquashMerge() {
		errs = append(errs, errors.New("squash merging should be enabled"))
	}
	if ghRepo.GetAllowRebaseMerge() {
		errs = append(errs, errors.New("rebase merging should be disabled"))
	}
	if !ghRepo.GetAllowAutoMerge() {
		errs = append(errs, errors.New("auto-merge should be enabled"))
	}
	if !ghRepo.GetDeleteBranchOnMerge() {
		errs = append(errs, errors.New("head branches should be automatically deleted"))
	}
	if !ghRepo.GetAllowUpdateBranch() {
		errs = append(errs, errors.New("branches should always be suggested to update"))
	}

	if len(errs) > 0 {
		// Collect errors into a single error, these are all repo settings errors
		// and can be fixed in a single operation.
		return &repoSettingsError{errs: errs}
	}

	return nil
}

func (l *RepoSettingsLinter) Fix(ctx context.Context, cfg common.Config, toFixErr error) error {
	var linterError *repoSettingsError
	if errors.As(toFixErr, &linterError) {
		return l.fixRepoSettings(ctx, cfg)
	}

	var errDefaultBranch *defaultBranchError
	if errors.As(toFixErr, &errDefaultBranch) {
		return l.fixDefaultBranch(ctx, cfg, errDefaultBranch)
	}

	return common.ErrNoFix
}

func (l *RepoSettingsLinter) fixRepoSettings(ctx context.Context, cfg common.Config) error {
	client, owner, repo, err := githubClient(cfg.Module)
	if err != nil {
		return err
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
		return fmt.Errorf("could not update repository settings for %s: %w", cfg.Module, err)
	}

	return nil
}

func (l *RepoSettingsLinter) fixDefaultBranch(ctx context.Context, cfg common.Config, toFixErr *defaultBranchError) error {
	client, owner, repo, err := githubClient(cfg.Module)
	if err != nil {
		return err
	}

	_, _, err = client.Repositories.RenameBranch(ctx, owner, repo, toFixErr.have, toFixErr.want)
	if err != nil {
		return fmt.Errorf("could not rename default branch to %s: %w", toFixErr.want, err)
	}

	return nil
}

type repoSettingsError struct{ errs []error }

func (e *repoSettingsError) Error() string { return errors.Join(e.errs...).Error() }

type defaultBranchError struct {
	have string
	want string
}

func (e *defaultBranchError) Error() string {
	return fmt.Sprintf("default branch should be %q, but is %q", e.want, e.have)
}
