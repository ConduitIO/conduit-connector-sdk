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
	"slices"

	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/lint/common"
	"github.com/google/go-github/v70/github"
)

const defaultBranch = "main"

type RulesetsLinter struct{}

var _ common.Linter = (*RulesetsLinter)(nil)

func (b *RulesetsLinter) Name() string {
	return "github-rulesets"
}

func (b *RulesetsLinter) Lint(ctx context.Context, cfg common.Config) []error {
	client := githubClient()

	owner, repo, err := ownerAndRepoFromModule(cfg.Module)
	if err != nil {
		return []error{err}
	}

	var errs []error
	err = b.lintRulesets(ctx, client, cfg.Module, owner, repo)
	if errors.Is(err, errNoRulesets) {
		// No rulesets, check if the repo is using branch protection.
		err = b.lintBranchProtection(ctx, client, cfg.Module, owner, repo)
		if errors.Is(err, errNoBranchProtection) {
			err = fmt.Errorf("no rulesets or branch protection for %q", defaultBranch)
		}
	}
	if err != nil {
		errs = append(errs, err)
	}

	return errs
}

func (b *RulesetsLinter) lintRulesets(ctx context.Context, client *github.Client, module, owner, repo string) error {
	rulesets, _, err := client.Repositories.GetAllRulesets(ctx, owner, repo, true)
	if err != nil {
		return fmt.Errorf("could not get rulesets: %w", err)
	}

	if len(rulesets) == 0 {
		return errNoRulesets
	}

	// TODO
	return nil
}

func (b *RulesetsLinter) lintBranchProtection(ctx context.Context, client *github.Client, module, owner, repo string) error {
	// No rulesets, check if the repo is using branch protection.
	protection, _, err := client.Repositories.GetBranchProtection(ctx, owner, repo, defaultBranch)
	if err != nil {
		return errNoBranchProtection
	}

	var errs []error
	if protection.RequiredPullRequestReviews == nil {
		errs = append(errs, fmt.Errorf("branch protection for %q should require pull request reviews", defaultBranch))
	} else {
		// Check if required pull request reviews are set up correctly.
		if !protection.RequiredPullRequestReviews.RequireCodeOwnerReviews {
			errs = append(errs, fmt.Errorf("branch protection for %q should require code owner reviews", defaultBranch))
		}
		if protection.RequiredPullRequestReviews.RequiredApprovingReviewCount < 1 {
			errs = append(errs, fmt.Errorf("branch protection for %q should require at least one approving review", defaultBranch))
		}
	}
	if protection.RequiredStatusChecks == nil {
		errs = append(errs, fmt.Errorf("branch protection for %q should require status checks", defaultBranch))
	} else {
		// Check if required status checks are set up correctly.
		if !protection.RequiredStatusChecks.Strict {
			errs = append(errs, fmt.Errorf("branch protection for %q should require branches to be up to date before merging", defaultBranch))
		}

		missingChecks := []string{
			"golangci-lint",
			"test",
			"validate-generated-files",
		}
		if protection.RequiredStatusChecks.Checks != nil {
			for _, check := range *protection.RequiredStatusChecks.Checks {
				if i := slices.Index(missingChecks, check.Context); i != -1 {
					missingChecks = append(missingChecks[:i], missingChecks[i+1:]...)
				}
			}
		}
		if len(missingChecks) > 0 {
			errs = append(errs, fmt.Errorf("branch protection for %q is missing required status checks: %v", defaultBranch, missingChecks))
		}
	}

	if protection.RequiredConversationResolution == nil || !protection.RequiredConversationResolution.Enabled {
		errs = append(errs, fmt.Errorf("branch protection for %q should require conversation resolution", defaultBranch))
	}

	if protection.AllowDeletions != nil && protection.AllowDeletions.Enabled {
		errs = append(errs, fmt.Errorf("branch protection for %q should not allow deletions", defaultBranch))
	}
	if protection.AllowForcePushes != nil && protection.AllowForcePushes.Enabled {
		errs = append(errs, fmt.Errorf("branch protection for %q should not allow force pushes", defaultBranch))
	}

	if len(errs) > 0 {
		return &branchProtectionError{errs: errs}
	}

	return nil
}

type rulesetsError struct{ errs []error }

func (e *rulesetsError) Error() string { return errors.Join(e.errs...).Error() }

type branchProtectionError struct{ errs []error }

func (e *branchProtectionError) Error() string { return errors.Join(e.errs...).Error() }

var (
	errNoRulesets         = errors.New("no ruleset found")
	errNoBranchProtection = errors.New("no branch protection found")
)
