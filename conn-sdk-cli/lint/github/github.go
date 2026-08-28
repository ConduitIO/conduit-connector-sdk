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
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/gofri/go-github-ratelimit/v2/github_ratelimit"
	"github.com/google/go-github/v70/github"
)

var (
	globalGithubClient         *github.Client
	initGlobalGithubClientOnce sync.Once
)

const githubTokenEnvVar = "GITHUB_TOKEN"

func githubClient(module string) (client *github.Client, owner, repo string, err error) {
	initGlobalGithubClient()
	client = globalGithubClient
	owner, repo, err = ownerAndRepoFromModule(module)
	return
}

func initGlobalGithubClient() {
	initGlobalGithubClientOnce.Do(func() {
		rateLimiter := github_ratelimit.NewClient(nil)
		client := github.NewClient(rateLimiter)

		if token := os.Getenv(githubTokenEnvVar); token != "" {
			client = client.WithAuthToken(token)
		}

		globalGithubClient = client
	})
}

// ownerAndRepoFromModule returns the owner and repo name from a Go module name.
func ownerAndRepoFromModule(module string) (string, string, error) {
	moduleParts := strings.Split(module, "/")
	if len(moduleParts) != 3 {
		return "", "", fmt.Errorf("invalid module format: %s", module)
	}
	if moduleParts[0] != "github.com" {
		return "", "", fmt.Errorf("invalid module format (expected github.com): %s", module)
	}
	return moduleParts[1], moduleParts[2], nil
}
