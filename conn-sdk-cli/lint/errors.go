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
	"fmt"
	"strings"

	"github.com/conduitio/conduit-connector-sdk/conn-sdk-cli/lint/common"
)

type linterError struct {
	linter common.Linter
	err    error
}

func newLinterError(linter common.Linter, err error) *linterError {
	return &linterError{
		linter: linter,
		err:    err,
	}
}

func (e *linterError) Error() string {
	return e.err.Error()
}

type linterErrors []*linterError

func (errs linterErrors) Error() string {
	const divider = "\n\n----------------\n\n"

	var s strings.Builder
	for _, err := range errs {
		indent := strings.Repeat(" ", len(err.linter.Name())+2)

		errStr := err.Error()
		errStr = strings.ReplaceAll(
			errStr,
			"\n",
			fmt.Sprintf("\n%s", indent),
		)

		s.WriteString(fmt.Sprintf("%s: %s", err.linter.Name(), errStr))
		s.WriteString(divider)
	}

	// Note: add new line because Cobra adds a space in front of the error message.
	return "\n" + strings.TrimSuffix(s.String(), divider)
}
