// Copyright © 2022 Meroxa, Inc.
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

package example

import (
	"time"

	"example.com/asdf/internal"
)

type GlobalConfig struct {
	Internal Another
	// GlobalString is an example parameter. This comment will be converted into
	// the parameter description. Can be multiline too! Just write a godoc like
	// you normally would.
	GlobalString string `default:"foo" required:"true"`
	// GlobalDuration demonstrates that time.Duration is also allowed.
	GlobalDuration time.Duration `default:"1s"`
}

type Another internal.GlobalConfig
