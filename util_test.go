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

package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/config"
)

// ExampleUtil_ParseConfig shows the usage of Util.ParseConfig.
func ExampleUtil_ParseConfig() {
	cfg := config.Config{
		"foo": "bar   ", // will be sanitized
		// "bar" is missing, will be set to the default value
		"nested.baz": "1m",
	}

	var target struct {
		Foo    string `json:"foo"`
		Bar    int    `json:"bar"`
		Nested struct {
			Baz time.Duration `json:"baz"`
		} `json:"nested"`
	}

	err := Util.ParseConfig(context.Background(), cfg, &target)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v", target)

	// Output: {Foo:bar Bar:42 Nested:{Baz:1m0s}}
}
