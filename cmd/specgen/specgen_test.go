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

package specgen

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/matryer/is"
)

func TestParseSpecification(t *testing.T) {
	is := is.New(t)
	spec, err := ParseSpecification("./example")
	is.NoErr(err)

	spew.Dump(spec)
}

func TestParseModule(t *testing.T) {
	is := is.New(t)
	mod, err := ParseModule("./example")
	is.NoErr(err)

	spew.Dump(mod)
}

func TestParsePackage(t *testing.T) {
	is := is.New(t)
	pkg, err := ParsePackage("./example")
	is.NoErr(err)

	spew.Dump(pkg)
}
