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
	"testing"

	"github.com/conduitio/conduit-connector-protocol/cplugin"
	"github.com/matryer/is"
)

func TestSpecifier_NilSource(t *testing.T) {
	is := is.New(t)
	// ensure that having a connector without a source still works
	p := NewSpecifierPlugin(Specification{}, nil, UnimplementedDestination{})
	_, err := p.Specify(context.Background(), cplugin.SpecifierSpecifyRequest{})
	is.NoErr(err)
}

func TestSpecifier_NilDestination(t *testing.T) {
	is := is.New(t)
	// ensure that having a connector without a destination still works
	p := NewSpecifierPlugin(Specification{}, UnimplementedSource{}, nil)
	_, err := p.Specify(context.Background(), cplugin.SpecifierSpecifyRequest{})
	is.NoErr(err)
}
