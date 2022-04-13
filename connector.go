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

// Connector combines all constructors for each plugin into one struct.
type Connector struct {
	// NewSpecification should create a new Specification that describes the
	// connector. This field is mandatory, if it is empty the connector won't
	// work.
	NewSpecification func() Specification
	// NewSource should create a new Source plugin. If the plugin doesn't
	// implement a source connector this field can be nil.
	NewSource func() Source
	// NewDestination should create a new Destination plugin. If the plugin
	// doesn't implement a destination connector this field can be nil.
	NewDestination func() Destination
}
