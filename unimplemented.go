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

import "context"

// UnimplementedDestination should be embedded to have forward compatible implementations.
type UnimplementedDestination struct{}

// Parameters needs to be overridden in the actual implementation.
func (UnimplementedDestination) Parameters() map[string]Parameter {
	return nil
}

// Configure needs to be overridden in the actual implementation.
func (UnimplementedDestination) Configure(context.Context, map[string]string) error {
	return ErrUnimplemented
}

// Open needs to be overridden in the actual implementation.
func (UnimplementedDestination) Open(context.Context) error {
	return ErrUnimplemented
}

// Write needs to be overridden in the actual implementation.
func (UnimplementedDestination) Write(context.Context, []Record) (int, error) {
	return 0, ErrUnimplemented
}

// Teardown needs to be overridden in the actual implementation.
func (UnimplementedDestination) Teardown(context.Context) error {
	return ErrUnimplemented
}
func (UnimplementedDestination) mustEmbedUnimplementedDestination() {}

// UnimplementedSource should be embedded to have forward compatible implementations.
type UnimplementedSource struct{}

// Parameters needs to be overridden in the actual implementation.
func (UnimplementedSource) Parameters() map[string]Parameter {
	return nil
}

// Configure needs to be overridden in the actual implementation.
func (UnimplementedSource) Configure(context.Context, map[string]string) error {
	return ErrUnimplemented
}

// Open needs to be overridden in the actual implementation.
func (UnimplementedSource) Open(context.Context, Position) error {
	return ErrUnimplemented
}

// Read needs to be overridden in the actual implementation.
func (UnimplementedSource) Read(context.Context) (Record, error) {
	return Record{}, ErrUnimplemented
}

// Ack should be overridden if acks need to be forwarded to the source,
// otherwise it is optional.
func (UnimplementedSource) Ack(context.Context, Position) error {
	return ErrUnimplemented
}

// Teardown needs to be overridden in the actual implementation.
func (UnimplementedSource) Teardown(context.Context) error {
	return ErrUnimplemented
}
func (UnimplementedSource) mustEmbedUnimplementedSource() {}
