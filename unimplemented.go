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

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
)

// UnimplementedDestination should be embedded to have forward compatible implementations.
type UnimplementedDestination struct{}

// Config needs to be overridden in the actual implementation.
func (UnimplementedDestination) Config() DestinationConfig {
	panic("it is required to implement Config")
}

// Open needs to be overridden in the actual implementation.
func (UnimplementedDestination) Open(context.Context) error {
	return fmt.Errorf("action \"Open\": %w", ErrUnimplemented)
}

// Write needs to be overridden in the actual implementation.
func (UnimplementedDestination) Write(context.Context, []opencdc.Record) (int, error) {
	return 0, fmt.Errorf("action \"Write\": %w", ErrUnimplemented)
}

// Teardown needs to be overridden in the actual implementation.
func (UnimplementedDestination) Teardown(context.Context) error {
	return fmt.Errorf("action \"Teardown\": %w", ErrUnimplemented)
}

// LifecycleOnCreated won't do anything by default.
func (UnimplementedDestination) LifecycleOnCreated(context.Context, config.Config) error {
	return nil
}

// LifecycleOnUpdated won't do anything by default.
func (UnimplementedDestination) LifecycleOnUpdated(context.Context, config.Config, config.Config) error {
	return nil
}

// LifecycleOnDeleted won't do anything by default.
func (UnimplementedDestination) LifecycleOnDeleted(context.Context, config.Config) error {
	return nil
}

func (UnimplementedDestination) mustEmbedUnimplementedDestination() {}

type UnimplementedDestinationConfig struct{}

func (UnimplementedDestinationConfig) mustEmbedUnimplementedDestinationConfig() {}

func (UnimplementedDestinationConfig) Validate(context.Context) error {
	return nil
}

// UnimplementedSource should be embedded to have forward compatible implementations.
type UnimplementedSource struct{}

// Config needs to be overridden in the actual implementation.
func (UnimplementedSource) Config() SourceConfig {
	panic("it is required to implement Config")
}

// Open needs to be overridden in the actual implementation.
func (UnimplementedSource) Open(context.Context, opencdc.Position) error {
	return fmt.Errorf("action \"Open\": %w", ErrUnimplemented)
}

// Read needs to be overridden in the actual implementation.
func (UnimplementedSource) Read(context.Context) (opencdc.Record, error) {
	return opencdc.Record{}, fmt.Errorf("action \"Read\": %w", ErrUnimplemented)
}

// ReadN can be overridden. If it's not implemented, Read will be used as a fallback.
func (s UnimplementedSource) ReadN(context.Context, int) ([]opencdc.Record, error) {
	return nil, fmt.Errorf("action \"ReadN\": %w", ErrUnimplemented)
}

// Ack should be overridden if acks need to be forwarded to the source,
// otherwise it is optional.
func (UnimplementedSource) Ack(context.Context, opencdc.Position) error {
	return fmt.Errorf("action \"Ack\": %w", ErrUnimplemented)
}

// Teardown needs to be overridden in the actual implementation.
func (UnimplementedSource) Teardown(context.Context) error {
	return fmt.Errorf("action \"Teardown\": %w", ErrUnimplemented)
}

// LifecycleOnCreated won't do anything by default.
func (UnimplementedSource) LifecycleOnCreated(context.Context, config.Config) error {
	return nil
}

// LifecycleOnUpdated won't do anything by default.
func (UnimplementedSource) LifecycleOnUpdated(context.Context, config.Config, config.Config) error {
	return nil
}

// LifecycleOnDeleted won't do anything by default.
func (UnimplementedSource) LifecycleOnDeleted(context.Context, config.Config) error {
	return nil
}

func (UnimplementedSource) mustEmbedUnimplementedSource() {}

type UnimplementedSourceConfig struct{}

func (UnimplementedSourceConfig) mustEmbedUnimplementedSourceConfig() {}

func (UnimplementedSourceConfig) Validate(context.Context) error {
	return nil
}
