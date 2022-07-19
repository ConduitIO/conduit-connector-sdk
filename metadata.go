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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
)

var (
	ErrMetadataValueNotFound = errors.New("metadata value not found")
)

const (
	// MetadataCreatedAt is a Record.Metadata key for the time when the record
	// was created in the 3rd party system. The expected format is a unix
	// timestamp in nanoseconds.
	MetadataCreatedAt = cpluginv1.MetadataCreatedAt
	// MetadataReadAt is a Record.Metadata key for the time when the record was
	// read from the 3rd party system. The expected format is a unix timestamp
	// in nanoseconds.
	MetadataReadAt = cpluginv1.MetadataReadAt

	// MetadataConduitPluginName is a Record.Metadata key for the name of the
	// plugin that created this record.
	MetadataConduitPluginName = cpluginv1.MetadataConduitPluginName
	// MetadataConduitPluginVersion is a Record.Metadata key for the version of
	// the plugin that created this record.
	MetadataConduitPluginVersion = cpluginv1.MetadataConduitPluginVersion
)

// GetMetadataCreatedAt parses the value for key MetadataCreatedAt as a unix
// timestamp. If the value does not exist or the value is empty the function
// returns ErrMetadataValueNotFound. If the value is not a valid unix timestamp
// in nanoseconds the function returns an error.
func GetMetadataCreatedAt(metadata map[string]string) (time.Time, error) {
	raw := metadata[MetadataCreatedAt]
	if raw == "" {
		return time.Time{}, fmt.Errorf("failed to get value for %q: %w", MetadataCreatedAt, ErrMetadataValueNotFound)
	}

	unixNano, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse value for %q: %w", MetadataCreatedAt, err)
	}

	return time.Unix(0, unixNano), nil
}

// SetMetadataCreatedAt sets the metadata value for key MetadataCreatedAt as a
// unix timestamp in nanoseconds.
func SetMetadataCreatedAt(metadata map[string]string, createdAt time.Time) {
	metadata[MetadataCreatedAt] = strconv.FormatInt(createdAt.UnixNano(), 10)
}

// GetMetadataReadAt parses the value for key MetadataReadAt as a unix
// timestamp. If the value does not exist or the value is empty the function
// returns ErrMetadataValueNotFound. If the value is not a valid unix timestamp
// in nanoseconds the function returns an error.
func GetMetadataReadAt(metadata map[string]string) (time.Time, error) {
	raw := metadata[MetadataReadAt]
	if raw == "" {
		return time.Time{}, fmt.Errorf("failed to get value for %q: %w", MetadataReadAt, ErrMetadataValueNotFound)
	}

	unixNano, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse value for %q: %w", MetadataReadAt, err)
	}

	return time.Unix(0, unixNano), nil
}

// SetMetadataReadAt sets the metadata value for key MetadataReadAt as a unix
// timestamp in nanoseconds.
func SetMetadataReadAt(metadata map[string]string, createdAt time.Time) {
	metadata[MetadataReadAt] = strconv.FormatInt(createdAt.UnixNano(), 10)
}

// GetMetadataConduitPluginName returns the value for key
// MetadataConduitPluginName. If the value is does not exist or is empty the
// function returns ErrMetadataValueNotFound.
func GetMetadataConduitPluginName(metadata map[string]string) (string, error) {
	return getMetadataValue(metadata, MetadataConduitPluginName)
}

// SetMetadataConduitPluginName sets the metadata value for key
// MetadataConduitPluginName.
func SetMetadataConduitPluginName(metadata map[string]string, name string) {
	metadata[MetadataConduitPluginName] = name
}

// GetMetadataConduitPluginVersion returns the value for key
// MetadataConduitPluginVersion. If the value is does not exist or is empty the
// function returns ErrMetadataValueNotFound.
func GetMetadataConduitPluginVersion(metadata map[string]string) (string, error) {
	return getMetadataValue(metadata, MetadataConduitPluginVersion)
}

// SetMetadataConduitPluginVersion sets the metadata value for key
// MetadataConduitPluginVersion.
func SetMetadataConduitPluginVersion(metadata map[string]string, version string) {
	metadata[MetadataConduitPluginVersion] = version
}

// getMetadataValue returns the value for a specific key. If the value is does
// not exist or is empty the function returns ErrMetadataValueNotFound.
func getMetadataValue(metadata map[string]string, key string) (string, error) {
	str := metadata[key]
	if str == "" {
		return "", fmt.Errorf("failed to get value for %q: %w", key, ErrMetadataValueNotFound)
	}
	return str, nil
}
