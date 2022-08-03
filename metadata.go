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
	"fmt"
	"strconv"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
)

const (
	// MetadataOpenCDCVersion is a Record.Metadata key for the version of the
	// OpenCDC format (e.g. "v1"). This field exists to ensure the OpenCDC
	// format version can be easily identified in case the record gets marshaled
	// into a different untyped format (e.g. JSON).
	MetadataOpenCDCVersion = cpluginv1.MetadataOpenCDCVersion

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

// SetOpenCDCVersion sets the metadata value for key MetadataVersion to the
// current version of OpenCDC used. This
func (m Metadata) SetOpenCDCVersion() {
	m[MetadataOpenCDCVersion] = cpluginv1.OpenCDCVersion
}

// GetOpenCDCVersion returns the value for key
// MetadataOpenCDCVersion. If the value is does not exist or is empty the
// function returns ErrMetadataFieldNotFound.
func (m Metadata) GetOpenCDCVersion() (string, error) {
	return m.getValue(MetadataOpenCDCVersion)
}

// GetCreatedAt parses the value for key MetadataCreatedAt as a unix
// timestamp. If the value does not exist or the value is empty the function
// returns ErrMetadataFieldNotFound. If the value is not a valid unix timestamp
// in nanoseconds the function returns an error.
func (m Metadata) GetCreatedAt() (time.Time, error) {
	raw := m[MetadataCreatedAt]
	if raw == "" {
		return time.Time{}, fmt.Errorf("failed to get value for %q: %w", MetadataCreatedAt, ErrMetadataFieldNotFound)
	}

	unixNano, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse value for %q: %w", MetadataCreatedAt, err)
	}

	return time.Unix(0, unixNano), nil
}

// SetCreatedAt sets the metadata value for key MetadataCreatedAt as a
// unix timestamp in nanoseconds.
func (m Metadata) SetCreatedAt(createdAt time.Time) {
	m[MetadataCreatedAt] = strconv.FormatInt(createdAt.UnixNano(), 10)
}

// GetReadAt parses the value for key MetadataReadAt as a unix
// timestamp. If the value does not exist or the value is empty the function
// returns ErrMetadataFieldNotFound. If the value is not a valid unix timestamp
// in nanoseconds the function returns an error.
func (m Metadata) GetReadAt() (time.Time, error) {
	raw := m[MetadataReadAt]
	if raw == "" {
		return time.Time{}, fmt.Errorf("failed to get value for %q: %w", MetadataReadAt, ErrMetadataFieldNotFound)
	}

	unixNano, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse value for %q: %w", MetadataReadAt, err)
	}

	return time.Unix(0, unixNano), nil
}

// SetReadAt sets the metadata value for key MetadataReadAt as a unix
// timestamp in nanoseconds.
func (m Metadata) SetReadAt(createdAt time.Time) {
	m[MetadataReadAt] = strconv.FormatInt(createdAt.UnixNano(), 10)
}

// GetConduitPluginName returns the value for key
// MetadataConduitPluginName. If the value is does not exist or is empty the
// function returns ErrMetadataFieldNotFound.
func (m Metadata) GetConduitPluginName() (string, error) {
	return m.getValue(MetadataConduitPluginName)
}

// SetConduitPluginName sets the metadata value for key
// MetadataConduitPluginName.
func (m Metadata) SetConduitPluginName(name string) {
	m[MetadataConduitPluginName] = name
}

// GetConduitPluginVersion returns the value for key
// MetadataConduitPluginVersion. If the value is does not exist or is empty the
// function returns ErrMetadataFieldNotFound.
func (m Metadata) GetConduitPluginVersion() (string, error) {
	return m.getValue(MetadataConduitPluginVersion)
}

// SetConduitPluginVersion sets the metadata value for key
// MetadataConduitPluginVersion.
func (m Metadata) SetConduitPluginVersion(version string) {
	m[MetadataConduitPluginVersion] = version
}

// getValue returns the value for a specific key. If the value is does
// not exist or is empty the function returns ErrMetadataFieldNotFound.
func (m Metadata) getValue(key string) (string, error) {
	str := m[key]
	if str == "" {
		return "", fmt.Errorf("failed to get value for %q: %w", key, ErrMetadataFieldNotFound)
	}
	return str, nil
}
