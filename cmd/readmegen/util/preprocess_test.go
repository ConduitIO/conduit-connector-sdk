// Copyright © 2024 Meroxa, Inc.
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

package util

import "testing"

func TestPreprocess(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		want    string
		wantErr bool
	}{{
		name: "no tags",
		data: "hello world",
		want: "hello world",
	}, {
		name:    "tag not closed (missing)",
		data:    "<!-- readmegen:name --> some text",
		wantErr: true,
	}, {
		name:    "tag not closed (wrong tag name)",
		data:    "<!-- readmegen:name --> some text <!-- /readmegen:other -->",
		wantErr: true,
	}, {
		name:    "end tag not closed",
		data:    "<!-- readmegen:name --> some text <!-- /readmegen:name",
		wantErr: true,
	}, {
		name:    "unknown tag",
		data:    "<!-- readmegen:foo --> some text <!-- readmegen:foo -->",
		wantErr: true,
	}, {
		name: "single tag",
		data: "<!-- readmegen:name --> some text <!-- /readmegen:name -->",
		want: `<!-- readmegen:name -->{{ title .specification.Name }}<!-- /readmegen:name -->`,
	}, {
		name: "multiple tags",
		data: `
<!-- readmegen:name -->

This text will be overwritten,

regardless of what it holds...

<!-- /readmegen:name -->

# This will stay!

<!-- readmegen:version -->Another overwritten text<!-- /readmegen:version -->`,
		want: `
<!-- readmegen:name -->{{ title .specification.Name }}<!-- /readmegen:name -->

# This will stay!

<!-- readmegen:version -->{{ .specification.Version }}<!-- /readmegen:version -->`,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Preprocess(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Preprocess() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Preprocess() = %v, want %v", got, tt.want)
			}
		})
	}
}
