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
	"testing"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"github.com/matryer/is"
	"go.uber.org/multierr"
)

func TestValidation_Param_Type(t *testing.T) {
	is := is.New(t)
	tests := []struct {
		name    string
		config  map[string]string
		params  map[string]Parameter
		wantErr bool
	}{
		{
			name: "valid type number",
			config: map[string]string{
				"param1": "3",
			},
			params: map[string]Parameter{
				"param1": {
					Default: "3.3",
					Type:    ParameterTypeNumber,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid type number",
			config: map[string]string{
				"param1": "not-a-number",
			},
			params: map[string]Parameter{
				"param1": {
					Default: "3.3",
					Type:    ParameterTypeNumber,
				},
			},
			wantErr: true,
		},
		{
			name: "valida default type number",
			config: map[string]string{
				"param1": "",
			},
			params: map[string]Parameter{
				"param1": {
					Default: "3",
					Type:    ParameterTypeNumber,
				},
			},
			wantErr: false,
		},
		{
			name: "valid type bool",
			config: map[string]string{
				"param1": "1", // 1, t, T, True, TRUE are all valid booleans
			},
			params: map[string]Parameter{
				"param1": {
					Type: ParameterTypeBool,
				},
			},
			wantErr: false,
		},
		{
			name: "valid type bool",
			config: map[string]string{
				"param1": "true",
			},
			params: map[string]Parameter{
				"param1": {
					Type: ParameterTypeBool,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid type bool",
			config: map[string]string{
				"param1": "not-a-bool",
			},
			params: map[string]Parameter{
				"param1": {
					Type: ParameterTypeBool,
				},
			},
			wantErr: true,
		},
		{
			name: "valid type duration",
			config: map[string]string{
				"param1": "1s",
			},
			params: map[string]Parameter{
				"param1": {
					Type: ParameterTypeDuration,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid type duration",
			config: map[string]string{
				"param1": "not-a-duration",
			},
			params: map[string]Parameter{
				"param1": {
					Type: ParameterTypeDuration,
				},
			},
			wantErr: true,
		},
		{
			name: "valid type string",
			config: map[string]string{
				"param1": "param",
			},
			params: map[string]Parameter{
				"param1": {
					Type: ParameterTypeString,
				},
			},
			wantErr: false,
		},
		{
			name: "valid type file",
			config: map[string]string{
				"param1": "some-data", // a file is a slice of bytes, so any string is valid
			},
			params: map[string]Parameter{
				"param1": {
					Type: ParameterTypeFile,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyConfigValidations(tt.params, tt.config)
			if err != nil && tt.wantErr {
				is.True(errors.Is(err, ErrInvalidParamType))
			} else if err != nil || tt.wantErr {
				t.Errorf("UtilityFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestValidation_Param_Value(t *testing.T) {
	is := is.New(t)

	tests := []struct {
		name    string
		config  map[string]string
		params  map[string]Parameter
		wantErr bool
		err     error
	}{
		{
			name: "required validation failed",
			config: map[string]string{
				"param1": "",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationRequired{},
				}},
			},
			wantErr: true,
			err:     ErrRequiredParameterMissing,
		},
		{
			name: "required validation pass",
			config: map[string]string{
				"param1": "value",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationRequired{},
				}},
			},
			wantErr: false,
		},
		{
			name: "less than validation failed",
			config: map[string]string{
				"param1": "20",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationLessThan{10},
				}},
			},
			wantErr: true,
			err:     ErrLessThanValidationFail,
		},
		{
			name: "less than validation pass",
			config: map[string]string{
				"param1": "0",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationLessThan{10},
				}},
			},
			wantErr: false,
		},
		{
			name: "greater than validation failed",
			config: map[string]string{
				"param1": "0",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationGreaterThan{10},
				}},
			},
			wantErr: true,
			err:     ErrGreaterThanValidationFail,
		},
		{
			name: "greater than validation failed",
			config: map[string]string{
				"param1": "20",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationGreaterThan{10},
				}},
			},
			wantErr: false,
		},
		{
			name: "inclusion validation failed",
			config: map[string]string{
				"param1": "three",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationInclusion{[]string{"one", "two"}},
				}},
			},
			wantErr: true,
			err:     ErrInclusionValidationFail,
		},
		{
			name: "inclusion validation pass",
			config: map[string]string{
				"param1": "two",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationInclusion{[]string{"one", "two"}},
				}},
			},
			wantErr: false,
		},
		{
			name: "exclusion validation failed",
			config: map[string]string{
				"param1": "one",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationExclusion{[]string{"one", "two"}},
				}},
			},
			wantErr: true,
			err:     ErrExclusionValidationFail,
		},
		{
			name: "exclusion validation pass",
			config: map[string]string{
				"param1": "three",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationExclusion{[]string{"one", "two"}},
				}},
			},
			wantErr: false,
		},
		{
			name: "regex validation failed",
			config: map[string]string{
				"param1": "a-a",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationRegex{"[a-z]-[1-9]"},
				}},
			},
			wantErr: true,
			err:     ErrRegexValidationFail,
		},
		{
			name: "regex validation pass",
			config: map[string]string{
				"param1": "a-8",
			},
			params: map[string]Parameter{
				"param1": {Validations: []Validation{
					ValidationRegex{"[a-z]-[1-9]"},
				}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyConfigValidations(tt.params, tt.config)
			if err != nil && tt.wantErr {
				is.True(errors.Is(err, tt.err))
			} else if err != nil || tt.wantErr {
				t.Errorf("UtilityFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestValidation_toCPluginV1(t *testing.T) {
	is := is.New(t)
	validations := []Validation{
		ValidationRequired{},
		ValidationLessThan{5},
		ValidationGreaterThan{0},
		ValidationInclusion{[]string{"1", "2"}},
		ValidationExclusion{[]string{"3", "4"}},
		ValidationRegex{"[a-z]*"},
	}
	want := []cpluginv1.ParameterValidation{
		{
			Type:  cpluginv1.ValidationTypeRequired,
			Value: "",
		}, {
			Type:  cpluginv1.ValidationTypeLessThan,
			Value: "5.000000",
		}, {
			Type:  cpluginv1.ValidationTypeGreaterThan,
			Value: "0.000000",
		}, {
			Type:  cpluginv1.ValidationTypeInclusion,
			Value: "1,2",
		}, {
			Type:  cpluginv1.ValidationTypeExclusion,
			Value: "3,4",
		}, {
			Type:  cpluginv1.ValidationTypeRegex,
			Value: "[a-z]*",
		},
	}
	got := convertValidations(validations)
	is.Equal(got, want)
}

func TestValidation_Multi_Error(t *testing.T) {
	is := is.New(t)

	params := map[string]Parameter{
		"limit": {
			Type: ParameterTypeNumber,
			Validations: []Validation{
				ValidationGreaterThan{0},
				ValidationRegex{"^[0-9]"},
			}},
		"option": {
			Type: ParameterTypeString,
			Validations: []Validation{
				ValidationInclusion{[]string{"one", "two", "three"}},
				ValidationExclusion{[]string{"one", "five"}},
			}},
		"name": {
			Type: ParameterTypeString,
			Validations: []Validation{
				ValidationRequired{},
			}},
	}
	config := map[string]string{
		"limit":  "-1",
		"option": "five",
	}

	err := applyConfigValidations(params, config)
	is.True(err != nil)

	errs := multierr.Errors(err)
	counters := make([]int, len(errs))
	for i, err := range errs {
		switch {
		case errors.Is(err, ErrRequiredParameterMissing):
			// name is missing
			counters[i]++
		case errors.Is(err, ErrInclusionValidationFail):
			// option is not included in list
			counters[i]++
		case errors.Is(err, ErrExclusionValidationFail):
			// option is not excluded from list
			counters[i]++
		case errors.Is(err, ErrGreaterThanValidationFail):
			// limit is not greater than 0
			counters[i]++
		case errors.Is(err, ErrRegexValidationFail):
			// limit does not match the regex pattern
			counters[i]++
		}
	}
	// each one of these errors should occur once
	for _, counter := range counters {
		is.Equal(counter, 1)
	}
}
