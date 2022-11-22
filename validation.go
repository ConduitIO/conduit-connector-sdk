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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"
)

var (
	ErrInvalidParamValue         = errors.New("invalid parameter value")
	ErrInvalidParamType          = errors.New("invalid parameter type")
	ErrRequiredParameterMissing  = errors.New("required parameter is not provided")
	ErrLessThanValidationFail    = errors.New("less-than validation failed")
	ErrGreaterThanValidationFail = errors.New("greater-than validation failed")
	ErrInclusionValidationFail   = errors.New("inclusion validation failed")
	ErrExclusionValidationFail   = errors.New("exclusion validation failed")
	ErrRegexValidationFail       = errors.New("regex validation failed")
)

const (
	ParameterTypeString ParameterType = iota + 1
	ParameterTypeInt
	ParameterTypeFloat
	ParameterTypeBool
	ParameterTypeFile
	ParameterTypeDuration
)

type ParameterType int

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(ParameterTypeString)-int(cpluginv1.ParameterTypeString)]
	_ = cTypes[int(ParameterTypeInt)-int(cpluginv1.ParameterTypeInt)]
	_ = cTypes[int(ParameterTypeFloat)-int(cpluginv1.ParameterTypeFloat)]
	_ = cTypes[int(ParameterTypeBool)-int(cpluginv1.ParameterTypeBool)]
	_ = cTypes[int(ParameterTypeFile)-int(cpluginv1.ParameterTypeFile)]
	_ = cTypes[int(ParameterTypeDuration)-int(cpluginv1.ParameterTypeDuration)]
}

type Validation interface {
	validate(value string) error
	toCPluginV1() cpluginv1.ParameterValidation
}

type ValidationRequired struct {
}

func (v ValidationRequired) validate(value string) error {
	if len(strings.TrimSpace(value)) == 0 {
		return ErrRequiredParameterMissing
	}
	return nil
}

func (v ValidationRequired) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeRequired,
		Value: "",
	}
}

type ValidationLessThan struct {
	Value float64
}

func (v ValidationLessThan) validate(value string) error {
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%q value should be a number: %w", value, ErrInvalidParamValue)
	}
	if !(val < v.Value) {
		return fmt.Errorf("%q should be less than %.4f: %w", value, v.Value, ErrLessThanValidationFail)
	}
	return nil
}

func (v ValidationLessThan) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeLessThan,
		Value: fmt.Sprintf("%f", v.Value),
	}
}

type ValidationGreaterThan struct {
	Value float64
}

func (v ValidationGreaterThan) validate(value string) error {
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%q value should be a number: %w", value, ErrInvalidParamValue)
	}
	if !(val > v.Value) {
		return fmt.Errorf("%q should be greater than %.4f: %w", value, v.Value, ErrGreaterThanValidationFail)
	}
	return nil
}

func (v ValidationGreaterThan) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeGreaterThan,
		Value: fmt.Sprintf("%f", v.Value),
	}
}

type ValidationInclusion struct {
	List []string
}

func (v ValidationInclusion) validate(value string) error {
	if !slices.Contains(v.List, value) {
		return fmt.Errorf("%q value must be included in the list {%s}: %w", value, strings.Join(v.List, ","), ErrInclusionValidationFail)
	}
	return nil
}

func (v ValidationInclusion) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeInclusion,
		Value: strings.Join(v.List, ","),
	}
}

type ValidationExclusion struct {
	List []string
}

func (v ValidationExclusion) validate(value string) error {
	if slices.Contains(v.List, value) {
		return fmt.Errorf("%q value must be excluded from the list {%s}: %w", value, strings.Join(v.List, ","), ErrExclusionValidationFail)
	}
	return nil
}

func (v ValidationExclusion) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeExclusion,
		Value: strings.Join(v.List, ","),
	}
}

type ValidationRegex struct {
	Pattern string
}

func (v ValidationRegex) validate(value string) error {
	r, err := regexp.Compile(v.Pattern)
	if err != nil {
		return fmt.Errorf("regex pattern compilation failed: %w", ErrInvalidParamValue)
	}
	if !r.MatchString(value) {
		return fmt.Errorf("%q should match the regex %q: %w", value, v.Pattern, ErrRegexValidationFail)
	}

	return nil
}

func (v ValidationRegex) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeRegex,
		Value: v.Pattern,
	}
}

func convertValidations(validations []Validation) []cpluginv1.ParameterValidation {
	out := make([]cpluginv1.ParameterValidation, len(validations))
	for i, v := range validations {
		out[i] = v.toCPluginV1()
	}
	return out
}

// ApplyConfigValidations a utility function that applies all the validations for parameters, return an error that
// consists of a combination of errors from the configurations.
func applyConfigValidations(params map[string]Parameter, config map[string]string) error {
	multiErr := assignParamDefaults(params, config)

	for pKey, param := range params {
		if len(strings.TrimSpace(config[pKey])) != 0 {
			multiErr = multierr.Append(multiErr, validateParamType(param, pKey, config[pKey]))
		}
		multiErr = multierr.Append(multiErr, validateParamValue(pKey, config[pKey], param))
	}
	return multiErr
}

// validateParamValue validates that a configuration value matches all the validations required for the parameter.
func validateParamValue(key string, value string, param Parameter) error {
	var multiErr error
	var err error

	for _, v := range param.Validations {
		err = v.validate(value)
		if err != nil {
			multiErr = multierr.Append(multiErr, fmt.Errorf("error validating %q: %w", key, err))
		}
	}
	return multiErr
}

// assignParamDefaults fills any empty configuration with its assigned default value
// returns an error if a parameter is not recognized.
func assignParamDefaults(params map[string]Parameter, config map[string]string) error {
	var multiErr error
	for key, val := range config {
		if _, ok := params[key]; !ok {
			multiErr = multierr.Append(multiErr, fmt.Errorf("unrecognized parameter %q", key))
		}
		if len(strings.TrimSpace(val)) == 0 {
			config[key] = params[key].Default
		}
	}
	return multiErr
}

// validateParamType validates that a parameter value is parsable to its assigned type.
func validateParamType(p Parameter, key string, value string) error {
	switch p.Type {
	case ParameterTypeInt:
		_, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("%q: %q value is not an integer: %w", key, value, ErrInvalidParamType)
		}
	case ParameterTypeFloat:
		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("%q: %q value is not a float: %w", key, value, ErrInvalidParamType)
		}
	case ParameterTypeDuration:
		_, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("%q: %q value is not a duration: %w", key, value, ErrInvalidParamType)
		}
	case ParameterTypeBool:
		_, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("%q: %q value is not a boolean: %w", key, value, ErrInvalidParamType)
		}
		// type ParameterTypeFile and ParameterTypeString don't need type validations, since a file is an array of bytes,
		// which is a string, and all the configuration values are strings
	}
	return nil
}
