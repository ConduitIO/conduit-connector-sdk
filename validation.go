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
	if value == "" {
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
		formatted := strconv.FormatFloat(v.Value, 'f', -1, 64)
		return fmt.Errorf("%q should be less than %s: %w", value, formatted, ErrLessThanValidationFail)
	}
	return nil
}

func (v ValidationLessThan) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeLessThan,
		Value: strconv.FormatFloat(v.Value, 'f', -1, 64),
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
		formatted := strconv.FormatFloat(v.Value, 'f', -1, 64)
		return fmt.Errorf("%q should be greater than %s: %w", value, formatted, ErrGreaterThanValidationFail)
	}
	return nil
}

func (v ValidationGreaterThan) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeGreaterThan,
		Value: strconv.FormatFloat(v.Value, 'f', -1, 64),
	}
}

type ValidationInclusion struct {
	List []string
}

func (v ValidationInclusion) validate(value string) error {
	if !slices.Contains(v.List, value) {
		return fmt.Errorf("%q value must be included in the list [%s]: %w", value, strings.Join(v.List, ","), ErrInclusionValidationFail)
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
		return fmt.Errorf("%q value must be excluded from the list [%s]: %w", value, strings.Join(v.List, ","), ErrExclusionValidationFail)
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
	Regex *regexp.Regexp
}

func (v ValidationRegex) validate(value string) error {
	if !v.Regex.MatchString(value) {
		return fmt.Errorf("%q should match the regex %q: %w", value, v.Regex.String(), ErrRegexValidationFail)
	}

	return nil
}

func (v ValidationRegex) toCPluginV1() cpluginv1.ParameterValidation {
	return cpluginv1.ParameterValidation{
		Type:  cpluginv1.ValidationTypeRegex,
		Value: v.Regex.String(),
	}
}

func convertValidations(validations []Validation) []cpluginv1.ParameterValidation {
	out := make([]cpluginv1.ParameterValidation, len(validations))
	for i, v := range validations {
		out[i] = v.toCPluginV1()
	}
	return out
}

// private struct to group all validation functions
type validator map[string]Parameter

// Validate is a utility function that applies all the validations for parameters, returns an error that
// consists of a combination of errors from the configurations.
func (v validator) Validate(config map[string]string) error {
	// cfg is the config map with default values assigned
	cfg, multiErr := v.initConfig(config)

	var err error
	for pKey := range v {
		err = v.validateParamType(pKey, cfg[pKey])
		if err != nil {
			// append error and continue with next parameter
			multiErr = multierr.Append(multiErr, err)
			continue
		}
		multiErr = multierr.Append(multiErr, v.validateParamValue(pKey, cfg[pKey]))
	}
	return multiErr
}

// validateParamValue validates that a configuration value matches all the validations required for the parameter.
func (v validator) validateParamValue(key string, value string) error {
	var multiErr error

	isRequired := false
	for _, v := range v[key].Validations {
		if _, ok := v.(ValidationRequired); ok {
			isRequired = true
		}
		err := v.validate(value)
		if err != nil {
			multiErr = multierr.Append(multiErr, fmt.Errorf("error validating %q: %w", key, err))
		}
	}
	if value == "" && !isRequired {
		return nil // empty otional parameter is valid
	}

	return multiErr
}

// initConfig checks for unrecognized params, and fills any empty configuration with its assigned default value
// returns an error if a parameter is not recognized.
func (v validator) initConfig(config map[string]string) (map[string]string, error) {
	output := make(map[string]string)
	var multiErr error
	for key, val := range config {
		// trim key and val
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		// check for unrecognized params
		if _, ok := v[key]; !ok {
			multiErr = multierr.Append(multiErr, fmt.Errorf("unrecognized parameter %q", key))
			continue
		}
		// set config map
		output[key] = val
	}
	// assign defaults
	for key, param := range v {
		if output[key] == "" {
			output[key] = param.Default
		}
	}
	return output, multiErr
}

// validateParamType validates that a parameter value is parsable to its assigned type.
func (v validator) validateParamType(key string, value string) error {
	// empty value is valid for all types
	if value == "" {
		return nil
	}
	switch v[key].Type {
	case ParameterTypeInt:
		_, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("error validating %q: %q value is not an integer: %w", key, value, ErrInvalidParamType)
		}
	case ParameterTypeFloat:
		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("error validating %q: %q value is not a float: %w", key, value, ErrInvalidParamType)
		}
	case ParameterTypeDuration:
		_, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("error validating %q: %q value is not a duration: %w", key, value, ErrInvalidParamType)
		}
	case ParameterTypeBool:
		_, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("error validating %q: %q value is not a boolean: %w", key, value, ErrInvalidParamType)
		}
		// type ParameterTypeFile and ParameterTypeString don't need type validations, since a file is an array of bytes,
		// which is a string, and all the configuration values are strings
	}
	return nil
}
