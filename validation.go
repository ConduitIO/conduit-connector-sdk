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
	"regexp"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
)

var (
	ErrUnrecognizedParameter    = config.ErrUnrecognizedParameter
	ErrInvalidParameterValue    = config.ErrInvalidParameterValue
	ErrInvalidParameterType     = config.ErrInvalidParameterType
	ErrInvalidValidationType    = config.ErrInvalidValidationType
	ErrRequiredParameterMissing = config.ErrRequiredParameterMissing

	ErrLessThanValidationFail    = config.ErrLessThanValidationFail
	ErrGreaterThanValidationFail = config.ErrGreaterThanValidationFail
	ErrInclusionValidationFail   = config.ErrInclusionValidationFail
	ErrExclusionValidationFail   = config.ErrExclusionValidationFail
	ErrRegexValidationFail       = config.ErrRegexValidationFail
)

const (
	ParameterTypeString ParameterType = iota + 1
	ParameterTypeInt
	ParameterTypeFloat
	ParameterTypeBool
	ParameterTypeFile
	ParameterTypeDuration
)

type ParameterType config.ParameterType

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(ParameterTypeString)-int(cpluginv1.ParameterTypeString)]
	_ = cTypes[int(ParameterTypeInt)-int(cpluginv1.ParameterTypeInt)]
	_ = cTypes[int(ParameterTypeFloat)-int(cpluginv1.ParameterTypeFloat)]
	_ = cTypes[int(ParameterTypeBool)-int(cpluginv1.ParameterTypeBool)]
	_ = cTypes[int(ParameterTypeFile)-int(cpluginv1.ParameterTypeFile)]
	_ = cTypes[int(ParameterTypeDuration)-int(cpluginv1.ParameterTypeDuration)]

	_ = cTypes[int(ParameterTypeString)-int(config.ParameterTypeString)]
	_ = cTypes[int(ParameterTypeInt)-int(config.ParameterTypeInt)]
	_ = cTypes[int(ParameterTypeFloat)-int(config.ParameterTypeFloat)]
	_ = cTypes[int(ParameterTypeBool)-int(config.ParameterTypeBool)]
	_ = cTypes[int(ParameterTypeFile)-int(config.ParameterTypeFile)]
	_ = cTypes[int(ParameterTypeDuration)-int(config.ParameterTypeDuration)]

	_ = cTypes[int(config.ValidationTypeRequired)-int(cpluginv1.ValidationTypeRequired)]
	_ = cTypes[int(config.ValidationTypeGreaterThan)-int(cpluginv1.ValidationTypeGreaterThan)]
	_ = cTypes[int(config.ValidationTypeLessThan)-int(cpluginv1.ValidationTypeLessThan)]
	_ = cTypes[int(config.ValidationTypeInclusion)-int(cpluginv1.ValidationTypeInclusion)]
	_ = cTypes[int(config.ValidationTypeExclusion)-int(cpluginv1.ValidationTypeExclusion)]
	_ = cTypes[int(config.ValidationTypeRegex)-int(cpluginv1.ValidationTypeRegex)]
}

type Validation interface {
	configValidation() config.Validation
}

type ValidationRequired struct{}

func (v ValidationRequired) configValidation() config.Validation {
	return config.ValidationRequired(v)
}

type ValidationLessThan struct {
	Value float64
}

func (v ValidationLessThan) configValidation() config.Validation {
	return config.ValidationLessThan{V: v.Value}
}

type ValidationGreaterThan struct {
	Value float64
}

func (v ValidationGreaterThan) configValidation() config.Validation {
	return config.ValidationGreaterThan{V: v.Value}
}

type ValidationInclusion struct {
	List []string
}

func (v ValidationInclusion) configValidation() config.Validation {
	return config.ValidationInclusion{List: v.List}
}

type ValidationExclusion struct {
	List []string
}

func (v ValidationExclusion) configValidation() config.Validation {
	return config.ValidationExclusion{List: v.List}
}

type ValidationRegex struct {
	Regex *regexp.Regexp
}

func (v ValidationRegex) configValidation() config.Validation {
	return config.ValidationRegex{Regex: v.Regex}
}

func convertValidations(validations []Validation) []cpluginv1.ParameterValidation {
	if validations == nil {
		return nil
	}
	out := make([]cpluginv1.ParameterValidation, len(validations))
	for i, v := range validations {
		val := v.configValidation()
		out[i] = cpluginv1.ParameterValidation{
			Type:  cpluginv1.ValidationType(val.Type()),
			Value: val.Value(),
		}
	}
	return out
}
