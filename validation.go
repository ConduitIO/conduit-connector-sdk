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
	"strings"

	"github.com/conduitio/conduit-connector-protocol/cpluginv1"
)

const (
	ValidationTypeRequired ValidationType = iota + 1
	ValidationTypeGreaterThan
	ValidationTypeLessThan
	ValidationTypeInclusion
	ValidationTypeExclusion
	ValidationTypeRegex
)
const (
	ParameterTypeString ParameterType = iota + 1
	ParameterTypeNumber
	ParameterTypeBool
	ParameterTypeFile
	ParameterTypeDuration
)

type ValidationType int
type ParameterType int

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(ValidationTypeRequired)-int(cpluginv1.ValidationTypeRequired)]
	_ = cTypes[int(ValidationTypeGreaterThan)-int(cpluginv1.ValidationTypeGreaterThan)]
	_ = cTypes[int(ValidationTypeLessThan)-int(cpluginv1.ValidationTypeLessThan)]
	_ = cTypes[int(ValidationTypeInclusion)-int(cpluginv1.ValidationTypeInclusion)]
	_ = cTypes[int(ValidationTypeExclusion)-int(cpluginv1.ValidationTypeExclusion)]
	_ = cTypes[int(ValidationTypeRegex)-int(cpluginv1.ValidationTypeRegex)]

	_ = cTypes[int(ParameterTypeString)-int(cpluginv1.ParameterTypeString)]
	_ = cTypes[int(ParameterTypeNumber)-int(cpluginv1.ParameterTypeNumber)]
	_ = cTypes[int(ParameterTypeBool)-int(cpluginv1.ParameterTypeBool)]
	_ = cTypes[int(ParameterTypeFile)-int(cpluginv1.ParameterTypeFile)]
	_ = cTypes[int(ParameterTypeDuration)-int(cpluginv1.ParameterTypeDuration)]
}

type Validation interface {
	validate(key string, value string) error
	vType() ValidationType
	value() string
}

type ValidationRequired struct {
}

func (v ValidationRequired) validate(key string, value string) error {
	// TBD
	return nil
}

func (v ValidationRequired) vType() ValidationType {
	return ValidationTypeRequired
}

func (v ValidationRequired) value() string {
	return ""
}

type ValidationLessThan struct {
	Value float64
}

func (v ValidationLessThan) validate(key string, value string) error {
	// TBD
	return nil
}

func (v ValidationLessThan) vType() ValidationType {
	return ValidationTypeLessThan
}

func (v ValidationLessThan) value() string {
	return fmt.Sprintf("%f", v.Value)
}

type ValidationGreaterThan struct {
	Value float64
}

func (v ValidationGreaterThan) validate(key string, value string) error {
	// TBD
	return nil
}

func (v ValidationGreaterThan) vType() ValidationType {
	return ValidationTypeGreaterThan
}

func (v ValidationGreaterThan) value() string {
	return fmt.Sprintf("%f", v.Value)
}

type ValidationInclusion struct {
	List []string
}

func (v ValidationInclusion) validate(key string, value string) error {
	// TBD
	return nil
}

func (v ValidationInclusion) vType() ValidationType {
	return ValidationTypeInclusion
}

func (v ValidationInclusion) value() string {
	return strings.Join(v.List, ", ")
}

type ValidationExclusion struct {
	List []string
}

func (v ValidationExclusion) validate(key string, value string) error {
	// TBD
	return nil
}

func (v ValidationExclusion) vType() ValidationType {
	return ValidationTypeExclusion
}

func (v ValidationExclusion) value() string {
	return strings.Join(v.List, ", ")
}

type ValidationRegex struct {
	Pattern string
}

func (v ValidationRegex) validate(key string, value string) error {
	// TBD
	return nil
}

func (v ValidationRegex) vType() ValidationType {
	return ValidationTypeRegex
}

func (v ValidationRegex) value() string {
	return v.Pattern
}

func ConvertValidations(validations []Validation) []cpluginv1.ParameterValidation {
	out := make([]cpluginv1.ParameterValidation, len(validations))
	for i, v := range validations {
		out[i] = cpluginv1.ParameterValidation{
			Type:  cpluginv1.ValidationType(v.vType()),
			Value: v.value(),
		}
	}
	return out
}
