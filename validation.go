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
	_ = cTypes[int(ParameterTypeString)-int(cpluginv1.ParameterTypeString)]
	_ = cTypes[int(ParameterTypeNumber)-int(cpluginv1.ParameterTypeNumber)]
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
	// TBD
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
	// TBD
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
	// TBD
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
	// TBD
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
	// TBD
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
	// TBD
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
