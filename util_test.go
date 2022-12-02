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
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestParseConfig_Simple_Struct(t *testing.T) {
	is := is.New(t)

	type Person struct {
		Name string `mapstructure:"person_name"`
		Age  int
	}

	input := map[string]string{
		"person_name": "meroxa",
		"age":         "91",
	}
	want := Person{
		Name: "meroxa",
		Age:  91,
	}

	var got Person
	err := Util.ParseConfig(input, &got)
	is.NoErr(err)
	is.Equal(want, got)
}

func TestParseConfig_Embedded_Struct(t *testing.T) {
	is := is.New(t)

	type Family struct {
		LastName string
	}
	type Location struct {
		City string
	}
	type Person struct {
		Family    `mapstructure:",squash"`
		Location  `mapstructure:",squash"`
		FirstName string
	}

	input := map[string]string{
		"FirstName": "conduit",
		"lastname":  "meroxa",
		"City":      "San Francisco",
	}
	want := Person{
		Family:    Family{LastName: "meroxa"},
		Location:  Location{City: "San Francisco"},
		FirstName: "conduit",
	}

	var got Person
	err := Util.ParseConfig(input, &got)
	is.NoErr(err)
	is.Equal(want, got)
}

func TestParseConfig_All_Types(t *testing.T) {
	is := is.New(t)
	type Config struct {
		MyString      string
		MyBool1       bool
		MyBool2       bool
		MyBool3       bool
		MyBoolDefault bool

		MyInt    int
		MyUint   uint
		MyInt8   int8
		MyUint8  uint8
		MyInt16  int16
		MyUint16 uint16
		MyInt32  int32
		MyUint32 uint32
		MyInt64  int64
		MyUint64 uint64

		MyByte byte
		MyRune rune

		MyFloat32 float32
		MyFloat64 float64

		MyDuration        time.Duration
		MyDurationDefault time.Duration

		MySlice      []string
		MyIntSlice   []int
		MyFloatSlice []float32
	}

	input := map[string]string{
		"mystring": "string",
		"mybool1":  "t",
		"mybool2":  "true",
		"mybool3":  "1", // 1 is true
		"myInt":    "-1",
		"myuint":   "1",
		"myint8":   "-1",
		"myuint8":  "1",
		"myInt16":  "-1",
		"myUint16": "1",
		"myint32":  "-1",
		"myuint32": "1",
		"myint64":  "-1",
		"myuint64": "1",

		"mybyte": "99", // 99 fits in one byte
		"myrune": "4567",

		"myfloat32": "1.1122334455",
		"myfloat64": "1.1122334455",

		"myduration": "1s",

		"myslice":      "1,2,3,4",
		"myIntSlice":   "1,2,3,4",
		"myFloatSlice": "1.1,2.2",
	}
	want := Config{
		MyString:          "string",
		MyBool1:           true,
		MyBool2:           true,
		MyBool3:           true,
		MyBoolDefault:     false, // default
		MyInt:             -1,
		MyUint:            0x1,
		MyInt8:            -1,
		MyUint8:           0x1,
		MyInt16:           -1,
		MyUint16:          0x1,
		MyInt32:           -1,
		MyUint32:          0x1,
		MyInt64:           -1,
		MyUint64:          0x1,
		MyByte:            0x63,
		MyRune:            4567,
		MyFloat32:         1.1122334,
		MyFloat64:         1.1122334455,
		MyDuration:        1000000000,
		MyDurationDefault: 0,
		MySlice:           []string{"1", "2", "3", "4"},
		MyIntSlice:        []int{1, 2, 3, 4},
		MyFloatSlice:      []float32{1.1, 2.2},
	}

	var result Config
	err := Util.ParseConfig(input, &result)
	is.NoErr(err)
	is.Equal(want, result)
}
