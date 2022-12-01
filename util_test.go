package sdk

import (
	"testing"

	"github.com/matryer/is"
)

func TestParseConfig_Simple_Struct(t *testing.T) {
	is := is.New(t)

	type Person struct {
		Name   string `mapstructure:"person_name"`
		Age    int
		Emails []string
		Extra  map[string]string
	}

	input := map[string]interface{}{
		"person_name": "meroxa",
		"age":         91,
		"emails":      []string{"one", "two", "three"},
		"extra": map[string]string{
			"string": "value",
		},
	}
	want := Person{
		Name:   "meroxa",
		Age:    91,
		Emails: []string{"one", "two", "three"},
		Extra:  map[string]string{"string": "value"},
	}

	var got Person
	err := parseConfig(input, &got)
	is.NoErr(err)
	is.Equal(want, got)
}

func Test_ParseConfig_Embedded_Struct(t *testing.T) {
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

	input := map[string]interface{}{
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
	err := parseConfig(input, &got)
	is.NoErr(err)
	is.Equal(want, got)
}
