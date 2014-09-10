package heka_clever_plugins

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestSpec struct {
	input    string
	expected interface{}
}

// Test_ParseTitleAndKeyvals_Errors - verifies that message and (optional) key-values are extracted
func Test_ParseTitleAndKeyvals(t *testing.T) {
	specs := []TestSpec{
		TestSpec{"MSG", []string{"MSG", `{}`}},
		TestSpec{"MSG a=b", []string{"MSG", `{"a":"b"}`}},
		TestSpec{"MSG a=b  c=d ", []string{"MSG", `{"a":"b","c":"d"}`}},
	}
	for _, spec := range specs {
		actualTitle, actualJsonString, err := ParseTitleAndKeyvals(spec.input)
		assert.NoError(t, err)
		assert.Equal(t, spec.expected.([]string)[0], actualTitle)
		assert.Equal(t, spec.expected.([]string)[1], actualJsonString)
	}
}

// Test_ParseTitleAndKeyvals_Errors - verifies that errors are passed up
func Test_ParseTitleAndKeyvals_Errors(t *testing.T) {
	inputs := []string{
		"MSG ===",
	}
	for _, input := range inputs {
		actualTitle, actualJsonString, err := ParseTitleAndKeyvals(input)
		assert.Error(t, err)
		assert.Equal(t, "", actualTitle, "shouldn't have pulled a title, but instead got '%s'", actualTitle)
		assert.Equal(t, "", actualJsonString, "shouldn't have pulled a jsonString, but instead got '%s'", actualJsonString)
	}
}

// Test_keyvalToJsonString - verifies key-values are extracted, equal signs are handled, and extra whitespace is ignored.
func Test_keyvalToJsonString(t *testing.T) {
	specs := []TestSpec{
		TestSpec{"", `{}`},
		TestSpec{"  ", `{}`},
		TestSpec{"x=y", `{"x":"y"}`},
		TestSpec{" x=y ", `{"x":"y"}`},
		TestSpec{"x=y a=b", `{"a":"b","x":"y"}`},
		TestSpec{"a=b x=y", `{"a":"b","x":"y"}`},
		TestSpec{"    x=y      a=b   ", `{"a":"b","x":"y"}`},
		TestSpec{"  x=y   a=b  b==a   ", `{"a":"b","b":"=a","x":"y"}`},
		TestSpec{`  x="y z"  a=b  b==a   `, `{"a":"b","b":"=a","x":"y z"}`},
	}
	for _, spec := range specs {
		actual, err := keyvalToJsonString(spec.input)
		assert.NoError(t, err)
		assert.Equal(t, spec.expected, actual)
	}
}

// Test_keyvalToJsonString_Errors - verifies scenarios where key-values should fail to be extracted.
func Test_keyvalToJsonString_Errors(t *testing.T) {
	inputs := []string{
		"====",
		" a ",
		" a=b c  ",
	}
	for _, input := range inputs {
		actual, err := keyvalToJsonString(input)
		assert.Error(t, err)
		assert.Equal(t, "", actual, "shouldn't have pulled a keyval, but instead got '%s'", actual)
	}
}

// Test_keyvalToJsonString_Errors - verifies helper function that pulls first key=val from a string
func Test_findKeyValPair(t *testing.T) {
	k, v, rest, err := findKeyValPair("a=b =c=d")
	assert.NoError(t, err)
	assert.Equal(t, "a", k, "wrong key")
	assert.Equal(t, "b", v, "wrong val")
	assert.Equal(t, " =c=d", rest, "wrong rest of string")
}
