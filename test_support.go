package heka_clever_plugins

import (
	"code.google.com/p/gomock/gomock"
	"github.com/mozilla-services/heka/pipelinemock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type FilterOutputHelper struct {
	MockHelper       *pipelinemock.MockPluginHelper
	MockOutputRunner *MockOutputRunner
}

func NewOutputTestHelper(t mock.TestingT, ctrl *gomock.Controller) *FilterOutputHelper {
	fth := new(FilterOutputHelper)
	fth.MockHelper = pipelinemock.NewMockPluginHelper(ctrl)
	fth.MockOutputRunner = &MockOutputRunner{mock.Mock{}, t}
	return fth
}

func ExpectCall(t *testing.T, mock *mock.Mock, description string, method string,
	predicate func([]interface{}) bool) {
	foundMatchingCall := false
	for _, call := range mock.Calls {
		if call.Method == method && predicate(call.Arguments) {
			foundMatchingCall = true
			break
		}
	}
	assert.True(t, foundMatchingCall, "No matching call found for call '%s'", description)
}
