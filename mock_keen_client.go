package heka_clever_plugins

import (
	"github.com/stretchr/testify/mock"
)

func NewMockKeenClient() MockKeenClient {
	return MockKeenClient{mock.Mock{}}
}

type MockKeenClient struct {
	mock mock.Mock
}

func (mkc *MockKeenClient) AddEvent(collection string, event interface{}) error {
	args := mkc.mock.Called(collection, event)
	return args.Error(0)
}

func (mkc *MockKeenClient) EXPECT() *mock.Mock {
	return &mkc.mock
}
