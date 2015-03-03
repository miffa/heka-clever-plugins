package heka_clever_plugins

import (
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/pipelinemock"
	"github.com/rafrombrc/gomock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"time"
)

// MockFilterRunner
type MockFilterRunner struct {
	mock mock.Mock
	t    mock.TestingT
}

func (mfr *MockFilterRunner) EXPECT() *mock.Mock {
	return &mfr.mock
}

func (mfr *MockFilterRunner) ExpectCall(description string, method string, predicate func([]interface{}) bool) {
	foundMatchingCall := false
	for _, call := range mfr.mock.Calls {
		if call.Method == method && predicate(call.Arguments) {
			foundMatchingCall = true
			break
		}
	}
	assert.True(mfr.t, foundMatchingCall, "No matching call found for call '%s'", description)
}

func (mfr *MockFilterRunner) InChan() chan *pipeline.PipelinePack {
	args := mfr.mock.Called()
	return args.Get(0).(chan *pipeline.PipelinePack)
}

func (mfr *MockFilterRunner) Filter() pipeline.Filter {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) Start(h pipeline.PluginHelper, wg *sync.WaitGroup) error {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) Ticker() (ticker <-chan time.Time) {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) Inject(pack *pipeline.PipelinePack) bool {
	args := mfr.mock.Called(pack)
	return args.Bool(0)
}

func (mfr *MockFilterRunner) MatchRunner() *pipeline.MatchRunner {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) RetainPack(pack *pipeline.PipelinePack) {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) Name() string {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) SetName(name string) {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) Plugin() pipeline.Plugin {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) LogError(err error) {
	mfr.mock.Called(err)
}

func (mfr *MockFilterRunner) LogMessage(msg string) {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) SetLeakCount(count int) {
	panic("Method not implemented")
}

func (mfr *MockFilterRunner) LeakCount() int {
	panic("Method not implemented")
}

// FilterTestHelper
type FilterTestHelper struct {
	MockHelper       *pipelinemock.MockPluginHelper
	MockFilterRunner *MockFilterRunner
}

func NewFilterTestHelper(t mock.TestingT, ctrl *gomock.Controller) *FilterTestHelper {
	fth := new(FilterTestHelper)
	fth.MockHelper = pipelinemock.NewMockPluginHelper(ctrl)
	fth.MockFilterRunner = &MockFilterRunner{mock.Mock{}, t}
	return fth
}
