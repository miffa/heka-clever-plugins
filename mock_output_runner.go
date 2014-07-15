package heka_clever_plugins

import (
	"github.com/mozilla-services/heka/pipeline"
	"github.com/stretchr/testify/mock"
	"sync"
	"time"
)

type MockOutputRunner struct {
	mock mock.Mock
	t    mock.TestingT
}

func (mor *MockOutputRunner) EXPECT() *mock.Mock {
	return &mor.mock
}

func (mor *MockOutputRunner) InChan() chan *pipeline.PipelinePack {
	args := mor.mock.Called()
	return args.Get(0).(chan *pipeline.PipelinePack)
}

func (mor *MockOutputRunner) Output() pipeline.Output {
	panic("Method not implemented")
	return nil
}

func (mor *MockOutputRunner) Start(h pipeline.PluginHelper, wg *sync.WaitGroup) error {
	panic("Method not implemented")
	return nil
}

func (mor *MockOutputRunner) Ticker() (ticker <-chan time.Time) {
	panic("Method not implemented")
	return nil
}

func (mor *MockOutputRunner) RetainPack(pack *pipeline.PipelinePack) {
	panic("Method not implemented")
}

func (mor *MockOutputRunner) MatchRunner() *pipeline.MatchRunner {
	panic("Method not implemented")
	return nil
}

func (mor *MockOutputRunner) Encoder() pipeline.Encoder {
	panic("Method not implemented")
	return nil
}

func (mor *MockOutputRunner) Encode(pack *pipeline.PipelinePack) ([]byte, error) {
	panic("Method not implemented")
	return nil, nil
}

func (mor *MockOutputRunner) UsesFraming() bool {
	panic("Method not implemented")
	return false
}

func (mor *MockOutputRunner) SetUseFraming(useFraming bool) {
	panic("Method not implemented")
}

func (mor *MockOutputRunner) Name() string {
	panic("Method not implemented")
	return ""
}

func (mor *MockOutputRunner) SetName(name string) {
	panic("Method not implemented")
}

func (mor *MockOutputRunner) Plugin() pipeline.Plugin {
	panic("Method not implemented")
	return nil
}

func (mor *MockOutputRunner) LogError(err error) {
	mor.mock.Called(err)
}

func (mor *MockOutputRunner) LogMessage(msg string) {
	panic("Method not implemented")
}

func (mor *MockOutputRunner) PluginGlobals() *pipeline.PluginGlobals {
	panic("Method not implemented")
	return nil
}

func (mor *MockOutputRunner) SetLeakCount(count int) {
	panic("Method not implemented")
}

func (mor *MockOutputRunner) LeakCount() int {
	panic("Method not implemented")
}
