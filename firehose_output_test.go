package heka_clever_plugins

import (
	"testing"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/pipelinemock"
	"github.com/rafrombrc/gomock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestRun(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockOR := pipelinemock.NewMockOutputRunner(mockCtrl)
	mockPH := pipelinemock.NewMockPluginHelper(mockCtrl)
	mockFirehose := NewMockRecordPutter(mockCtrl)

	firehoseOutput := FirehoseOutput{
		client: mockFirehose,
	}

	testChan := make(chan *pipeline.PipelinePack)
	mockOR.EXPECT().InChan().Return(testChan)

	// Send test input through the channel
	input := `{"key":"value"}`
	go func() {
		testPack := pipeline.PipelinePack{
			Message: &message.Message{
				Payload: &input,
			},
		}

		testChan <- &testPack
		close(testChan)
	}()

	mockFirehose.EXPECT().PutRecord([]byte(input)).Return(nil)

	err := firehoseOutput.Run(mockOR, mockPH)
	assert.NoError(t, err, "did not expect err for valid Run()")
}
