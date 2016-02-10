package heka_clever_plugins

import (
	"testing"
	"time"
	"errors"

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

// TestRunWithTimestamp tests that if a TimestampColumn is provided in the config
// then the Heka message's timestamp gets added to the message with that column name.
func TestRunWithTimestamp(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockOR := pipelinemock.NewMockOutputRunner(mockCtrl)
	mockPH := pipelinemock.NewMockPluginHelper(mockCtrl)
	mockFirehose := NewMockRecordPutter(mockCtrl)

	firehoseOutput := FirehoseOutput{
		client:          mockFirehose,
		timestampColumn: "created",
	}

	testChan := make(chan *pipeline.PipelinePack)
	mockOR.EXPECT().InChan().Return(testChan)

	// Send test input through the channel
	input := `{"key": "value"}`
	timestamp := time.Date(2015, 07, 1, 13, 14, 15, 0, time.UTC).UnixNano()
	go func() {
		testPack := pipeline.PipelinePack{
			Message: &message.Message{
				Payload:   &input,
				Timestamp: &timestamp,
			},
		}

		testChan <- &testPack
		close(testChan)
	}()

	expected := `{"created":"2015-07-01 13:14:15.000","key":"value"}`
	mockFirehose.EXPECT().PutRecord([]byte(expected)).Return(nil)

	err := firehoseOutput.Run(mockOR, mockPH)
	assert.NoError(t, err, "did not expect err for valid Run()")
}

// TestRunWithoutData tests that if an empty row is passed to the plugin
// then the plugin returns an error but continues
func TestRunWithoutData(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockOR := pipelinemock.NewMockOutputRunner(mockCtrl)
	mockPH := pipelinemock.NewMockPluginHelper(mockCtrl)
	mockFirehose := NewMockRecordPutter(mockCtrl)

	firehoseOutput := FirehoseOutput{
		client:          mockFirehose,
		timestampColumn: "created",
	}

	testChan := make(chan *pipeline.PipelinePack)
	mockOR.EXPECT().InChan().Return(testChan)

	// Send test input through the channel
	input := `{}`
	timestamp := time.Date(2015, 07, 1, 13, 14, 15, 0, time.UTC).UnixNano()
	go func() {
		testPack := pipeline.PipelinePack{
			Message: &message.Message{
				Payload:   &input,
				Timestamp: &timestamp,
			},
		}

		testChan <- &testPack
		close(testChan)
	}()

	expected_error := errors.New("No fields found in message")
	mockOR.EXPECT().LogError(expected_error).Return()

	err := firehoseOutput.Run(mockOR, mockPH)
	assert.NoError(t, err, "don't fail Run() for no fields found")
}
