package heka_clever_plugins

import (
	"runtime"
	"testing"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/pipelinemock"
	"github.com/mozilla-services/heka/plugins"
	"github.com/rafrombrc/gomock/gomock"
	"github.com/stretchr/testify/assert"
)

// TestEncoder tests that messages get encoded
func TestEncoder(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockOR := pipelinemock.NewMockOutputRunner(mockCtrl)
	mockPH := pipelinemock.NewMockPluginHelper(mockCtrl)
	mockClient := NewMockRecordPutter(mockCtrl)
	conf := FirehoseOutputConfig{
		FlushCount:    1,
		FlushInterval: 0,
	}

	firehoseOutput := new(FirehoseOutput)
	err := firehoseOutput.Init(&conf)
	assert.NoError(t, err, "did not expect error from Init()")
	// override the client with a mock
	firehoseOutput.client = mockClient

	// create a real stop channel and encoder
	mockChan := make(chan bool)
	mockOR.EXPECT().StopChan().Return(mockChan)

	encoder := new(plugins.PayloadEncoder)
	encConfig := new(plugins.PayloadEncoderConfig)
	encoder.Init(encConfig)
	mockOR.EXPECT().Encoder().Return(encoder)
	err = firehoseOutput.Prepare(mockOR, mockPH)
	assert.NoError(t, err, "did not expect error from Prepare()")

	// Send test input through the channel
	input := `{"key":"value"}`
	testPack := pipeline.PipelinePack{
		Message: &message.Message{
			Payload: &input,
		},
		QueueCursor: "queuecursor",
	}
	expected := [][]byte{
		[]byte(input),
	}

	mockOR.EXPECT().Encode(gomock.Any()).Return([]byte(input), nil)
	mockClient.EXPECT().PutRecordBatch(expected).Return(nil)
	mockOR.EXPECT().UpdateCursor("queuecursor")
	err = firehoseOutput.ProcessMessage(&testPack)
	assert.NoError(t, err, "did not expect err for valid message")

	runtime.Gosched()
	firehoseOutput.CleanUp()
}

// TestEmptyMessage tests that an error is generated when the message is empty
func TestEmptyMessage(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockOR := pipelinemock.NewMockOutputRunner(mockCtrl)
	mockFirehose := NewMockRecordPutter(mockCtrl)
	conf := FirehoseOutputConfig{
		FlushCount:    1,
		FlushInterval: 0,
	}
	firehoseOutput := FirehoseOutput{
		client: mockFirehose,
		conf:   &conf,
		or:     mockOR,
	}

	// Send test input through the channel
	input := ``
	testPack := pipeline.PipelinePack{
		Message: &message.Message{
			Payload: &input,
		},
		QueueCursor: "queuecursor",
	}

	mockOR.EXPECT().Encode(gomock.Any()).Return(nil, nil)
	err := firehoseOutput.ProcessMessage(&testPack)
	assert.Error(t, err, "did not return err for empty json object")

	mockOR.EXPECT().Encode(gomock.Any()).Return([]byte(""), nil)
	err = firehoseOutput.ProcessMessage(&testPack)
	assert.Error(t, err, "did not return err for empty json object")
}

// TestBatchLimit tests that an error is generated a batch limit that is too
// large is given
func TestBatchLimit(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	conf := FirehoseOutputConfig{
		FlushCount:    501,
		FlushInterval: 0,
	}

	firehoseOutput := new(FirehoseOutput)
	err := firehoseOutput.Init(&conf)
	assert.Error(t, err, "expected an error from Init() because FloushCount > 500")
}

// TestMessageBatching makes sure that messages are batched appropriately
func TestMessageBatching(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockOR := pipelinemock.NewMockOutputRunner(mockCtrl)
	mockPH := pipelinemock.NewMockPluginHelper(mockCtrl)
	mockClient := NewMockRecordPutter(mockCtrl)
	conf := FirehoseOutputConfig{
		FlushCount:    3,
		FlushInterval: 0,
	}

	firehoseOutput := new(FirehoseOutput)
	err := firehoseOutput.Init(&conf)
	assert.NoError(t, err, "did not expect error from Init()")
	// override the client with a mock
	firehoseOutput.client = mockClient

	// create a real stop channel & encoder
	mockChan := make(chan bool)
	mockOR.EXPECT().StopChan().Return(mockChan)

	encoder := new(plugins.PayloadEncoder)
	encConfig := new(plugins.PayloadEncoderConfig)
	encoder.Init(encConfig)
	mockOR.EXPECT().Encoder().Return(encoder)

	err = firehoseOutput.Prepare(mockOR, mockPH)
	assert.NoError(t, err, "did not expect error from Prepare()")

	// Send 3 messages to trigger the flush
	input1 := `{"key":"value1"}`
	testPack1 := pipeline.PipelinePack{
		Message: &message.Message{
			Payload: &input1,
		},
		QueueCursor: "queuecursor1",
	}

	input2 := `{"key":"value2"}`
	testPack2 := pipeline.PipelinePack{
		Message: &message.Message{
			Payload: &input2,
		},
		QueueCursor: "queuecursor2",
	}

	input3 := `{"key":"value3"}`
	testPack3 := pipeline.PipelinePack{
		Message: &message.Message{
			Payload: &input3,
		},
		QueueCursor: "queuecursor3",
	}

	expected := [][]byte{
		[]byte(`{"key":"value1"}`),
		[]byte(`{"key":"value2"}`),
		[]byte(`{"key":"value3"}`),
	}

	mockOR.EXPECT().Encode(gomock.Any()).Return([]byte(input1), nil)
	err = firehoseOutput.ProcessMessage(&testPack1)
	assert.NoError(t, err, "did not expect err for valid message (1)")
	runtime.Gosched()

	mockOR.EXPECT().Encode(gomock.Any()).Return([]byte(input2), nil)
	err = firehoseOutput.ProcessMessage(&testPack2)
	assert.NoError(t, err, "did not expect err for valid message (2)")
	runtime.Gosched()

	mockOR.EXPECT().Encode(gomock.Any()).Return([]byte(input3), nil)
	mockClient.EXPECT().PutRecordBatch(expected).Return(nil)
	mockOR.EXPECT().UpdateCursor("queuecursor3")
	err = firehoseOutput.ProcessMessage(&testPack3)
	assert.NoError(t, err, "did not expect err for valid message (3)")

	// Make sure everything got sent and cleanup
	runtime.Gosched()
	assert.Equal(t, 0, len(firehoseOutput.batchedRecords), "pending records should be empty")
	assert.Equal(t, 3, firehoseOutput.sentRecordCount, "3 messages should have been sent")
	assert.Equal(t, 3, firehoseOutput.recvRecordCount, "3 messages should have been recv")
	assert.Equal(t, 0, firehoseOutput.droppedRecordCount, "0 messages should have been dropped")
	firehoseOutput.CleanUp()
}
