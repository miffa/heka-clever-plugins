package heka_clever_plugins

import (
	"runtime"
	"testing"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/pipelinemock"
	"github.com/rafrombrc/gomock/gomock"
	"github.com/stretchr/testify/assert"
)

// TestJSONMessage tests that Messages with JSON Payloads get sent
func TestJSONMessage(t *testing.T) {
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
	input := `{"key":"value"}`
	testPack := pipeline.PipelinePack{
		Message: &message.Message{
			Payload: &input,
		},
		QueueCursor: "queuecursor",
	}

	mockFirehose.EXPECT().PutRecord([]byte(input)).Return(nil)
	mockOR.EXPECT().UpdateCursor("queuecursor")
	err := firehoseOutput.ProcessMessage(&testPack)
	assert.NoError(t, err, "did not expect err for valid message")
}

// TestEmptyMessage tests that an error is generated when there is an empty
// JSON message
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
	input := `{}`
	testPack := pipeline.PipelinePack{
		Message: &message.Message{
			Payload: &input,
		},
		QueueCursor: "queuecursor",
	}

	err := firehoseOutput.ProcessMessage(&testPack)
	assert.Error(t, err, "did not return err for empty json object")
}

// TestMessageWithTimestamp tests that if a TimestampColumn is provided in the config
// then the Heka message's timestamp gets added to the message with that column name.
func TestMessageWithTimestamp(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockOR := pipelinemock.NewMockOutputRunner(mockCtrl)
	mockFirehose := NewMockRecordPutter(mockCtrl)
	conf := FirehoseOutputConfig{
		TimestampColumn: "created",
		FlushCount:      1,
		FlushInterval:   0,
	}
	firehoseOutput := FirehoseOutput{
		client: mockFirehose,
		conf:   &conf,
		or:     mockOR,
	}

	// Send test input through the channel
	input := `{"key":"value"}`
	timestamp := time.Date(2015, 07, 1, 13, 14, 15, 0, time.UTC).UnixNano()
	testPack := pipeline.PipelinePack{
		Message: &message.Message{
			Payload:   &input,
			Timestamp: &timestamp,
		},
		QueueCursor: "queuecursor",
	}

	expected := `{"created":"2015-07-01 13:14:15.000","key":"value"}`
	mockFirehose.EXPECT().PutRecord([]byte(expected)).Return(nil)
	mockOR.EXPECT().UpdateCursor("queuecursor")
	err := firehoseOutput.ProcessMessage(&testPack)
	assert.NoError(t, err, "did not expect err for valid message")
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
		FlushInterval: 1000,
	}

	firehoseOutput := new(FirehoseOutput)
	err := firehoseOutput.Init(&conf)
	assert.NoError(t, err, "did not expect error from Init()")
	// override the client with a mock
	firehoseOutput.client = mockClient

	// create a real stop channel
	mockChan := make(chan bool)
	mockOR.EXPECT().StopChan().Return(mockChan)
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

	err = firehoseOutput.ProcessMessage(&testPack1)
	assert.NoError(t, err, "did not expect err for valid message (1)")
	runtime.Gosched()

	err = firehoseOutput.ProcessMessage(&testPack2)
	assert.NoError(t, err, "did not expect err for valid message (2)")
	runtime.Gosched()

	mockClient.EXPECT().PutRecordBatch(expected).Return(nil)
	mockOR.EXPECT().UpdateCursor("queuecursor3")
	err = firehoseOutput.ProcessMessage(&testPack3)
	assert.NoError(t, err, "did not expect err for valid message (3)")
	runtime.Gosched()

	// Make sure everything got sent and cleanup
	assert.Equal(t, 0, len(firehoseOutput.batchedRecords), "pending records should be empty")
	assert.Equal(t, 3, firehoseOutput.sentRecordCount, "3 messages should have been sent")
	assert.Equal(t, 3, firehoseOutput.recvRecordCount, "3 messages should have been recv")
	assert.Equal(t, 0, firehoseOutput.droppedRecordCount, "0 messages should have been dropped")
	firehoseOutput.CleanUp()
}
