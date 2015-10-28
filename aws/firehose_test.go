package aws

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestValidPutRecord(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := NewMockFirehoseAPI(mockCtrl)

	f := Firehose{
		client: mockFirehoseAPI,
		stream: "test",
	}

	data := []byte("test")
	expectedInput := &firehose.PutRecordInput{
		DeliveryStreamName: &f.stream,
		Record: &firehose.Record{
			Data: data,
		},
	}

	// Return success
	mockFirehoseAPI.EXPECT().PutRecord(expectedInput).Return(&firehose.PutRecordOutput{}, nil)

	err := f.PutRecord(data)
	assert.NoError(t, err, "valid Send() failed")
}

func TestInvalidSend(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := NewMockFirehoseAPI(mockCtrl)

	f := Firehose{
		client: mockFirehoseAPI,
		stream: "test",
	}

	data := []byte("test")
	expectedInput := &firehose.PutRecordInput{
		DeliveryStreamName: &f.stream,
		Record: &firehose.Record{
			Data: data,
		},
	}

	// Return error
	mockFirehoseAPI.EXPECT().PutRecord(expectedInput).Return(nil, errors.New("test error"))

	err := f.PutRecord(data)
	assert.Error(t, err, "expected invalid Send() to fail")
}

func TestValidPutRecordBatch(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := NewMockFirehoseAPI(mockCtrl)

	f := Firehose{
		client: mockFirehoseAPI,
		stream: "test",
	}

	data := [][]byte{
		[]byte("test"),
		[]byte("test"),
	}

	expectedInput := &firehose.PutRecordBatchInput{
		DeliveryStreamName: &f.stream,
		Records: []*firehose.Record{
			&firehose.Record{
				Data: data[0],
			},
			&firehose.Record{
				Data: data[1],
			},
		},
	}

	// Return success
	mockFirehoseAPI.EXPECT().PutRecord(expectedInput).Return(&firehose.PutRecordOutput{}, nil)

	err := f.PutRecordBatch(data)
	assert.NoError(t, err, "valid Send() failed")
}

func TestInvalidPutRecordBatch(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := NewMockFirehoseAPI(mockCtrl)

	f := Firehose{
		client: mockFirehoseAPI,
		stream: "test",
	}

	data := [][]byte{
		[]byte("test"),
		[]byte("test"),
	}

	expectedInput := &firehose.PutRecordBatchInput{
		DeliveryStreamName: &f.stream,
		Records: []*firehose.Record{
			&firehose.Record{
				Data: data[0],
			},
			&firehose.Record{
				Data: data[1],
			},
		},
	}

	// Return error
	mockFirehoseAPI.EXPECT().PutRecordBatch(expectedInput).Return(nil, errors.New("test error"))

	err := f.PutRecordBatch(data)
	assert.Error(t, err, "expected invalid Send() to fail")
}
