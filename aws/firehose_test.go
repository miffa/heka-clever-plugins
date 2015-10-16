package aws

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestValidSend(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := NewMockFirehoseAPI(mockCtrl)

	f := firehoseConfig{
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

	mockFirehoseAPI.EXPECT().PutRecord(expectedInput).Return(&firehose.PutRecordOutput{}, nil)

	err := f.Send(data)
	assert.NoError(t, err, "valid Send() failed")
}

func TestInvalidSend(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockFirehoseAPI := NewMockFirehoseAPI(mockCtrl)

	f := firehoseConfig{
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

	mockFirehoseAPI.EXPECT().PutRecord(expectedInput).Return(nil, errors.New("test error"))

	err := f.Send(data)
	assert.Error(t, err, "expected invalid Send() to fail")
}
