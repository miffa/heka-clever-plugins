package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	. "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
)

type Firehose interface {
	Send(data []byte) error
}

type firehoseConfig struct {
	client FirehoseAPI
	stream string
}

func NewFirehose(region, stream string) Firehose {
	awsConfig := aws.NewConfig().WithRegion(region)
	return &firehoseConfig{
		client: firehose.New(awsConfig),
		stream: stream,
	}
}

func (f firehoseConfig) Send(data []byte) error {
	input := &firehose.PutRecordInput{
		DeliveryStreamName: &f.stream,
		Record: &firehose.Record{
			Data: data,
		},
	}
	_, err := f.client.PutRecord(input)
	return err
}
