package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	iface "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
)

// RecordPutter is the interface for sending data to a delivery stream
type RecordPutter interface {
	PutRecord(record []byte) error
	PutRecordBatch(records [][]byte) error
}

// Firehose represents a single aws Firehose stream
type Firehose struct {
	client iface.FirehoseAPI
	stream string
}

// NewFirehose returns a configured Firehose object
func NewFirehose(region, stream string) *Firehose {
	awsConfig := aws.NewConfig().WithRegion(region)
	return &Firehose{
		client: firehose.New(session.New(), awsConfig),
		stream: stream,
	}
}

// PutRecord sends a single record to the Firehose stream
func (f Firehose) PutRecord(record []byte) error {
	input := &firehose.PutRecordInput{
		DeliveryStreamName: &f.stream,
		Record: &firehose.Record{
			Data: record,
		},
	}
	_, err := f.client.PutRecord(input)
	return err
}

// PutRecordBatch sends an array of records to the Firehose stream
// as a single batch request
func (f Firehose) PutRecordBatch(records [][]byte) error {
	// Construct the array of firehose.Records
	awsRecords := make([]*firehose.Record, len(records))
	for idx, record := range records {
		awsRecords[idx] = &firehose.Record{
			Data: record,
		}
	}

	input := &firehose.PutRecordBatchInput{
		DeliveryStreamName: &f.stream,
		Records:            awsRecords,
	}
	res, err := f.client.PutRecordBatch(input)
	if err != nil {
		return err
	}
	// Check for any individual records failing
	if *res.FailedPutCount != 0 {
		return fmt.Errorf("%d records failed to upload", *res.FailedPutCount)
	}
	return nil
}
