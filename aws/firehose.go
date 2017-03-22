package aws

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	iface "github.com/aws/aws-sdk-go/service/firehose/firehoseiface"

	"gopkg.in/Clever/kayvee-go.v3/logger"
)

var kvlog = logger.New("redshift-output")

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
	sess := session.Must(session.NewSession(aws.NewConfig().WithRegion(region)))
	return &Firehose{
		client: firehose.New(sess),
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

func (f Firehose) sendRecords(records [][]byte) (*firehose.PutRecordBatchOutput, error) {
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

	return f.client.PutRecordBatch(input)
}

// PutRecordBatch sends an array of records to the Firehose stream
// as a single batch request
func (f Firehose) PutRecordBatch(records [][]byte) error {
	res, err := f.sendRecords(records)
	if err != nil {
		return err
	}

	retries := 0
	delay := 1
	for *res.FailedPutCount != 0 {
		retryRecords := [][]byte{}
		for idx, entry := range res.RequestResponses {
			if entry != nil && entry.ErrorMessage != nil && *entry.ErrorMessage != "" {
				retryRecords = append(retryRecords, records[idx])
			}
		}
		if retries > 4 {
			return fmt.Errorf("Too many retries. failed to put %d records -- stream: %s", len(retryRecords), f.stream)
		}

		// Retry sending records
		time.Sleep(time.Duration(delay) * time.Second)
		res, err = f.sendRecords(retryRecords)
		if err != nil {
			return err
		}

		retries += 1
		delay *= 2
	}
	return nil
}
