package cloudwatchlogs

import (
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

func GetLogStream(svc *cloudwatchlogs.CloudWatchLogs, group, name string) (*cloudwatchlogs.LogStream, error) {
	// TODO: limit > 1 ... page through results
	params := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(group), // Required
		Descending:          aws.Bool(true),
		Limit:               aws.Int64(1),
		LogStreamNamePrefix: aws.String(name),
	}
	resp, err := svc.DescribeLogStreams(params)
	if err != nil {
		// TODO: Remove debugging
		fmt.Println(err.Error())
		return nil, err
	}

	for _, stream := range resp.LogStreams {
		if *stream.LogStreamName == name {
			return stream, nil
		}
	}
	return nil, fmt.Errorf("couldn't find a stream called:", name)
}

////////////////////
// Batching
////////////////////

func NewLogBatch(group, stream string) *LogBatch {
	return &LogBatch{
		LogGroupName:  group,
		LogStreamName: stream,
		Events:        []*cloudwatchlogs.InputLogEvent{},
		Created:       time.Now(),
	}
}

type LogBatch struct {
	LogGroupName  string
	LogStreamName string
	Events        []*cloudwatchlogs.InputLogEvent
	Created       time.Time
}

var maxBatchSizeBytes = 1048576

type ExceedsBatchSizeError struct {
}

// Size computes size of the LogBatch in bytes
func (e ExceedsBatchSizeError) Error() string {
	return fmt.Sprintf("Adding this log to the current batch would exceed the max size of %d bytes", maxBatchSizeBytes)
}

type ExceedsBatchMaxEventsError struct {
}

// Size computes size of the LogBatch in bytes
func (e ExceedsBatchMaxEventsError) Error() string {
	return "Adding this log to the current batch would exceed max count of 10,000"
}
func (lb *LogBatch) Len() int {
	return len(lb.Events)
}

func (lb *LogBatch) Swap(i, j int) {
	s := lb.Events
	s[i], s[j] = s[j], s[i]
}

func (lb *LogBatch) Less(i, j int) bool {
	s := lb.Events
	return *s[i].Timestamp < *s[j].Timestamp
}

// Size computes size of the LogBatch in bytes
// "this size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event"
func (lb *LogBatch) Size() int {
	total := 0
	for _, e := range lb.Events {
		total += (26 + len(*e.Message))
	}
	return total
}

func (lb *LogBatch) TryAddLog(line string, timestamp int64) error {
	// Validate that adding new event wont exceed batch limits
	// See AWS docs for limitations:
	// http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	// TODO: Add other validations
	if len(lb.Events) >= 10000 {
		return ExceedsBatchMaxEventsError{}
	}
	if lb.Size()+(26+len(line)) > maxBatchSizeBytes {
		return ExceedsBatchSizeError{}
	}

	// Create new event
	newEvent := cloudwatchlogs.InputLogEvent{}
	newEvent.Message = &line        //aws.String(line)
	newEvent.Timestamp = &timestamp //aws.Int64(timestamp)

	// Add to batch
	lb.Events = append(lb.Events, &newEvent)

	return nil
}

func (lb *LogBatch) Put(svc *cloudwatchlogs.CloudWatchLogs) error {
	stream, err := GetLogStream(svc, lb.LogGroupName, lb.LogStreamName)
	if err != nil {
		// TODO: remove debugging
		fmt.Println(err.Error())
		return err
	}
	token := *stream.UploadSequenceToken

	// TODO: Need to create log stream if dne?

	// events must be sorted by timestamp
	sort.Sort(lb)

	params := &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     lb.Events,
		LogGroupName:  aws.String(lb.LogGroupName),
		LogStreamName: aws.String(lb.LogStreamName),
		SequenceToken: aws.String(token),
	}
	resp, err := svc.PutLogEvents(params)

	if err != nil {
		// TODO: remove debugging
		fmt.Println(err.Error())
		return err
	}

	// Pretty-print the response data.
	fmt.Println(resp)
	return nil
}
