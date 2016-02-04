package cloudwatchlogs

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_BatchLen(t *testing.T) {
	t.Log("Verify can add items to a LogBatch")
	startTime := time.Now().Unix() * 1000
	lb := NewLogBatch("mockLogGroup", "mockLogStream")
	assert.Equal(t, 0, lb.Len())
	lb.TryAddLog("foo", startTime)
	assert.Equal(t, 1, lb.Len())
	lb.TryAddLog("bar", startTime)
	assert.Equal(t, 2, lb.Len())
}

func Test_BatchSort(t *testing.T) {
	startTime := time.Now().Unix() * 1000
	lb := NewLogBatch("mockLogGroup", "mockLogStream")
	lb.TryAddLog("bar", startTime+1)
	lb.TryAddLog("baz", startTime+2)
	lb.TryAddLog("foo", startTime)

	t.Log("Verify LogBatch sorts logs based on timestamp")
	sort.Sort(lb)

	assert.Equal(t, "foo", *lb.Events[0].Message)
	assert.Equal(t, startTime, *lb.Events[0].Timestamp)

	assert.Equal(t, "bar", *lb.Events[1].Message)
	assert.Equal(t, startTime+1, *lb.Events[1].Timestamp)

	assert.Equal(t, "baz", *lb.Events[2].Message)
	assert.Equal(t, startTime+2, *lb.Events[2].Timestamp)
}

func Test_BatchErrors(t *testing.T) {
	startTime := time.Now().Unix() * 1000

	t.Log("Verify batch max size is 1,048,576")
	lb := NewLogBatch("mockLogGroup", "mockLogStream")

	tooLarge := string(make([]byte, maxBatchSizeBytes-25))
	err := lb.TryAddLog(tooLarge, startTime)
	assert.Error(t, err)
	assert.IsType(t, ExceedsBatchSizeError{}, err)

	justRight := string(make([]byte, maxBatchSizeBytes-26))
	err = lb.TryAddLog(justRight, startTime)
	assert.NoError(t, err)

	err = lb.TryAddLog("a", startTime)
	assert.Error(t, err)
	assert.IsType(t, ExceedsBatchSizeError{}, err)

	t.Log("Verify batch max events is 10,000")
	lb = NewLogBatch("mockLogGroup", "mockLogStream")
	for i := 0; i < 10000; i++ {
		err = lb.TryAddLog("a", startTime)
		assert.NoError(t, err)
	}
	err = lb.TryAddLog("a", startTime)
	assert.Error(t, err)
	assert.IsType(t, ExceedsBatchMaxEventsError{}, err)
}
