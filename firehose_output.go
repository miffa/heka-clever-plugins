package heka_clever_plugins

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Clever/heka-clever-plugins/aws"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type MsgPack struct {
	record      []byte
	queueCursor string
}

type FirehoseOutput struct {
	recvRecordCount    int64
	sentRecordCount    int64
	droppedRecordCount int64
	batchedRecords     [][]byte
	queueCursor        string
	batchChan          chan MsgPack
	stopChan           chan bool
	client             aws.RecordPutter
	conf               *FirehoseOutputConfig
	or                 pipeline.OutputRunner
	reportLock         sync.Mutex
	flushTicker        *time.Ticker
}

type FirehoseOutputConfig struct {
	// Kineses stream name to put data to
	Stream string `toml:"stream"`
	// AWS region the stream lives in
	Region string `toml:"region"`
	// Optional column to use as the message timestamp
	TimestampColumn string `toml:"timestamp_column"`
	// Interval at which accumulated messages should be bulk put to
	// firehose, in milliseconds (default 1000, i.e. 1 second).
	FlushInterval uint32 `toml:"flush_interval"`
	// Number of messages that triggers a put to firehose
	// (default to 1, maximum is 500)
	FlushCount int `toml:"flush_count"`
}

func (f *FirehoseOutput) ConfigStruct() interface{} {
	return &FirehoseOutputConfig{
		FlushInterval: 1000,
		FlushCount:    1,
	}
}

func (f *FirehoseOutput) Init(config interface{}) error {
	f.conf = config.(*FirehoseOutputConfig)

	if f.conf.FlushCount > 500 {
		return fmt.Errorf("FlushCount cannot exceed 500 messages")
	}

	f.batchChan = make(chan MsgPack, 100)
	f.batchedRecords = make([][]byte, 0, f.conf.FlushCount)

	if f.conf.Stream == "" {
		return fmt.Errorf("Unspecificed stream name")
	}

	if os.Getenv("HEKA_TESTING") == "" {
		f.client = aws.NewFirehose(f.conf.Region, f.conf.Stream)
	} else {
		endpoint := os.Getenv("MOCK_FIREHOSE_ENDPOINT")
		if endpoint == "" {
			return fmt.Errorf("env-var MOCK_FIREHOSE_ENDPOINT not found for heka-testing")
		}
		fmt.Println("Mocking out firehose output: " + endpoint)
		f.client = aws.NewMockRecordPutter(f.conf.Stream, endpoint)
	}

	return nil
}

func (f *FirehoseOutput) Prepare(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	f.or = or
	f.stopChan = or.StopChan()

	// Setup the batch ticker
	if f.conf.FlushInterval > 0 {
		d, err := time.ParseDuration(fmt.Sprintf("%dms", f.conf.FlushInterval))
		if err != nil {
			return fmt.Errorf("can't create flush ticker: %s", err.Error())
		}
		f.flushTicker = time.NewTicker(d)
	} else {
		// Create an empty Ticker so that the ticker channel can still be
		// checked
		ticker := time.Ticker{}
		f.flushTicker = &ticker
	}

	go f.batchSender()
	return nil
}

func (f *FirehoseOutput) parseFields(pack *pipeline.PipelinePack) map[string]interface{} {
	m := pack.Message
	object := make(map[string]interface{})

	// Handle standard heka fields
	object["uuid"] = m.GetUuidString()
	object["timestamp"] = time.Unix(0, m.GetTimestamp()).Format("2006-01-02 15:04:05.000")
	object["type"] = m.GetType()
	object["logger"] = m.GetLogger()
	object["severity"] = m.GetSeverity()
	object["payload"] = m.GetPayload()
	object["envversion"] = m.GetEnvVersion()
	object["pid"] = m.GetPid()
	object["hostname"] = m.GetHostname()

	// store each dynamic field as a top level entry
	for _, field := range m.Fields {
		// ignore byte fields and empty fields
		if field.Name != nil && field.GetValueType() != message.Field_BYTES {
			object[*field.Name] = field.GetValue()
		}
	}
	return object
}

func (f *FirehoseOutput) ProcessMessage(pack *pipeline.PipelinePack) error {
	atomic.AddInt64(&f.recvRecordCount, 1)
	payload := pack.Message.GetPayload()
	timestamp := time.Unix(0, pack.Message.GetTimestamp()).Format("2006-01-02 15:04:05.000")

	// Verify input is valid json
	object := make(map[string]interface{})
	err := json.Unmarshal([]byte(payload), &object)
	if err != nil {
		// Since payload is not a json object, parse the entire pack
		// into a map of fields and dynamic fields
		object = f.parseFields(pack)
	}

	if len(object) == 0 {
		atomic.AddInt64(&f.droppedRecordCount, 1)
		return errors.New("No fields found in message")
	}

	if f.conf.TimestampColumn != "" {
		// add Heka message's timestamp to column named in timestampColumn
		object[f.conf.TimestampColumn] = timestamp
	}

	record, err := json.Marshal(object)
	if err != nil {
		atomic.AddInt64(&f.droppedRecordCount, 1)
		return err
	}

	// Send data to the batcher
	f.batchChan <- MsgPack{record: record, queueCursor: pack.QueueCursor}
	return nil
}

// batchSender is a go routine that gets sent:
//   - messages to be batched
//   - a stop signal
//   - a flush signal
func (f *FirehoseOutput) batchSender() {
	ok := true
	for ok {
		select {
		case <-f.stopChan:
			ok = false
			continue
		case pack := <-f.batchChan:
			f.batchedRecords = append(f.batchedRecords, pack.record)
			f.queueCursor = pack.queueCursor
			if len(f.batchedRecords) >= f.conf.FlushCount {
				f.sendBatch()
			}
		case <-f.flushTicker.C:
			if len(f.batchedRecords) > 0 {
				f.sendBatch()
			}
		}
	}
}

// sendBatch is called everytime the batchedRecords queue is full or
// the timer has expired
func (f *FirehoseOutput) sendBatch() {
	count := int64(len(f.batchedRecords))
	err := f.client.PutRecordBatch(f.batchedRecords)

	// Update the cursor (these messages are either lost forever or sent)
	// and reset the queue
	f.or.UpdateCursor(f.queueCursor)
	f.batchedRecords = f.batchedRecords[0:0]

	if err != nil {
		// TODO: PutRecordBatch should return the number of successful records
		//       so that the correct amount can be set here
		atomic.AddInt64(&f.droppedRecordCount, count)
		f.or.LogError(err)
	} else {
		atomic.AddInt64(&f.sentRecordCount, count)
	}
}

func (f *FirehoseOutput) CleanUp() {
	if f.flushTicker != nil {
		f.flushTicker.Stop()
	}
}

func (f *FirehoseOutput) ReportMsg(msg *message.Message) error {
	f.reportLock.Lock()
	defer f.reportLock.Unlock()

	message.NewInt64Field(msg, "sentRecordCount",
		atomic.LoadInt64(&f.sentRecordCount), "count")
	message.NewInt64Field(msg, "droppedRecordCount",
		atomic.LoadInt64(&f.droppedRecordCount), "count")
	message.NewInt64Field(msg, "recvRecordCount",
		atomic.LoadInt64(&f.recvRecordCount), "count")
	return nil
}

func init() {
	pipeline.RegisterPlugin("FirehoseOutput", func() interface{} {
		return new(FirehoseOutput)
	})
}
