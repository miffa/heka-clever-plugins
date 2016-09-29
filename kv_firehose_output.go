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
	"github.com/Clever/heka-clever-plugins/batcher"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type KVFirehoseOutput struct {
	conf     *KVFirehoseOutputConfig
	or       pipeline.OutputRunner
	batchers map[string]batcher.Batcher

	mockEndpoint string

	reportLock         sync.Mutex
	recvRecordCount    int64
	sentRecordCount    int64
	droppedRecordCount int64
}

type KVFirehoseOutputConfig struct {
	// The value of this field is used as the firehose `series` (or stream) name
	SeriesField string `toml:"series_field"`
	// AWS region the stream lives in
	Region string `toml:"region"`
	// Interval at which accumulated messages should be bulk put to
	// firehose, in milliseconds (default 1000, i.e. 1 second).
	FlushInterval uint32 `toml:"flush_interval"`
	// Number of messages that triggers a push to firehose
	// (default to 1, maximum is 500)
	FlushCount int `toml:"flush_count"`
	// Size of batch that triggers a push to firehose
	// (default to 1024 * 1024 (1mb))
	FlushSize int `toml:"flush_size"`
}

type syncPutterAdapter struct {
	client aws.RecordPutter
	output *KVFirehoseOutput
}

func (s *syncPutterAdapter) Flush(batch [][]byte) {
	count := int64(len(batch))

	var err error
	// Expotential backoff with retry limit
	for retries, delay := 0, 1; retries < 5; retries, delay = retries+1, delay*2 {
		err = s.client.PutRecordBatch(batch)

		if err == nil {
			break
		}

		s.output.or.LogError(
			fmt.Errorf("Firehose put-record failure: %s.  Retry %d", err.Error(), retries))

		time.Sleep(time.Duration(delay*250) * time.Millisecond)
	}

	if err != nil {
		// TODO: PutRecordBatch should return the number of successful records
		//       so that the correct amount can be set here
		atomic.AddInt64(&s.output.droppedRecordCount, count)
		s.output.or.LogError(err)
	} else {
		atomic.AddInt64(&s.output.sentRecordCount, count)
	}
}

func (f *KVFirehoseOutput) ConfigStruct() interface{} {
	return &KVFirehoseOutputConfig{
		FlushInterval: 1000,
		FlushCount:    1,
		FlushSize:     1024 * 1024,
	}
}

func (f *KVFirehoseOutput) Init(config interface{}) error {
	f.conf = config.(*KVFirehoseOutputConfig)

	if f.conf.FlushCount > 500 {
		return fmt.Errorf("FlushCount cannot exceed 500 messages")
	}

	f.batchers = map[string]batcher.Batcher{}

	if os.Getenv("HEKA_TESTING") != "" {
		endpoint := os.Getenv("MOCK_FIREHOSE_ENDPOINT")
		if endpoint == "" {
			return fmt.Errorf("env-var MOCK_FIREHOSE_ENDPOINT not found for heka-testing")
		}
		fmt.Println("Mocking out firehose output: " + endpoint)
		f.mockEndpoint = endpoint
	}

	return nil
}

func (f *KVFirehoseOutput) Prepare(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	f.or = or

	go f.listenForStop(or.StopChan())

	return nil
}

func (f *KVFirehoseOutput) listenForStop(stopChan <-chan bool) {
	<-stopChan

	for _, batch := range f.batchers {
		batch.Flush()
	}
}

func (f *KVFirehoseOutput) createBatcherSync(seriesName string) batcher.Sync {
	var client aws.RecordPutter

	if f.mockEndpoint == "" {
		client = aws.NewFirehose(f.conf.Region, seriesName)
	} else {
		fmt.Printf("Mocking out firehose output '%s' to %s\n", seriesName, f.mockEndpoint)
		client = aws.NewMockRecordPutter(seriesName, f.mockEndpoint)
	}

	return &syncPutterAdapter{client: client, output: f}
}

func (f *KVFirehoseOutput) parseFields(pack *pipeline.PipelinePack) (
	seriesName string, object map[string]interface{},
) {
	m := pack.Message
	object = make(map[string]interface{})

	// Adding the timestamp twice to maintain reverse compatibility
	object["timestamp"] = time.Unix(0, m.GetTimestamp()).Format("2006-01-02 15:04:05.000")
	object["time"] = time.Unix(0, m.GetTimestamp()).Format("2006-01-02 15:04:05.000")

	// Handle standard heka fields
	object["uuid"] = m.GetUuidString()
	object["type"] = m.GetType()
	object["logger"] = m.GetLogger()
	object["severity"] = m.GetSeverity()
	object["payload"] = m.GetPayload()
	object["envversion"] = m.GetEnvVersion()
	object["pid"] = m.GetPid()
	object["hostname"] = m.GetHostname()

	seriesName = ""
	// store each dynamic field as a top level entry
	for _, field := range m.Fields {
		fieldType := field.GetValueType()

		if *field.Name == f.conf.SeriesField && fieldType == message.Field_STRING {
			seriesName = field.GetValue().(string)
		} else if field.Name != nil && fieldType != message.Field_BYTES {
			// ignore byte fields and empty fields
			object[*field.Name] = field.GetValue()
		}
	}
	return seriesName, object
}

func (f *KVFirehoseOutput) ProcessMessage(pack *pipeline.PipelinePack) error {
	atomic.AddInt64(&f.recvRecordCount, 1)
	seriesName, object := f.parseFields(pack)

	if seriesName == "" {
		atomic.AddInt64(&f.droppedRecordCount, 1)
		return errors.New("No series name found in message")
	}

	if len(object) == 0 {
		atomic.AddInt64(&f.droppedRecordCount, 1)
		return errors.New("No fields found in message")
	}

	record, err := json.Marshal(object)
	if err != nil {
		atomic.AddInt64(&f.droppedRecordCount, 1)
		return err
	}

	batch, ok := f.batchers[seriesName]
	if !ok {
		sync := f.createBatcherSync(seriesName)
		batch = batcher.New(sync)
		f.batchers[seriesName] = batch
	}
	batch.Send(record)

	// We're forgoing any benefits of disk buffering.  With or without it all the messages are
	// lost if the container crashes.  Supporting would significantly complicate the code.
	f.or.UpdateCursor(pack.QueueCursor)

	return nil
}

func (f *KVFirehoseOutput) CleanUp() {
}

func (f *KVFirehoseOutput) ReportMsg(msg *message.Message) error {
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
	pipeline.RegisterPlugin("KVFirehoseOutput", func() interface{} {
		return new(KVFirehoseOutput)
	})
}
