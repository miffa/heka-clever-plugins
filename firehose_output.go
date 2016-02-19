package heka_clever_plugins

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/Clever/heka-clever-plugins/aws"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type FirehoseOutput struct {
	client          aws.RecordPutter
	timestampColumn string
}

type FirehoseOutputConfig struct {
	Stream          string `toml:"stream"`
	Region          string `toml:"region"`
	TimestampColumn string `toml:"timestamp_column"`
}

func (f *FirehoseOutput) ConfigStruct() interface{} {
	return &FirehoseOutputConfig{}
}

func (f *FirehoseOutput) Init(rawConfig interface{}) error {
	config := rawConfig.(*FirehoseOutputConfig)
	f.client = aws.NewFirehose(config.Region, config.Stream)
	f.timestampColumn = config.TimestampColumn
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

func (f *FirehoseOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	for pack := range or.InChan() {
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

		// Only recycle the pack after all data has been pulled from it
		pack.Recycle(nil)

		if len(object) == 0 {
			or.LogError(errors.New("No fields found in message"))
			continue
		}

		if f.timestampColumn != "" {
			// add Heka message's timestamp to column named in timestampColumn
			object[f.timestampColumn] = timestamp
		}

		record, err := json.Marshal(object)
		if err != nil {
			or.LogError(err)
			continue
		}

		// Send data to the firehose
		err = f.client.PutRecord(record)
		if err != nil {
			or.LogError(err)
			continue
		}
	}

	return nil
}

func init() {
	pipeline.RegisterPlugin("FirehoseOutput", func() interface{} {
		return new(FirehoseOutput)
	})
}
