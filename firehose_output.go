package heka_clever_plugins

import (
	"encoding/json"
	"time"
	"errors"

	"github.com/Clever/heka-clever-plugins/aws"

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

func (f *FirehoseOutput) useEncoder(or pipeline.OutputRunner, pack *pipeline.PipelinePack, object *map[string]interface{}) error {
	if or.Encoder() == nil {
		return errors.New("tried to use encoder, but not configured")
	}
	outBytes, err := or.Encode(pack)
	if err != nil {
		return err
	}

	return json.Unmarshal(outBytes, &object)
}

func (f *FirehoseOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	for pack := range or.InChan() {
		payload := pack.Message.GetPayload()
		timestamp := time.Unix(0, pack.Message.GetTimestamp()).Format("2006-01-02 15:04:05.000")
		pack.Recycle(nil)

		// Verify input is valid json
		object := make(map[string]interface{})
		err := json.Unmarshal([]byte(payload), &object)
		if err != nil {
			// attempt to use Encoder if it exists
			if encode_err := f.useEncoder(or, pack, &object); encode_err != nil {
				or.LogError(err)
				or.LogError(encode_err)
				continue
			}
		}

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
