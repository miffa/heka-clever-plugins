package heka_clever_plugins

import (
	"encoding/json"
	"time"

	"github.com/mozilla-services/heka/pipeline"
)

type FakeOutput struct {
	timestampColumn string
}

type FakeOutputConfig struct {
	Stream          string `toml:"stream"`
	Region          string `toml:"region"`
	TimestampColumn string `toml:"timestamp_column"`
}

func (f *FakeOutput) ConfigStruct() interface{} {
	return &FakeOutputConfig{}
}

func (f *FakeOutput) Init(rawConfig interface{}) error {
	config := rawConfig.(*FakeOutputConfig)
	f.timestampColumn = config.TimestampColumn
	return nil
}

func (f *FakeOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	for pack := range or.InChan() {
		payload := pack.Message.GetPayload()
		timestamp := time.Unix(0, pack.Message.GetTimestamp()).Format("2006-01-02 15:04:05.000")
		// Explicitly do NOT call pack.Recycle()

		// Verify input is valid json
		object := make(map[string]interface{})
		err := json.Unmarshal([]byte(payload), &object)
		if err != nil {
			or.LogError(err)
			continue
		}

		if f.timestampColumn != "" {
			// add Heka message's timestamp to column named in timestampColumn
			object[f.timestampColumn] = timestamp
		}

		_, err = json.Marshal(object)
		if err != nil {
			or.LogError(err)
			continue
		}

	}
	return nil
}

func init() {
	pipeline.RegisterPlugin("FakeOutput", func() interface{} {
		return new(FakeOutput)
	})
}
