package heka_clever_plugins

import (
	"encoding/json"

	"github.com/Clever/heka-clever-plugins/aws"

	"github.com/mozilla-services/heka/pipeline"
)

type FirehoseOutput struct {
	client aws.Firehose
}

type FirehoseOutputConfig struct {
	Stream string `toml:"stream"`
	Region string `toml:"region"`
}

func (f *FirehoseOutput) ConfigStruct() interface{} {
	return &FirehoseOutputConfig{}
}

func (f *FirehoseOutput) Init(rawConfig interface{}) error {
	config := rawConfig.(*FirehoseOutputConfig)
	f.client = aws.NewFirehose(config.Region, config.Stream)
	return nil
}

func (f *FirehoseOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	for pack := range or.InChan() {
		payload := pack.Message.GetPayload()
		pack.Recycle(nil)

		// REVIEW - is this neccessary? if we can guarantee it's valid json,
		// we can send the []byte directly to the firehose
		object := make(map[string]interface{})
		err := json.Unmarshal([]byte(payload), &object)
		if err != nil {
			or.LogError(err)
			continue
		}

		data, err := json.Marshal(object)
		if err != nil {
			or.LogError(err)
			continue
		}
		err = f.client.Send([]byte(data))
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
