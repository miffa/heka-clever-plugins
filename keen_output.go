package heka_clever_plugins

import (
	"encoding/json"
	"github.com/inconshreveable/go-keen"
	"github.com/mozilla-services/heka/pipeline"
)

type KeenClient interface {
	AddEvent(collection string, event interface{}) error
}

type KeenOutput struct {
	client     KeenClient
	collection string
}

type KeenOutputConfig struct {
	ApiKey       string `toml:"api_key"`
	ProjectToken string `toml:"project_token"`
	Collection   string `toml:"collection_name"`
}

func (ko *KeenOutput) ConfigStruct() interface{} {
	return &KeenOutputConfig{}
}

func (ko *KeenOutput) Init(rawConf interface{}) error {
	config := rawConf.(*KeenOutputConfig)
	ko.client = &keen.Client{ApiKey: config.ApiKey, ProjectToken: config.ProjectToken}
	ko.collection = config.Collection
	return nil
}

func (ko *KeenOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	for pack := range or.InChan() {
		payload := pack.Message.GetPayload()
		pack.Recycle()

		event := make(map[string]interface{})
		err := json.Unmarshal([]byte(payload), &event)
		if err != nil {
			or.LogError(err)
			continue
		}
		err = ko.client.AddEvent(ko.collection, event)
		if err != nil {
			or.LogError(err)
			continue
		}
	}
	return nil
}

func init() {
	pipeline.RegisterPlugin("KeenOutput", func() interface{} {
		return new(KeenOutput)
	})
}
