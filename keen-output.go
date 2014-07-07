package heka_clever_plugins

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/inconshreveable/go-keen"
	"github.com/mozilla-services/heka/pipeline"
)

type KeenOutput struct {
	client *keen.Client
}

type KeenOutputConfig struct {
	ApiKey       string `toml:"api_key"`
	ProjectToken string `toml:"project_token"`
}

func (ko *KeenOutput) ConfigStruct() interface{} {
	return &KeenOutputConfig{}
}

func (ko *KeenOutput) Init(rawConf interface{}) error {
	config := rawConf.(*KeenOutputConfig)
	ko.client = &keen.Client{ApiKey: config.ApiKey, ProjectToken: config.ProjectToken}
	return nil
}

func (ko *KeenOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	var (
		err  error
		pack *pipeline.PipelinePack
	)

	for pack = range or.InChan() {
		value, ok := pack.Message.GetFieldValue("Data")
		if !ok {
			return errors.New("Could not get field value 'Data'")
		}

		event := make(map[string]interface{})
		err = json.Unmarshal([]byte(value.(string)), &event)
		if err != nil {
			or.LogError(err)
		}
		err = ko.client.AddEvent("job-finished", event)
		if err != nil {
			or.LogError(err)
		}
		pack.Recycle()
	}
	return nil
}

func init() {
	pipeline.RegisterPlugin("KeenOutput", func() interface{} {
		return new(KeenOutput)
	})
}
