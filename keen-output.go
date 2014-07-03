package heka_clever_plugins

import (
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
)

type KeenOutput struct{}

func (ko *KeenOutput) Init(config interface{}) error {
	return nil
}

func (ko *KeenOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	var (
		// e    error
		pack *pipeline.PipelinePack
	)

	for pack = range or.InChan() {
		fmt.Println(pack.Message.Payload)
	}
	return
}

func init() {
	pipeline.RegisterPlugin("KeenOutput", func() interface{} {
		return new(KeenOutput)
	})
}
