package heka_clever_plugins

import (
	"time"

	cw "github.com/Clever/heka-clever-plugins/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/mozilla-services/heka/pipeline"
)

type CloudwatchLogsOutput struct {
	client   *cloudwatchlogs.CloudWatchLogs
	logGroup string
	// batches maps from stream name to LogBatch
	batches map[string]*cw.LogBatch
}

type CloudwatchLogsOutputConfig struct {
	Region   string `toml:"region"`
	LogGroup string `toml:"log_group"`
}

func (f *CloudwatchLogsOutput) ConfigStruct() interface{} {
	return &CloudwatchLogsOutputConfig{}
}

func (f *CloudwatchLogsOutput) Init(rawConfig interface{}) error {
	config := rawConfig.(*CloudwatchLogsOutputConfig)
	f.client = cloudwatchlogs.New(session.New(), aws.NewConfig().WithRegion(config.Region))
	return nil
}

func (f *CloudwatchLogsOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) error {
	for pack := range or.InChan() {
		payload := pack.Message.GetPayload()
		hostname := pack.Message.GetHostname()
		timestamp := time.Unix(0, pack.Message.GetTimestamp()).Second() * 1000
		pack.Recycle(nil)

		// Write immediately
		batch := cw.NewLogBatch(f.logGroup, hostname)

		err := batch.TryAddLog(payload, int64(timestamp))
		if err != nil {
			or.LogError(err)
			continue
		}

		err = batch.Put(f.client)
		if err != nil {
			or.LogError(err)
			continue
		}
	}
	return nil
}

func init() {
	pipeline.RegisterPlugin("CloudwatchLogsOutput", func() interface{} {
		return new(CloudwatchLogsOutput)
	})
}
