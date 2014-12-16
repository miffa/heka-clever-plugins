package heka_clever_plugins

import (
	"fmt"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	gs "github.com/rafrombrc/gospec/src/gospec"
)

func FieldEncoderSpec(c gs.Context) {

	c.Specify("A FieldEncoder", func() {
		encoder := new(FieldEncoder)
		config := encoder.ConfigStruct().(*FieldEncoderConfig)
		supply := make(chan *pipeline.PipelinePack, 1)
		pack := pipeline.NewPipelinePack(supply)
		payload := `original payload`
		pack.Message.SetPayload(payload)
		ts := time.Now()
		pack.Message.SetTimestamp(ts.UnixNano())
		message.NewStringField(pack.Message, "name_foo", "value_foo")

		var (
			output []byte
			err    error
		)

		c.Specify("outputs original payload with default config", func() {
			err = encoder.Init(config)
			c.Expect(err, gs.IsNil)

			output, err = encoder.Encode(pack)
			c.Expect(err, gs.IsNil)
			c.Expect(string(output), gs.Equals, fmt.Sprint(payload))
		})

		c.Specify("possible to override message Payload with a templated value %X%", func() {
			conf := encoder.ConfigStruct().(*FieldEncoderConfig)
			conf.MessageFields = pipeline.MessageTemplate{
				"Payload": "%name_foo%",
			}
			expected := fmt.Sprintf("value_foo")

			err = encoder.Init(conf)
			c.Expect(err, gs.IsNil)

			output, err = encoder.Encode(pack)
			c.Expect(err, gs.IsNil)
			c.Expect(string(output), gs.Equals, expected)
		})

	})
}
