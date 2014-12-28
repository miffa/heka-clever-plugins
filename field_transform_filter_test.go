package heka_clever_plugins

import (
	"fmt"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	gs "github.com/rafrombrc/gospec/src/gospec"
)

type testSpec struct {
	name            string
	messageTemplate pipeline.MessageTemplate
	expectedPayload string
}

func FieldTransformFilterSpec(c gs.Context) {

	c.Specify("A FieldTransformFilter", func() {
		var (
			output []byte
			err    error
		)

		filter := new(FieldTransformFilter)
		config := encoder.ConfigStruct().(*FieldTranformFilterConfig)
		supply := make(chan *pipeline.PipelinePack, 1)
		pack := pipeline.NewPipelinePack(supply)
		payload := `original payload`
		pack.Message.SetPayload(payload)
		ts := time.Now()
		pack.Message.SetTimestamp(ts.UnixNano())
		message.NewStringField(pack.Message, "string_field", "string")
		message.NewIntField(pack.Message, "int_field", 1, "")
		f, err := message.NewField("bool_field", true, "")
		c.Expect(err, gs.IsNil)
		pack.Message.AddField(f)

		c.Specify("outputs original payload with default config", func() {
			err = encoder.Init(config)
			c.Expect(err, gs.IsNil)

			output, err = encoder.Encode(pack)
			c.Expect(err, gs.IsNil)
			c.Expect(string(output), gs.Equals, fmt.Sprint(payload))
		})

		for _, test := range []testSpec{
			testSpec{
				name: "string",
				messageTemplate: pipeline.MessageTemplate{
					"Payload": "%string_field%",
				},
				expectedPayload: "string",
			},
			testSpec{
				name: "int",
				messageTemplate: pipeline.MessageTemplate{
					"Payload": "%int_field%",
				},
				expectedPayload: "1",
			},
			testSpec{
				name: "bool",
				messageTemplate: pipeline.MessageTemplate{
					"Payload": "%bool_field%",
				},
				expectedPayload: "true",
			},
			testSpec{
				name: "nonexistent",
				messageTemplate: pipeline.MessageTemplate{
					"Payload": "%nonexistent_field%",
				},
				expectedPayload: "<nonexistent_field>",
			},
		} {
			c.Specify(fmt.Sprintf("possible to override message Payload with a templated '%s' value", test.name), func() {
				conf := filter.ConfigStruct().(*FieldTransformFilterConfig)
				conf.MessageFields = test.messageTemplate

				// Load message config
				err = filter.Init(conf)
				c.Expect(err, gs.IsNil)

				// Encode the pack and verify the payload has been overridden
				output, err = filter.Run(pack)
				c.Expect(err, gs.IsNil)
				actual := string(output)
				expected := fmt.Sprintf(test.expectedPayload)
				c.Expect(actual, gs.Equals, expected)
			})
		}

	})
}
