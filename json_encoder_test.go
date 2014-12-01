package heka_clever_plugins

import (
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"time"
)

func JsonEncoderSpec(c gs.Context) {

	c.Specify("A JsonEncoder", func() {
		encoder := new(JsonEncoder)
		config := encoder.ConfigStruct().(*JsonEncoderConfig)
		supply := make(chan *pipeline.PipelinePack, 1)
		pack := pipeline.NewPipelinePack(supply)
		payload := `{"original":"payload"}`
		pack.Message.SetPayload(payload)
		ts := time.Now()
		pack.Message.SetTimestamp(ts.UnixNano())

		var (
			output []byte
			err    error
		)

		c.Specify("honors append_newlines = false", func() {
			config.AppendNewlines = false
			err = encoder.Init(config)
			c.Expect(err, gs.IsNil)

			output, err = encoder.Encode(pack)
			c.Expect(err, gs.IsNil)
			c.Expect(string(output), gs.Equals, payload)
		})

		c.Specify("works with default config options", func() {
			err = encoder.Init(config)
			c.Expect(err, gs.IsNil)

			output, err = encoder.Encode(pack)
			c.Expect(err, gs.IsNil)
			c.Expect(string(output), gs.Equals, fmt.Sprintln(payload))
		})

		c.Specify("with config, possible to override message fields with a templated value %X%", func() {
			conf := encoder.ConfigStruct().(*JsonEncoderConfig)
			conf.MessageFields = pipeline.MessageTemplate{
				"Payload": "%original%",
			}
			expected := fmt.Sprintf("payload\n")

			err = encoder.Init(conf)
			c.Expect(err, gs.IsNil)

			output, err = encoder.Encode(pack)
			c.Expect(err, gs.IsNil)
			c.Expect(string(output), gs.Equals, expected)
		})

		for _, testPayload := range []string{
			`{}{"original":"payload"}`,
			`non json chars b {"original":"payload"}`,
			`non json chars b {"original":"payload"}`,
		} {
			c.Specify("fails if payload is not JSON parseable string", func() {
				// Cannot have any extra text before/after
				// It may be that a user wants to write other chars and JSON, and has written the format
				// JSON encoder takes JSON string as input, but can output anything based on user's format string.
				pack.Message.SetPayload(testPayload)
				err = encoder.Init(config)
				c.Expect(err, gs.IsNil, true)

				output, err = encoder.Encode(pack)
				c.Expect(err.Error(), gs.Equals, `string to encode is not JSON-parseable: `+testPayload)
				c.Expect(string(output), gs.Equals, "")
			})
		}

		c.Specify("fails if payload is not JSON parseable string", func() {
			// Cannot have any extra text before/after
			// It may be that a user wants to write other chars and JSON, and has written the format
			// JSON encoder takes JSON string as input, but can output anything based on user's format string.
			pack.Message.SetPayload(`non json chars {"original":"payload"}`)
			err = encoder.Init(config)
			c.Expect(err, gs.IsNil, true)

			output, err = encoder.Encode(pack)
			c.Expect(err.Error(), gs.Equals, `string to encode is not JSON-parseable: non json chars {"original":"payload"}`)
			c.Expect(string(output), gs.Equals, "")
		})
	})
}
