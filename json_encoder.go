package heka_clever_plugins

import (
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"strings"
)

type JsonEncoder struct {
	config        *JsonEncoderConfig
	MessageFields pipeline.MessageTemplate
}

type JsonEncoderConfig struct {
	AppendNewlines bool                     `toml:"append_newlines"`
	Payload        string                   `toml:"payload"`
	MessageFields  pipeline.MessageTemplate `toml:"message_fields"`
}

func (pe *JsonEncoder) ConfigStruct() interface{} {
	return &JsonEncoderConfig{
		AppendNewlines: true,
		Payload:        "",
	}
}

func (pe *JsonEncoder) Init(config interface{}) (err error) {
	conf := config.(*JsonEncoderConfig)
	pe.config = conf
	pe.MessageFields = make(pipeline.MessageTemplate)
	if conf.MessageFields != nil {
		for field, action := range conf.MessageFields {
			pe.MessageFields[field] = action
		}
	}
	return
}

// Encode parses a JSON payload. Given MessageFields in config, will overwrite values
// using string-interpolation, similarly to PayloadRegexDecoder except with values from
// the JSON instead of Regex matches.
func (pe *JsonEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	// Parse payload. Fail unless JSON string.
	payload := pack.Message.GetPayload()
	jsonMap, jsonString, err := ParseJson(payload, false)
	if err != nil || (strings.TrimLeft(payload, " ") != jsonString) {
		return nil, fmt.Errorf("string to encode is not JSON-parseable: %s", payload)
	}

	// For any field in the JSON that is a string, allow using its value
	// as a "sub". Given JSON-payload of  {"name":"world"}, one might write
	// 		[encoderName.message_fields]
	// 		Payload="Hello, %name%"
	subs := map[string]string{}
	// allow subbing with string representation of values
	// TODO: Refactor this to be shared in encoder/decoder
	for key, val := range jsonMap {
		stringVal, ok := val.(string)
		if ok {
			subs[key] = stringVal
		} else {
			// TODO: json dump, force to string
		}
	}

	// Interpolates the string using keys from subs
	// https://github.com/mozilla-services/heka/blob/dev/pipeline/message_template.go#100
	if err = pe.MessageFields.PopulateMessage(pack.Message, subs); err == nil {
		output = []byte(*pack.Message.Payload)
	} else {
		return nil, fmt.Errorf("failed to populate message with keys from JSON")
	}

	if pe.config.AppendNewlines {
		output = append(output, '\n')
	}

	return
}

func init() {
	pipeline.RegisterPlugin("JsonEncoder", func() interface{} {
		return new(JsonEncoder)
	})
}
