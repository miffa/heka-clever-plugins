package heka_clever_plugins

import (
	"encoding/json"
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

// Encode accepts a JSON payload and optionally rewrites fields via Populate message.
// Given MessageFields in config, will overwrite values  using string-interpolation,
// similarly to PayloadRegexDecoder except with values from the JSON instead of Regex matches.
//
// For example, given a JSON-payload of {"name":"world"}, one might write in the heka.toml
// 		[encoderName.message_fields]
// 		Payload="Hello, %name%"
// which would output: "Hello, world"
func (pe *JsonEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	// Parse payload. Fail unless JSON string.
	payload := pack.Message.GetPayload()
	jsonMap, jsonString, err := ParseJson(payload, false)
	if err != nil || (strings.TrimLeft(payload, " ") != jsonString) {
		return nil, fmt.Errorf("string to encode is not JSON-parseable: %s", payload)
	}

	subs, err := forceInterfacesToStrings(jsonMap)
	if err != nil {
		return nil, err
	}

	// interpolates the string using keys from subs
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

// forceInterfacesToStrings converts map[string]interface{} to map[string]string
// Returns an error if fails to json marshal one of the interface{} values.
func forceInterfacesToStrings(jsonMap map[string]interface{}) (subs map[string]string, err error) {
	subs = map[string]string{}
	for key, val := range jsonMap {
		formattedVal, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		stringVal := string(formattedVal)
		if castVal, ok := val.(string); ok {
			// JSON marshalled strings are surrounded in quotes - remove those
			// by just using the string the results from casting the interface{}.(string)
			stringVal = castVal
		}
		subs[key] = stringVal
	}
	return subs, nil
}

func init() {
	pipeline.RegisterPlugin("JsonEncoder", func() interface{} {
		return new(JsonEncoder)
	})
}
