package heka_clever_plugins

import (
	"fmt"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type FieldEncoder struct {
	config        *FieldEncoderConfig
	MessageFields pipeline.MessageTemplate
}

type FieldEncoderConfig struct {
	MessageFields pipeline.MessageTemplate `toml:"message_fields"`
}

func (pe *FieldEncoder) ConfigStruct() interface{} {
	return &FieldEncoderConfig{}
}

func (pe *FieldEncoder) Init(config interface{}) (err error) {
	conf := config.(*FieldEncoderConfig)
	pe.config = conf
	pe.MessageFields = make(pipeline.MessageTemplate)
	if conf.MessageFields != nil {
		for field, action := range conf.MessageFields {
			pe.MessageFields[field] = action
		}
	}
	return
}

// Encode accepts a PipelinePack and rewrites fields via PopulateMessage.
// Given MessageFields in config, will overwrite values using string-interpolation,
// with a syntax similar to PayloadRegexDecoder, butwith values from the message's Fields
//
// For example, a message with the Field[name] with value "world", one might
// write in the heka.toml:
// 		[encoderName.message_fields]
// 		Payload="Hello, %name%"
// which would output: "Hello, world"
func (pe *FieldEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	subs := getSubsFromMessageFields(pack)

	// interpolates strings using keys from subs. Mutates pack.Message, so it's
	// possible to update fields other than the Payload.
	// https://github.com/mozilla-services/heka/blob/dev/pipeline/message_template.go#100
	if err = pe.MessageFields.PopulateMessage(pack.Message, subs); err == nil {
		output = []byte(*pack.Message.Payload)
	} else {
		return nil, fmt.Errorf("failed to populate message with subs")
	}

	return
}

// getSubsFromMessageFields creates a map[string]string from pack.Message.Fields
func getSubsFromMessageFields(pack *pipeline.PipelinePack) (subs map[string]string) {
	subs = map[string]string{}
	for _, field := range pack.Message.Fields {
		name := field.GetName()
		copiedField := message.CopyField(field)
		// if a value cannot be typecast to a string, it will
		// be replaced with the empty string
		stringifiedValue, _ := copiedField.GetValue().(string)
		subs[name] = stringifiedValue
	}
	return subs
}

func init() {
	pipeline.RegisterPlugin("FieldEncoder", func() interface{} {
		return new(FieldEncoder)
	})
}
