package heka_clever_plugins

import (
	"fmt"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type FieldTransformFilter struct {
	config        *FieldTransformFilterConfig
	MessageFields pipeline.MessageTemplate
}

type FieldTransformFilterConfig struct {
	MessageFields pipeline.MessageTemplate `toml:"message_fields"`
	Type_         string                   `toml:"type"`
}

func (pe *FieldTransformFilter) ConfigStruct() interface{} {
	return &FieldTransformFilterConfig{}
}

func (pe *FieldTransformFilter) Init(config interface{}) (err error) {
	conf := config.(*FieldTransformFilterConfig)
	pe.config = conf
	pe.MessageFields = make(pipeline.MessageTemplate)
	if conf.MessageFields != nil {
		for field, action := range conf.MessageFields {
			pe.MessageFields[field] = action
		}
	}
	return
}

func (pe *FieldTransformFilter) Run(fr FilterRunner, h PluginHelper) (err error) {

	var (
		pack *PipelinePack
	)

	inChan = fr.InChan()
	for pack = range inChan {
		newPack = h.PipelinePack(pack.MsgLoopCount)

		if newPack != nil {

            pe.MessageFields.

		} else {
			fr.LogError(fmt.Errorf("failed to populate message with subs"))
		}

		pack.Recycle()
	}

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
		subs[name] = fmt.Sprint(copiedField.GetValue())
	}
	return subs
}

func init() {
	pipeline.RegisterPlugin("FieldTransformFilter", func() interface{} {
		return new(FieldTransformFilter)
	})
}
