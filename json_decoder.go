package heka_clever_plugins

import (
	"encoding/json"
	"fmt"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"strings"
)

type JsonDecoderConfig struct {
	MessageFields pipeline.MessageTemplate `toml:"message_fields"`
}

type JsonDecoder struct {
	dRunner       pipeline.DecoderRunner
	messageFields pipeline.MessageTemplate
}

func (kvd *JsonDecoder) ConfigStruct() interface{} {
	return new(JsonDecoderConfig)
}

func (kvd *JsonDecoder) Init(config interface{}) (err error) {
	conf := config.(*JsonDecoderConfig)
	kvd.messageFields = make(pipeline.MessageTemplate)
	if conf.MessageFields != nil {
		for field, action := range conf.MessageFields {
			kvd.messageFields[field] = action
		}
	}
	return
}

func (kvd *JsonDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	jsonMap, jsonString, err := ParseJson(pack.Message.GetPayload(), true)
	if err != nil {
		return nil, err
	}
	// Overwrite Payload with just the JSON string
	*pack.Message.Payload = jsonString

	// For keys that have string values, write to message.Fields
	for name, val := range jsonMap {
		// null in JSON becomes nil, which panics when message.NewField interally calls val.Type()
		if val != nil {
			if f, err := message.NewField(name, val, ""); err == nil {
				pack.Message.AddField(f)
			}
		}
	}

	if err = kvd.messageFields.PopulateMessage(pack.Message, nil); err != nil {
		return
	}
	return []*pipeline.PipelinePack{pack}, nil
}

func (kvd *JsonDecoder) SetDecoderRunner(dr pipeline.DecoderRunner) {
	kvd.dRunner = dr
}

func init() {
	pipeline.RegisterPlugin("JsonDecoder", func() interface{} {
		return new(JsonDecoder)
	})
}

// ParseJson takes a json string and unmarshals it into a struct
func ParseJson(s string, findJsonSubstring bool) (jsonMap map[string]interface{}, jsonString string, err error) {
	// Naively find start of JSON by looking for first '{'
	if findJsonSubstring {
		// Just parse one line
		if lineBreakIdx := strings.Index(s, "\n"); lineBreakIdx > 0 {
			s = s[:lineBreakIdx] + "\n"
		}
		if leftBraceIndex := strings.Index(s, "{"); leftBraceIndex >= 0 {
			s = s[leftBraceIndex:]
		} else {
			return nil, "", fmt.Errorf("could not find a json substring within log line: %s", s)
		}
	}

	if err := json.Unmarshal([]byte(s), &jsonMap); err != nil {
		return nil, "", err
	}
	return jsonMap, s, nil
}
