package heka_clever_plugins

import (
	"encoding/json"
	"fmt"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"regexp"
	"strings"
)

type KeyvalDecoderConfig struct {
}

type KeyvalDecoder struct {
	dRunner       pipeline.DecoderRunner
	messageFields pipeline.MessageTemplate
}

func (kvd *KeyvalDecoder) ConfigStruct() interface{} {
	return new(KeyvalDecoderConfig)
}

func (kvd *KeyvalDecoder) Init(config interface{}) (err error) {
	_ = config.(*KeyvalDecoderConfig)
	return
}

func (kvd *KeyvalDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	title, jsonString, err := ParseTitleAndKeyvals(pack.Message.GetPayload())
	if err != nil {
		return nil, err
	}

	message.NewStringField(pack.Message, "Title", title)
	pack.Message.SetPayload(jsonString)
	// pack.Message.AddField(f)
	// if err = kvd.messageFields.PopulateMessage(pack.Message, nil); err != nil {
	// 	return
	// }
	return []*pipeline.PipelinePack{pack}, nil
}

func (kvd *KeyvalDecoder) SetDecoderRunner(dr pipeline.DecoderRunner) {
	kvd.dRunner = dr
}

func init() {
	pipeline.RegisterPlugin("KeyvalDecoder", func() interface{} {
		return new(KeyvalDecoder)
	})
}

// ParseTitleAndKeyvals takes a string of form "TITLE a=b c=d ..." and returns its title and a stringified JSON of its key-val pairs
func ParseTitleAndKeyvals(s string) (title string, jsonString string, err error) {
	split := strings.SplitN(s, " ", 2)
	if len(split) == 1 {
		return s, "{}", nil
	}
	title = split[0]
	jsonString, err = keyvalToJsonString(split[1])
	if err != nil {
		return "", "", err
	}
	return title, jsonString, nil
}

// keyvalToJsonString takes a string of form "a=b c=d" and returns a stringified JSON repesentation `{"a":"b,"c":"d"}`
// Order of arguments is not preserved in outputted JSON.
func keyvalToJsonString(s string) (string, error) {
	items := map[string]string{}
	for {
		key, val, rest, err := findKeyValPair(s)
		if err != nil {
			return "", err
		}
		// Couldn't find a key-val pair
		if key == "" {
			break
		}
		items[key] = val
		s = rest
	}
	b, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	output := fmt.Sprintf(`%s`, string(b))
	return output, nil
}

// findKeyValPair takes a string and looks from left for a substring matching "key=val". Returns key, val, and rest of the string.
// Returns key = "" if cannot find a pair.
// Allows the val to be double-quote delimited.
func findKeyValPair(s string) (key string, val string, rest string, err error) {
	s = strings.TrimLeft(s, " ")
	re := regexp.MustCompile(`^(?P<Key>[^\s=]+)[=](["](?P<QuotedValue>[^"]+)["]|(?P<NonQuotedValue>\S+))`)

	// Is the regexp matched at all?
	pair := re.FindString(s)
	rest = s[len(pair):]
	if pair == "" {
		// Couldn't find a pair, but there's more to parse
		if rest != "" {
			return "", "", "", fmt.Errorf("couldnt parse '%s'", rest)
		}
		return "", "", "", nil
	}

	// Find the submatches within the key-val pair
	match := re.FindStringSubmatch(pair)
	captures := make(map[string]string)
	for i, name := range re.SubexpNames() {
		// Ignore the whole regexp match and unnamed groups
		if i == 0 || name == "" {
			continue
		}
		captures[name] = match[i]
	}

	key, _ = captures["Key"]
	if capVal, ok := captures["QuotedValue"]; ok && capVal != "" {
		val = capVal
	} else if capVal, ok := captures["NonQuotedValue"]; ok && capVal != "" {
		val = capVal
	}

	return key, val, rest, nil
}
