package heka_clever_plugins

import (
	. "github.com/mozilla-services/heka/pipeline"
	pipeline_ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mozilla-services/heka/pipelinemock"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"testing"
)

func JsonDecoderSpec(c gs.Context) {
	baseJsonPayload := `{"title":"TEST_TITLE","source":"TEST_SOURCE","level":"TEST_LEVEL"}`
	c.Specify("A JsonDecoder", func() {
		c.Specify("parses a json into Fields", func() {
			decoder := new(JsonDecoder)
			conf := decoder.ConfigStruct().(*JsonDecoderConfig)
			payload := string(baseJsonPayload + "\n")
			fnVerifyOutput := func(c gs.Context, pack *PipelinePack) {
				fieldAsserts := map[string]string{
					"title":  "TEST_TITLE",
					"source": "TEST_SOURCE",
					"level":  "TEST_LEVEL",
				}
				for field, expectedFieldValue := range fieldAsserts {
					title, ok := pack.Message.GetFieldValue(field)
					c.Expect(ok, gs.Equals, true)
					c.Expect(title, gs.Equals, expectedFieldValue)
				}

				c.Expect(pack.Message.GetPayload(), gs.Equals, payload)
				// Type shouldn't be set, because JsonDecoderConfig is empty
				c.Expect(pack.Message.GetType(), gs.Equals, "")
			}
			decodeMessageAndVerifyOutput(c, conf, payload, fnVerifyOutput)
		})

		c.Specify("allows setting MessageFields via config", func() {
			decoder := new(JsonDecoder)
			conf := decoder.ConfigStruct().(*JsonDecoderConfig)
			myType := "customTypeName"
			conf.MessageFields = MessageTemplate{"Type": myType}
			payload := string(`{}` + "\n")
			fnVerifyOutput := func(c gs.Context, pack *PipelinePack) {
				c.Expect(pack.Message.GetPayload(), gs.Equals, payload)
				// Type should be set to user configured value
				c.Expect(pack.Message.GetType(), gs.Equals, myType)
			}
			decodeMessageAndVerifyOutput(c, conf, payload, fnVerifyOutput)

		})

		c.Specify("finds json in message if preceded by non-JSON chars, and re-writes payload to just JSON", func() {
			decoder := new(JsonDecoder)
			conf := decoder.ConfigStruct().(*JsonDecoderConfig)
			payload := string(`other text not json ` + baseJsonPayload + "\n")
			fnVerifyOutput := func(c gs.Context, pack *PipelinePack) {
				fieldAsserts := map[string]string{
					"title":  "TEST_TITLE",
					"source": "TEST_SOURCE",
					"level":  "TEST_LEVEL",
				}
				for field, expectedFieldValue := range fieldAsserts {
					title, ok := pack.Message.GetFieldValue(field)
					c.Expect(ok, gs.Equals, true)
					c.Expect(title, gs.Equals, expectedFieldValue)
				}

				c.Expect(pack.Message.GetPayload(), gs.Equals, baseJsonPayload+"\n")
				// Type shouldn't be set, because JsonDecoderConfig is empty
				c.Expect(pack.Message.GetType(), gs.Equals, "")
			}
			decodeMessageAndVerifyOutput(c, conf, payload, fnVerifyOutput)
		})

		c.Specify("fails if multiple '{' in message", func() {
			decoder := new(JsonDecoder)
			conf := decoder.ConfigStruct().(*JsonDecoderConfig)
			payload := string(`{ ` + baseJsonPayload + "\n")
			fnVerifyOutput := func(c gs.Context, pack *PipelinePack) {
				// message fields should be nil, since message wasn't decoded successfully.
				for _, field := range []string{"title", "source", "level"} {
					_, ok := pack.Message.GetFieldValue(field)
					c.Expect(ok, gs.Equals, false)
				}

				// expect message to be unchanged
				c.Expect(pack.Message.GetPayload(), gs.Equals, payload)
				c.Expect(pack.Message.GetType(), gs.Equals, "")
			}
			decodeMessageAndVerifyOutput(c, conf, payload, fnVerifyOutput)
		})
	})
}

// packVerifier makes assertions on a pack (after being processed by a decoder)
type packVerifier func(c gs.Context, pack *PipelinePack)

// decodeMessageAndVerifyOutput takes a decoder conf, message payload, and a fn -> the fn is a number of
// assertions to verify that the message after decoding is as expected.
func decodeMessageAndVerifyOutput(c gs.Context, conf *JsonDecoderConfig, payload string, fn packVerifier) {
	t := &pipeline_ts.SimpleT{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 1. Initialize test decoder
	decoder := new(JsonDecoder)
	err := decoder.Init(conf)
	c.Assume(err, gs.IsNil)
	dRunner := pipelinemock.NewMockDecoderRunner(ctrl)
	decoder.SetDecoderRunner(dRunner)

	// 2. Set payload to be tested, and decode it
	supply := make(chan *PipelinePack, 1)
	pack := NewPipelinePack(supply)
	pack.Message.SetPayload(payload)
	_, err = decoder.Decode(pack)

	// 3. Assert outcome of decoding
	fn(c, pack)
	pack.Zero()
}

func Test_KeyvalSpecs(t *testing.T) {
	universalT = t
	r := gs.NewRunner()
	r.Parallel = false

	r.AddSpec(JsonDecoderSpec)

	gs.MainGoTest(r, t)
}
