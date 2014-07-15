package heka_clever_plugins

import (
	"code.google.com/p/gomock/gomock"
	"github.com/mozilla-services/heka/pipeline"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"github.com/stretchr/testify/mock"
	"testing"
)

const (
	EPOCH_TS = 1358969429.508
)

func getEmptyKeenOutputPack() *pipeline.PipelinePack {
	recycleChan := make(chan *pipeline.PipelinePack, 1)
	pack := pipeline.NewPipelinePack(recycleChan)
	pack.Message.SetType("KeenOutput")
	pack.Decoded = true
	pack.Message.SetTimestamp(int64(EPOCH_TS * 1e9))
	return pack
}

type SuccessfulTestCase struct {
	Description        string
	MessagePayload     string
	IsEventDataCorrect func(map[string]interface{}) bool
}

type ErrorTestCase struct {
	Description          string
	MessagePayload       string
	ExpectedErrorType    string
	ExpectedErrorMessage string
}

var universalT *testing.T

func KeenOutputSpec(c gs.Context) {
	ctrl := gomock.NewController(universalT)
	defer ctrl.Finish()

	c.Specify("A KeenOutput", func() {

		successTests := []SuccessfulTestCase{
			SuccessfulTestCase{
				"successfully records a valid job-finished message",
				"{\"JobType\":\"nyc_aris\",\"SystemId\":\"1234567890abcdefghijklmn\",\"TimeCreated\":\"2014-07-03T23:35:24.000Z\",\"Duration\":38900,\"Success\":true,\"Message\":\"\"}",
				func(eventData map[string]interface{}) bool {
					return eventData["JobType"] == "nyc_aris" &&
						eventData["SystemId"] == "1234567890abcdefghijklmn" &&
						eventData["TimeCreated"] == "2014-07-03T23:35:24.000Z" &&
						eventData["Duration"] == float64(38900) &&
						eventData["Success"] == true &&
						eventData["Message"] == ""
				},
			},
		}

		errorTests := []ErrorTestCase{
			ErrorTestCase{
				"logs an error but does not crash when the message payload is not valid JSON",
				"not json",
				"*json.SyntaxError",
				"invalid character 'o' in literal null (expecting 'u')",
			},
		}

		for _, test := range successTests {
			oth := NewOutputTestHelper(universalT, ctrl)
			output := new(KeenOutput)
			output.Init(&KeenOutputConfig{})
			mockClient := MockKeenClient{mock.Mock{}}
			output.client = &mockClient

			inChan := make(chan *pipeline.PipelinePack, 1)
			oth.MockOutputRunner.EXPECT().On("InChan").Return(inChan)
			mockClient.EXPECT().On("AddEvent", "job-finished", mock.Anything).Return(nil)

			pack := getEmptyKeenOutputPack()
			pack.Message.SetPayload(test.MessagePayload)
			inChan <- pack
			close(inChan)

			output.Run(oth.MockOutputRunner, oth.MockHelper)

			ExpectCall(universalT, &mockClient.mock, "AddEvent with expected JSON", "AddEvent",
				func(args []interface{}) bool {
					if len(args) != 2 {
						return false
					}
					eventData, ok := args[1].(map[string]interface{})
					return ok && test.IsEventDataCorrect(eventData)
				})
		}

		for _, test := range errorTests {
			oth := NewOutputTestHelper(universalT, ctrl)
			output := new(KeenOutput)
			output.Init(&KeenOutputConfig{})
			mockClient := MockKeenClient{mock.Mock{}}
			output.client = &mockClient

			inChan := make(chan *pipeline.PipelinePack, 1)
			oth.MockOutputRunner.EXPECT().On("InChan").Return(inChan)
			oth.MockOutputRunner.EXPECT().On("LogError", mock.AnythingOfType(test.ExpectedErrorType)).Return()

			pack := getEmptyKeenOutputPack()
			pack.Message.SetPayload(test.MessagePayload)
			inChan <- pack
			close(inChan)

			output.Run(oth.MockOutputRunner, oth.MockHelper)

			ExpectCall(universalT, &oth.MockOutputRunner.mock, "Log correct error", "LogError",
				func(args []interface{}) bool {
					if len(args) != 1 {
						return false
					}
					err, ok := args[0].(error)
					return ok && err.Error() == test.ExpectedErrorMessage
				})

			oth.MockOutputRunner.EXPECT().AssertExpectations(universalT)
		}
	})
}

func TestAllSpecs(t *testing.T) {
	universalT = t
	r := gs.NewRunner()
	r.Parallel = false

	r.AddSpec(KeenOutputSpec)

	gs.MainGoTest(r, t)
}
