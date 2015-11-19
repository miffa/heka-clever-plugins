// Automatically generated by MockGen. DO NOT EDIT!
// Source: /home/vagrant/go/src/github.com/aws/aws-sdk-go/service/firehose/firehoseiface/interface.go

package aws

import (
	request "github.com/aws/aws-sdk-go/aws/request"
	firehose "github.com/aws/aws-sdk-go/service/firehose"
	gomock "github.com/golang/mock/gomock"
)

// Mock of FirehoseAPI interface
type MockFirehoseAPI struct {
	ctrl     *gomock.Controller
	recorder *_MockFirehoseAPIRecorder
}

// Recorder for MockFirehoseAPI (not exported)
type _MockFirehoseAPIRecorder struct {
	mock *MockFirehoseAPI
}

func NewMockFirehoseAPI(ctrl *gomock.Controller) *MockFirehoseAPI {
	mock := &MockFirehoseAPI{ctrl: ctrl}
	mock.recorder = &_MockFirehoseAPIRecorder{mock}
	return mock
}

func (_m *MockFirehoseAPI) EXPECT() *_MockFirehoseAPIRecorder {
	return _m.recorder
}

func (_m *MockFirehoseAPI) CreateDeliveryStreamRequest(_param0 *firehose.CreateDeliveryStreamInput) (*request.Request, *firehose.CreateDeliveryStreamOutput) {
	ret := _m.ctrl.Call(_m, "CreateDeliveryStreamRequest", _param0)
	ret0, _ := ret[0].(*request.Request)
	ret1, _ := ret[1].(*firehose.CreateDeliveryStreamOutput)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) CreateDeliveryStreamRequest(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateDeliveryStreamRequest", arg0)
}

func (_m *MockFirehoseAPI) CreateDeliveryStream(_param0 *firehose.CreateDeliveryStreamInput) (*firehose.CreateDeliveryStreamOutput, error) {
	ret := _m.ctrl.Call(_m, "CreateDeliveryStream", _param0)
	ret0, _ := ret[0].(*firehose.CreateDeliveryStreamOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) CreateDeliveryStream(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "CreateDeliveryStream", arg0)
}

func (_m *MockFirehoseAPI) DeleteDeliveryStreamRequest(_param0 *firehose.DeleteDeliveryStreamInput) (*request.Request, *firehose.DeleteDeliveryStreamOutput) {
	ret := _m.ctrl.Call(_m, "DeleteDeliveryStreamRequest", _param0)
	ret0, _ := ret[0].(*request.Request)
	ret1, _ := ret[1].(*firehose.DeleteDeliveryStreamOutput)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) DeleteDeliveryStreamRequest(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DeleteDeliveryStreamRequest", arg0)
}

func (_m *MockFirehoseAPI) DeleteDeliveryStream(_param0 *firehose.DeleteDeliveryStreamInput) (*firehose.DeleteDeliveryStreamOutput, error) {
	ret := _m.ctrl.Call(_m, "DeleteDeliveryStream", _param0)
	ret0, _ := ret[0].(*firehose.DeleteDeliveryStreamOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) DeleteDeliveryStream(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DeleteDeliveryStream", arg0)
}

func (_m *MockFirehoseAPI) DescribeDeliveryStreamRequest(_param0 *firehose.DescribeDeliveryStreamInput) (*request.Request, *firehose.DescribeDeliveryStreamOutput) {
	ret := _m.ctrl.Call(_m, "DescribeDeliveryStreamRequest", _param0)
	ret0, _ := ret[0].(*request.Request)
	ret1, _ := ret[1].(*firehose.DescribeDeliveryStreamOutput)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) DescribeDeliveryStreamRequest(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DescribeDeliveryStreamRequest", arg0)
}

func (_m *MockFirehoseAPI) DescribeDeliveryStream(_param0 *firehose.DescribeDeliveryStreamInput) (*firehose.DescribeDeliveryStreamOutput, error) {
	ret := _m.ctrl.Call(_m, "DescribeDeliveryStream", _param0)
	ret0, _ := ret[0].(*firehose.DescribeDeliveryStreamOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) DescribeDeliveryStream(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "DescribeDeliveryStream", arg0)
}

func (_m *MockFirehoseAPI) ListDeliveryStreamsRequest(_param0 *firehose.ListDeliveryStreamsInput) (*request.Request, *firehose.ListDeliveryStreamsOutput) {
	ret := _m.ctrl.Call(_m, "ListDeliveryStreamsRequest", _param0)
	ret0, _ := ret[0].(*request.Request)
	ret1, _ := ret[1].(*firehose.ListDeliveryStreamsOutput)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) ListDeliveryStreamsRequest(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ListDeliveryStreamsRequest", arg0)
}

func (_m *MockFirehoseAPI) ListDeliveryStreams(_param0 *firehose.ListDeliveryStreamsInput) (*firehose.ListDeliveryStreamsOutput, error) {
	ret := _m.ctrl.Call(_m, "ListDeliveryStreams", _param0)
	ret0, _ := ret[0].(*firehose.ListDeliveryStreamsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) ListDeliveryStreams(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ListDeliveryStreams", arg0)
}

func (_m *MockFirehoseAPI) PutRecordRequest(_param0 *firehose.PutRecordInput) (*request.Request, *firehose.PutRecordOutput) {
	ret := _m.ctrl.Call(_m, "PutRecordRequest", _param0)
	ret0, _ := ret[0].(*request.Request)
	ret1, _ := ret[1].(*firehose.PutRecordOutput)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) PutRecordRequest(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PutRecordRequest", arg0)
}

func (_m *MockFirehoseAPI) PutRecord(_param0 *firehose.PutRecordInput) (*firehose.PutRecordOutput, error) {
	ret := _m.ctrl.Call(_m, "PutRecord", _param0)
	ret0, _ := ret[0].(*firehose.PutRecordOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) PutRecord(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PutRecord", arg0)
}

func (_m *MockFirehoseAPI) PutRecordBatchRequest(_param0 *firehose.PutRecordBatchInput) (*request.Request, *firehose.PutRecordBatchOutput) {
	ret := _m.ctrl.Call(_m, "PutRecordBatchRequest", _param0)
	ret0, _ := ret[0].(*request.Request)
	ret1, _ := ret[1].(*firehose.PutRecordBatchOutput)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) PutRecordBatchRequest(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PutRecordBatchRequest", arg0)
}

func (_m *MockFirehoseAPI) PutRecordBatch(_param0 *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
	ret := _m.ctrl.Call(_m, "PutRecordBatch", _param0)
	ret0, _ := ret[0].(*firehose.PutRecordBatchOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) PutRecordBatch(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "PutRecordBatch", arg0)
}

func (_m *MockFirehoseAPI) UpdateDestinationRequest(_param0 *firehose.UpdateDestinationInput) (*request.Request, *firehose.UpdateDestinationOutput) {
	ret := _m.ctrl.Call(_m, "UpdateDestinationRequest", _param0)
	ret0, _ := ret[0].(*request.Request)
	ret1, _ := ret[1].(*firehose.UpdateDestinationOutput)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) UpdateDestinationRequest(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "UpdateDestinationRequest", arg0)
}

func (_m *MockFirehoseAPI) UpdateDestination(_param0 *firehose.UpdateDestinationInput) (*firehose.UpdateDestinationOutput, error) {
	ret := _m.ctrl.Call(_m, "UpdateDestination", _param0)
	ret0, _ := ret[0].(*firehose.UpdateDestinationOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFirehoseAPIRecorder) UpdateDestination(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "UpdateDestination", arg0)
}