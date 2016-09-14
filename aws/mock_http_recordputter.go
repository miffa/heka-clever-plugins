package aws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type mockPutter struct {
	stream   string
	endpoint string
	client   *http.Client
}

func NewMockRecordPutter(stream, endpoint string) RecordPutter {
	return &mockPutter{
		stream:   stream,
		endpoint: endpoint,
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

func (m *mockPutter) PutRecord(record []byte) error {
	return m.PutRecordBatch([][]byte{record})
}

func (m *mockPutter) PutRecordBatch(records [][]byte) error {
	buf := bytes.NewBuffer([]byte{})
	for idx, rec := range records {
		var data map[string]interface{}
		err := json.Unmarshal(rec, &data)
		if err != nil {
			return err
		}

		data["_mock.stream"] = m.stream
		data["_mock.batch"] = idx

		mockRec, err := json.Marshal(data)
		if err != nil {
			return err
		}

		buf.Write(mockRec)
	}

	res, err := m.client.Post(m.endpoint, "application/json", buf)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("Non-200 from firehose output: %s", body)
	}

	return nil
}
