package batcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type batch [][]byte

type mockSync struct {
	flushChan chan struct{}
	batches   []batch
	metadatas [][]interface{}
}

func NewMockSync() *mockSync {
	return &mockSync{
		flushChan: make(chan struct{}, 1),
		batches:   []batch{},
	}
}

func (m *mockSync) Flush(b [][]byte, metadata []interface{}) {
	m.batches = append(m.batches, batch(b))
	m.metadatas = append(m.metadatas, metadata)
	m.flushChan <- struct{}{}
}

func (m *mockSync) waitForFlush(timeout time.Duration) error {
	select {
	case <-m.flushChan:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("The flush never came.  I waited %s.", timeout.String())
	}
}

func TestBatchingByCount(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Hour)
	batcher.FlushCount(2)

	assert.NoError(batcher.Send([]byte("hihi"), "meta-hi"))
	assert.NoError(batcher.Send([]byte("heyhey"), "meta-hey"))
	assert.NoError(batcher.Send([]byte("hmmhmm"), "meta-hmm")) // Shouldn't be in first batch

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(2, len(sync.batches[0]))
	assert.Equal(len(sync.batches), len(sync.metadatas))
	assert.Equal(len(sync.batches[0]), len(sync.metadatas[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
	assert.Equal("heyhey", string(sync.batches[0][1]))
	assert.Equal("meta-hi", sync.metadatas[0][0].(string))
	assert.Equal("meta-hey", sync.metadatas[0][1].(string))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestBatchingByTime(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Millisecond)
	batcher.FlushCount(2000000)

	assert.NoError(batcher.Send([]byte("hihi"), "meta-hi"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal(len(sync.batches), len(sync.metadatas))
	assert.Equal(len(sync.batches[0]), len(sync.metadatas[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
	assert.Equal("meta-hi", sync.metadatas[0][0].(string))

	assert.NoError(batcher.Send([]byte("heyhey"), "meta-hey"))
	assert.NoError(batcher.Send([]byte("yoyo"), "meta-yo"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(2, len(sync.batches[1]))
	assert.Equal(len(sync.batches), len(sync.metadatas))
	assert.Equal(len(sync.batches[1]), len(sync.metadatas[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))
	assert.Equal("meta-hey", sync.metadatas[1][0].(string))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestBatchingBySize(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Hour)
	batcher.FlushCount(2000000)
	batcher.FlushSize(8)

	// Large messages are sent immediately
	assert.NoError(batcher.Send([]byte("hellohello"), "meta-hello"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal(len(sync.batches), len(sync.metadatas))
	assert.Equal(len(sync.batches[0]), len(sync.metadatas[0]))
	assert.Equal("hellohello", string(sync.batches[0][0]))
	assert.Equal("meta-hello", sync.metadatas[0][0].(string))

	// Batcher tries not to exceed size limit
	assert.NoError(batcher.Send([]byte("heyhey"), "meta-hey"))
	assert.NoError(batcher.Send([]byte("hihi"), "meta-hi"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(1, len(sync.batches[1]))
	assert.Equal(len(sync.batches), len(sync.metadatas))
	assert.Equal(len(sync.batches[1]), len(sync.metadatas[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))
	assert.Equal("meta-hey", sync.metadatas[1][0].(string))

	assert.NoError(batcher.Send([]byte("yoyo"), "meta-yo")) // At this point "hihi" is in the batch

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(3, len(sync.batches))
	assert.Equal(2, len(sync.batches[2]))
	assert.Equal(len(sync.batches), len(sync.metadatas))
	assert.Equal(len(sync.batches[2]), len(sync.metadatas[2]))
	assert.Equal("hihi", string(sync.batches[2][0]))
	assert.Equal("yoyo", string(sync.batches[2][1]))
	assert.Equal("meta-hi", sync.metadatas[2][0].(string))
	assert.Equal("meta-yo", sync.metadatas[2][1].(string))

	assert.NoError(batcher.Send([]byte("okok"), ""))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)
}

func TestFlushing(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)
	batcher.FlushInterval(time.Hour)
	batcher.FlushCount(2000000)

	assert.NoError(batcher.Send([]byte("hihi"), "meta-hi"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)

	batcher.Flush()

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal(len(sync.batches), len(sync.metadatas))
	assert.Equal(len(sync.batches[0]), len(sync.metadatas[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
	assert.Equal("meta-hi", sync.metadatas[0][0].(string))
}

func TestSendingEmpty(t *testing.T) {
	var err error
	assert := assert.New(t)

	sync := NewMockSync()
	batcher := New(sync)

	err = batcher.Send([]byte{}, "")
	assert.Error(err)
}
