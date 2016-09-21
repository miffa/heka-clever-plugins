package batcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockSync struct {
	flushChan chan struct{}
	batches   [][]message
}

func NewMockSync() *mockSync {
	return &mockSync{
		flushChan: make(chan struct{}, 1),
		batches:   [][]message{},
	}
}

func (m *mockSync) Flush(batch []message) {
	m.batches = append(m.batches, batch)
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

	batcher.SendMessage(message("hihi"))
	batcher.SendMessage(message("heyhey"))
	batcher.SendMessage(message("hmmhmm")) // Shouldn't be in first batch

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(2, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
	assert.Equal("heyhey", string(sync.batches[0][1]))

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

	batcher.SendMessage(message("hihi"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))

	batcher.SendMessage(message("heyhey"))
	batcher.SendMessage(message("yoyo"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(2, len(sync.batches[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))

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

	batcher.SendMessage(message("hellohello")) // Large messages are sent immediately

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hellohello", string(sync.batches[0][0]))

	batcher.SendMessage(message("heyhey")) // Batcher tries not to exceed size limit
	batcher.SendMessage(message("hihi"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(2, len(sync.batches))
	assert.Equal(1, len(sync.batches[1]))
	assert.Equal("heyhey", string(sync.batches[1][0]))

	batcher.SendMessage(message("yoyo")) // At this point "hihi" is in the batch

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(3, len(sync.batches))
	assert.Equal(2, len(sync.batches[2]))
	assert.Equal("hihi", string(sync.batches[2][0]))
	assert.Equal("yoyo", string(sync.batches[2][1]))

	batcher.SendMessage(message("okok"))

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

	batcher.SendMessage(message("hihi"))

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.Error(err)

	batcher.Flush()

	err = sync.waitForFlush(time.Millisecond * 10)
	assert.NoError(err)

	assert.Equal(1, len(sync.batches))
	assert.Equal(1, len(sync.batches[0]))
	assert.Equal("hihi", string(sync.batches[0][0]))
}
