package batcher

import (
	"fmt"
	"time"
)

type Sync interface {
	Flush(batch [][]byte, metadata []interface{})
}

type Batcher interface {
	// Interval at which accumulated messages should be bulk put to
	// firehose (default 1 second).
	FlushInterval(dur time.Duration)
	// Number of messages that triggers a push to firehose
	// default to 10
	FlushCount(count int)
	// Size of batch that triggers a push to firehose
	// default to 1mb (1024 * 1024)
	FlushSize(size int)

	// Messages for length 0 are ignored
	Send(msg []byte, metadata interface{}) error
	Flush()
}

type batcher struct {
	flushInterval time.Duration
	flushCount    int
	flushSize     int

	sync      Sync
	msgChan   chan<- messagePack
	flushChan chan<- struct{}
}

type messagePack struct {
	msg      []byte
	metadata interface{}
}

func New(sync Sync) *batcher {
	msgChan := make(chan messagePack, 100)
	flushChan := make(chan struct{})

	b := &batcher{
		flushCount:    10,
		flushInterval: time.Second,
		flushSize:     1024 * 1024,

		sync:      sync,
		msgChan:   msgChan,
		flushChan: flushChan,
	}

	go b.startBatcher(msgChan, flushChan)

	return b
}

func (b *batcher) FlushInterval(dur time.Duration) {
	b.flushInterval = dur
}

func (b *batcher) FlushCount(count int) {
	b.flushCount = count
}

func (b *batcher) FlushSize(size int) {
	b.flushSize = size
}

func (b *batcher) Send(msg []byte, metadata interface{}) error {
	if len(msg) <= 0 {
		return fmt.Errorf("Empty messages can't be sent")
	}

	b.msgChan <- messagePack{msg, metadata}
	return nil
}

func (b *batcher) Flush() {
	b.flushChan <- struct{}{}
}

func (b *batcher) batchSize(batch [][]byte) int {
	total := 0
	for _, msg := range batch {
		total += len(msg)
	}

	return total
}

func (b *batcher) sendBatch(batch [][]byte, metadata []interface{}) ([][]byte, []interface{}) {
	if len(batch) > 0 {
		b.sync.Flush(batch, metadata)
	}
	return [][]byte{}, []interface{}{}
}

func (b *batcher) startBatcher(msgChan <-chan messagePack, flushChan <-chan struct{}) {
	batch := [][]byte{}
	metadata := []interface{}{}

	for {
		select {
		case <-time.After(b.flushInterval):
			batch, metadata = b.sendBatch(batch, metadata)
		case <-flushChan:
			batch, metadata = b.sendBatch(batch, metadata)
		case pack := <-msgChan:
			size := b.batchSize(batch)
			if b.flushSize < size+len(pack.msg) {
				batch, metadata = b.sendBatch(batch, metadata)
			}

			batch = append(batch, pack.msg)
			metadata = append(metadata, pack.metadata)

			if b.flushCount <= len(batch) || b.flushSize <= b.batchSize(batch) {
				batch, metadata = b.sendBatch(batch, metadata)
			}
		}
	}
}
