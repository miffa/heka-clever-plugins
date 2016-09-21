package batcher

import (
	"time"
)

type message []byte

type Sync interface {
	Flush(batch []message)
}

type Batcher interface {
	// Interval at which accumulated messages should be bulk put to
	// firehose (default 1 second).
	FlushInterval(dur time.Duration)
	// Number of messages that triggers a put to firehose
	// default to 10
	FlushCount(count int)
	// Number of messages that triggers a put to firehose
	// default to 1mb (1024 * 1024)
	FlushSize(size int)

	// Messages for length 0 are ignored
	SendMessage(msg message)
	Flush()
}

type batcher struct {
	flushInterval time.Duration
	flushCount    int
	flushSize     int

	sync      Sync
	msgChan   chan<- message
	flushChan chan<- struct{}
}

func New(sync Sync) *batcher {
	msgChan := make(chan message)
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

func (b *batcher) SendMessage(msg message) {
	if len(msg) > 0 {
		b.msgChan <- msg
	}
}

func (b *batcher) Flush() {
	b.flushChan <- struct{}{}
}

func (b *batcher) batchSize(batch []message) int {
	total := 0
	for _, msg := range batch {
		total += len(msg)
	}

	return total
}

func (b *batcher) sendBatch(batch []message) []message {
	if len(batch) > 0 {
		b.sync.Flush(batch)
	}
	return []message{}
}

func (b *batcher) startBatcher(msgChan <-chan message, flushChan <-chan struct{}) {
	batch := []message{}

	for {
		select {
		case <-time.After(b.flushInterval):
			batch = b.sendBatch(batch)
		case <-flushChan:
			batch = b.sendBatch(batch)
		case msg := <-msgChan:
			size := b.batchSize(batch)
			if b.flushSize < size+len(msg) {
				batch = b.sendBatch(batch)
			}

			batch = append(batch, msg)
			if b.flushCount <= len(batch) || b.flushSize <= b.batchSize(batch) {
				batch = b.sendBatch(batch)
			}
		}
	}
}
