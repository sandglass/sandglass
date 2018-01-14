package sg

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
)

type ProducerMessage struct {
	ConsumeIn time.Duration // TODO: waiting for server to handle this without manually setting an offset
	Value     []byte
}

type Producer interface {
	Produce(ctx context.Context, msg *ProducerMessage) error
	ProduceMessages(ctx context.Context, msgs []*ProducerMessage) error
	Close() error
}

type ProducerConfig struct {
	partition     string
	batchSize     int
	flushInterval time.Duration
	timeout       time.Duration
}

func newDefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		batchSize:     50,
		flushInterval: 10 * time.Second,
		timeout:       30 * time.Second,
	}
}

type ProducerOption func(*ProducerConfig)

func WithProducerPartition(partition string) func(*ProducerConfig) {
	return func(c *ProducerConfig) {
		c.partition = partition
	}
}

func WithProducerBatchSize(batchSize int) func(*ProducerConfig) {
	return func(c *ProducerConfig) {
		c.batchSize = batchSize
	}
}

func WithProducerFlushInterval(interval time.Duration) func(*ProducerConfig) {
	return func(c *ProducerConfig) {
		c.flushInterval = interval
	}
}

type SyncProducer struct {
	client           *Client
	topic, partition string
	conf             *ProducerConfig
}

func NewSyncProducer(client *Client, topic string, opts ...ProducerOption) *SyncProducer {
	return &SyncProducer{
		client: client,
		topic:  topic,
		conf:   genOptions(opts),
	}
}

func (p *SyncProducer) Produce(ctx context.Context, msg *ProducerMessage) error {
	return p.ProduceMessages(ctx, []*ProducerMessage{msg})
}

func (p *SyncProducer) ProduceMessages(ctx context.Context, msgs []*ProducerMessage) error {
	req := generateRequestFromPrducerMessages(p.topic, p.partition, msgs)
	_, err := p.client.client.Produce(ctx, req)
	return err
}

func (p *SyncProducer) Close() error {
	return nil
}

type AsyncProducer struct {
	client *Client
	topic  string

	msgCh  chan []*ProducerMessage
	wg     sync.WaitGroup
	stopCh chan struct{}

	conf *ProducerConfig
}

func genOptions(opts []ProducerOption) *ProducerConfig {
	conf := newDefaultProducerConfig()

	for _, opt := range opts {
		opt(conf)
	}

	return conf
}

func NewAsyncProducer(client *Client, topic string, opts ...ProducerOption) *AsyncProducer {
	p := &AsyncProducer{
		client: client,
		topic:  topic,
		conf:   genOptions(opts),
	}

	p.wg.Add(1)
	go p.loop()

	return p
}

func (p *AsyncProducer) Produce(ctx context.Context, msg *ProducerMessage) error {
	return p.ProduceMessages(ctx, []*ProducerMessage{msg})
}

func (p *AsyncProducer) ProduceMessages(ctx context.Context, msgs []*ProducerMessage) error {
	p.msgCh <- msgs
	return nil
}

func (p *AsyncProducer) loop() {
	defer p.wg.Done()
	produce := func(msgs []*ProducerMessage) error {
		req := generateRequestFromPrducerMessages(p.topic, p.conf.partition, msgs)
		ctx := context.Background()
		if p.conf.timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, p.conf.timeout)
			defer cancel()
		}

		_, err := p.client.client.Produce(ctx, req)
		return err
	}

	buf := make([]*ProducerMessage, 0, p.conf.batchSize)
loop:
	for {
		select {
		case msgs := <-p.msgCh:
			buf = append(buf, msgs...)
			if len(buf) == p.conf.batchSize {
				err := produce(buf)
				if err != nil {
					// TODO: handle this error
					log.Printf("unable to produce %d messages, got error: %v\n", len(buf), err)
				}
				buf = buf[:0]
			}
		case <-p.stopCh:
			break loop
		case <-time.After(p.conf.flushInterval):
			if len(buf) > 0 {
				err := produce(buf)
				if err != nil {
					// TODO: handle this error
					log.Printf("unable to produce %d messages, got error: %v\n", len(buf), err)
				}
				buf = buf[:0]
			}
		}
	}

	// flush
	if len(buf) > 0 {
		err := produce(buf)
		if err != nil {
			// TODO: handle this error
			log.Printf("unable to produce %d messages, got error: %v\n", len(buf), err)
		}
		buf = buf[:0]
	}
}

func (p *AsyncProducer) Close() error {
	close(p.stopCh)
	p.wg.Wait()
	return nil
}

func generateRequestFromPrducerMessages(topic, partition string, msgs []*ProducerMessage) *sgproto.ProduceMessageRequest {
	req := &sgproto.ProduceMessageRequest{
		Topic:     topic,
		Partition: partition,
		Messages:  make([]*sgproto.Message, len(msgs)),
	}

	for i, msg := range msgs {
		req.Messages[i] = &sgproto.Message{
			Value: msg.Value,
		}
	}
	return req
}

var _ Producer = (*SyncProducer)(nil)
var _ Producer = (*AsyncProducer)(nil)
