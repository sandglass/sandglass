package broker

import (
	"context"
	"sync"
	"time"

	"github.com/celrenheit/sandflake"

	"github.com/celrenheit/sandglass/sgproto"
	"golang.org/x/sync/errgroup"
)

type ConsumerGroup struct {
	broker    *Broker
	topic     string
	partition string
	name      string
	mu        sync.RWMutex
	receivers []*receiver
}

func NewConsumerGroup(b *Broker, topic, partition, name string) *ConsumerGroup {
	return &ConsumerGroup{
		broker:    b,
		name:      name,
		topic:     topic,
		partition: partition,
	}
}

type receiver struct {
	name   string
	msgCh  chan *sgproto.Message
	doneCh chan struct{}
}

func (c *ConsumerGroup) register(consumerName string) *receiver {
	r := c.getReceiver(consumerName)
	if r != nil {
		return r
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	r = &receiver{
		name:   consumerName,
		msgCh:  make(chan *sgproto.Message),
		doneCh: make(chan struct{}),
	}
	c.receivers = append(c.receivers, r)

	if len(c.receivers) == 1 {
		go c.consumeLoop()
	}

	return r
}

func (c *ConsumerGroup) consumeLoop() {
	from, err := c.broker.LastOffset(context.TODO(), c.topic, c.partition, c.name, "", sgproto.LastOffsetRequest_Commited)
	if err != nil {
		c.broker.Debug("got error when fetching last committed offset: %v ", err)
		return
	}

	msgCh := make(chan *sgproto.Message)
	var group errgroup.Group
	group.Go(func() error {
		now := sandflake.NewID(time.Now().UTC(), sandflake.MaxID.WorkerID(), sandflake.MaxID.Sequence(), sandflake.MaxID.RandomBytes())
		return c.broker.FetchRange(context.Background(), c.topic, c.partition, from, now, func(m *sgproto.Message) error {
			// skip the first if it is the same as the starting point
			if from == m.Offset {
				return nil
			}

			msgCh <- m

			return nil
		})
	})

	go func() {
		group.Wait()
		close(msgCh)
	}()

	var i int
loop:
	for m := range msgCh {
		isAcknowledged, err := c.broker.isAcknoweldged(context.TODO(), c.topic, c.partition, c.name, m.Offset)
		if err != nil {
			c.broker.Debug("STOPPING CONSUMPTION got err: %v", err)
			break
		}
		if isAcknowledged {
			// skip already acknowledged message
			continue
		}

		// select receiver
	selectreceiver:
		c.mu.RLock()
		r := c.receivers[i%len(c.receivers)]
		c.mu.RUnlock()

		select {
		case <-r.doneCh:
			if c.removeConsumer(r.name) {
				c.mu.RLock()
				l := len(c.receivers)
				c.mu.RUnlock()

				if l == 0 {
					break loop
				}

				goto selectreceiver // select another receiver
			}
		case r.msgCh <- m:
		}
		i++
	}

	c.mu.Lock()
	for _, r := range c.receivers {
		close(r.msgCh)
		close(r.doneCh)
	}
	c.receivers = c.receivers[:0]
	c.mu.Unlock()
}

func (c *ConsumerGroup) removeConsumer(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, r := range c.receivers {
		if r.name == name {
			c.receivers = append(c.receivers[:i], c.receivers[i+1:]...)
			return true
		}
	}

	return false
}

func (c *ConsumerGroup) getReceiver(consumerName string) *receiver {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, r := range c.receivers {
		if r.name == consumerName {
			return r
		}
	}

	return nil
}

func (c *ConsumerGroup) Consume(consumerName string) (<-chan *sgproto.Message, chan<- struct{}, error) {
	r := c.register(consumerName)

	return r.msgCh, r.doneCh, nil
}
