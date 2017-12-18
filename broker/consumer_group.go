package broker

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/celrenheit/sandflake"
	"github.com/gogo/protobuf/proto"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"golang.org/x/sync/errgroup"
)

var (
	// TODO: make these variables configurable
	RedeliveryTimeout  = 10 * time.Second
	MaxRedeliveryCount = 5
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
	defer func() { // close receivers for whatever reason
		c.mu.Lock()
		for _, r := range c.receivers {
			close(r.msgCh)
			close(r.doneCh)
		}
		c.receivers = c.receivers[:0]
		c.mu.Unlock()
	}()

	lastCommited, err := c.broker.LastOffset(context.TODO(), c.topic, c.partition, c.name, "", sgproto.MarkKind_Commited)
	if err != nil {
		c.broker.Debug("got error when fetching last committed offset: %v ", err)
		return
	}

	lastConsumed, err := c.broker.LastOffset(context.TODO(), c.topic, c.partition, c.name, "", sgproto.MarkKind_Consumed)
	if err != nil {
		c.broker.Debug("got error when fetching last committed offset: %v ", err)
		return
	}

	msgCh := make(chan *sgproto.Message)
	var group errgroup.Group

	if !lastCommited.Equal(lastConsumed) {
		group.Go(func() error {
			var (
				lastMessage *sgproto.Message
				committed   = false
			)
			req := &sgproto.FetchRangeRequest{
				Topic:     c.topic,
				Partition: c.partition,
				From:      lastCommited,
				To:        lastConsumed,
			}

			commit := func(offset sandflake.ID) {
				_, err := c.broker.Commit(context.TODO(), c.topic, c.partition, c.name, "", lastMessage.Offset)
				if err != nil {
					c.broker.Debug("unable to commit")
				}
			}

			i := 0
			err := c.broker.FetchRange(context.TODO(), req, func(m *sgproto.Message) error {
				if m.Offset.Equal(lastCommited) { // skip first item, since it is already committed
					lastMessage = m
					return nil
				}
				i++

				markedMsg, err := c.broker.GetMarkStateMessage(context.TODO(), &sgproto.GetMarkRequest{
					Topic:         c.topic,
					Partition:     c.partition,
					ConsumerGroup: c.name,
					Offset:        m.Offset,
				})
				if err != nil {
					s, ok := status.FromError(err)
					if !ok || s.Code() != codes.NotFound {
						return err
					}
				}

				var state sgproto.MarkState
				if markedMsg != nil {
					err := proto.Unmarshal(markedMsg.Value, &state)
					if err != nil {
						return err
					}
				}

				// advance commit offset
				// if we only got acked messages before
				if !committed && lastMessage != nil {
					if state.Kind != sgproto.MarkKind_Acknowledged {
						// we might commit in a goroutine, we can redo this the next time we consume
						if !lastMessage.Offset.Equal(lastCommited) {
							commit(lastMessage.Offset)
						}
						committed = true
					} else if i%10000 == 0 {
						go commit(lastMessage.Offset)
					}
				}
				lastMessage = m

				if shouldRedeliver(m.Index, state) {
					msgCh <- m // deliver

					// those calls should be batched
					if state.Kind == sgproto.MarkKind_Unknown {
						// TODO: Should we mark this consumed?
						_, err := c.broker.MarkConsumed(context.Background(), c.topic, c.partition, c.name, "NOT SET", m.Offset)
						if err != nil {
							c.broker.Debug("error while acking message for the first redilvery", err)
							return err
						}
					} else {
						state.DeliveryCount++

						if int(state.DeliveryCount) >= MaxRedeliveryCount {
							// Mark the message as ACKed
							// TODO: produce this a dead letter queue
							state.Kind = sgproto.MarkKind_Acknowledged
						}

						markedMsg.Value, err = proto.Marshal(&state)
						if err != nil {
							return err
						}

						// TODO: Should handle this in higher level method
						t := c.broker.GetTopic(ConsumerOffsetTopicName)
						p := t.ChoosePartitionForKey(markedMsg.Key)
						markedMsg.ClusteringKey = generateClusterKey(m.Offset, state.Kind)

						if _, err := c.broker.Produce(context.TODO(), &sgproto.ProduceMessageRequest{
							Topic:     ConsumerOffsetTopicName,
							Partition: p.Id,
							Messages:  []*sgproto.Message{markedMsg},
						}); err != nil {
							return err
						}
					}
				}

				return nil
			})
			if err != nil {
				return err
			}

			if !committed && lastMessage != nil {
				commit(lastMessage.Offset)
			}

			return nil
		})
	}
	group.Go(func() error {
		now := sandflake.NewID(time.Now().UTC(), sandflake.MaxID.WorkerID(), sandflake.MaxID.Sequence(), sandflake.MaxID.RandomBytes())
		req := &sgproto.FetchRangeRequest{
			Topic:     c.topic,
			Partition: c.partition,
			From:      lastConsumed,
			To:        now,
		}

		return c.broker.FetchRange(context.TODO(), req, func(m *sgproto.Message) error {
			// skip the first if it is the same as the starting point
			if lastConsumed == m.Offset {
				return nil
			}

			msgCh <- m

			return nil
		})
	})

	go func() {
		err := group.Wait()
		if err != nil {
			c.broker.Info("error in consumeLoop: %v", err)
		}
		close(msgCh)
	}()

	var i int
	var m *sgproto.Message
loop:
	for m = range msgCh {
		// select receiver
	selectreceiver:
		i++
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
	}

	if m != nil && !m.Offset.Equal(lastConsumed) {
		_, err := c.broker.MarkConsumed(context.TODO(), c.topic, c.partition, c.name, "REMOVE THIS", m.Offset)
		if err != nil {
			c.broker.Debug("unable to mark as consumed: %v", err)
		}
	}
}

func shouldRedeliver(index sandflake.ID, state sgproto.MarkState) bool {
	switch state.Kind {
	case sgproto.MarkKind_NotAcknowledged:
		return true
	case sgproto.MarkKind_Consumed, sgproto.MarkKind_Unknown: // inflight
		dur := RedeliveryTimeout
		if state.DeliveryCount > 0 {
			dur *= time.Duration(state.DeliveryCount)
		}
		return index.Time().Add(dur).Before(time.Now().UTC())
	case sgproto.MarkKind_Acknowledged, sgproto.MarkKind_Commited:
		return false
	default:
		panic("unknown markkind: " + state.Kind.String())
	}

	return false
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
