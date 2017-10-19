package broker

import (
	"context"
	"io"
	"strings"

	"github.com/celrenheit/sandglass/topic"

	"github.com/celrenheit/sandglass/sgproto"
)

func (b *Broker) Consume(ctx context.Context, topicName, partition, consumerGroup, consumerName string, fn func(msg *sgproto.Message) error) error {
	topic := b.getTopic(topicName)
	if topic == nil {
		return ErrTopicNotFound
	}

	leader := b.getPartitionLeader(topicName, partition)
	if leader == nil {
		return ErrNoLeaderFound
	}

	if leader.Name != b.Name() {
		b.Debug("consuming from remote: ", leader.Name)
		stream, err := leader.ConsumeFromGroup(ctx, &sgproto.ConsumeFromGroupRequest{
			Topic:             topicName,
			Partition:         partition,
			ConsumerGroupName: consumerGroup,
			ConsumerName:      consumerName,
		})
		if err != nil {
			return err
		}

		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			err = fn(msg)
			if err != nil {
				return err
			}
		}

		return nil
	}

	p := topic.GetPartition(partition)
	cg := b.getConsumerGroup(topicName, p, consumerGroup)
	msgCh, closeCh, err := cg.Consume(consumerName)
	if err != nil {
		return err
	}

	for msg := range msgCh {
		if err := fn(msg); err != nil {
			close(closeCh)
			return err
		}
	}

	return nil
}

func (b *Broker) getConsumerGroup(topicName string, partition *topic.Partition, name string) *ConsumerGroup {
	key := strings.Join([]string{topicName, partition.Id, name}, "/")
	c := b.getConsumer(key)
	if c != nil {
		return c
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	c = NewConsumerGroup(b, topicName, partition, name)
	b.consumers[key] = c

	return c
}

func (b *Broker) getConsumer(key string) *ConsumerGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.consumers[key]
}
