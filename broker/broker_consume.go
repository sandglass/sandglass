package broker

import (
	"context"
	"io"
	"strings"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
)

func (b *Broker) Consume(ctx context.Context, topicName, partition, consumerGroup, consumerName string, fn func(msg *sgproto.Message) error) error {
	offsetTopic := b.getTopic(ConsumerOffsetTopicName)
	if offsetTopic == nil {
		return ErrTopicNotFound
	}

	pk := partitionKey(topicName, partition, consumerGroup)
	offsetPartition := offsetTopic.ChoosePartitionForKey(pk)
	if offsetPartition == nil {
		return ErrPartitionNotFound
	}

	leader := b.getPartitionLeader(ConsumerOffsetTopicName, offsetPartition.Id)
	if leader == nil {
		return ErrNoLeaderFound
	}

	topic := b.getTopic(topicName)
	if topic == nil {
		return ErrTopicNotFound
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
	if p == nil {
		return ErrPartitionNotFound
	}
	cg := b.getConsumerGroup(topicName, partition, consumerGroup)
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

func (b *Broker) getConsumerGroup(topicName, partition string, name string) *ConsumerGroup {
	key := strings.Join([]string{topicName, partition, name}, string(storage.Separator))
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
