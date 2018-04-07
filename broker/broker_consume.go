package broker

import (
	"context"
	"io"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
)

func (b *Broker) Consume(ctx context.Context, req *sgproto.ConsumeFromGroupRequest, fn func(msg *sgproto.Message) error) error {
	offsetTopic := b.getTopic(ConsumerOffsetTopicName)
	if offsetTopic == nil {
		return ErrTopicNotFound
	}

	pk := partitionKey(req.Topic, req.Partition, req.Channel, req.ConsumerGroupName)
	offsetPartition := offsetTopic.ChoosePartitionForKey(pk)
	if offsetPartition == nil {
		return ErrPartitionNotFound
	}

	leader := b.getPartitionLeader(ConsumerOffsetTopicName, offsetPartition.Id)
	if leader == nil {
		return ErrNoLeaderFound
	}

	topic := b.getTopic(req.Topic)
	if topic == nil {
		return ErrTopicNotFound
	}

	if leader.Name != b.Name() {
		b.WithFields(logrus.Fields{
			"leader": leader.Name,
		}).Debugf("consuming from remote")
		stream, err := leader.ConsumeFromGroup(ctx, req)
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

	p := topic.GetPartition(req.Partition)
	if p == nil {
		return ErrPartitionNotFound
	}
	cg := b.getConsumerGroup(req.Topic, req.Partition, req.Channel, req.ConsumerGroupName)
	msgCh, closeCh, err := cg.Consume(req.ConsumerName)
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

func (b *Broker) getConsumerGroup(topicName, partition, channel string, name string) *ConsumerGroup {
	key := strings.Join([]string{topicName, partition, channel, name}, string(storage.Separator))
	c := b.getConsumer(key)
	if c != nil {
		return c
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	c = NewConsumerGroup(b, topicName, partition, channel, name)
	b.consumers[key] = c

	return c
}

func (b *Broker) getConsumer(key string) *ConsumerGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.consumers[key]
}
