package broker

import (
	"bytes"
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/celrenheit/sandflake"
	"github.com/gogo/protobuf/proto"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/topic"
)

func (b *Broker) Acknowledge(ctx context.Context, topicName, partitionName, consumerGroup string, offsets ...sandflake.ID) (bool, error) {
	return b.Mark(ctx, &sgproto.MarkRequest{
		Topic:         topicName,
		Partition:     partitionName,
		ConsumerGroup: consumerGroup,
		Offsets:       offsets,
		State: &sgproto.MarkState{
			Kind: sgproto.MarkKind_Acknowledged,
		},
	})
}

func (b *Broker) NotAcknowledge(ctx context.Context, topicName, partitionName, consumerGroup string, offsets ...sandflake.ID) (bool, error) {
	return b.Mark(ctx, &sgproto.MarkRequest{
		Topic:         topicName,
		Partition:     partitionName,
		ConsumerGroup: consumerGroup,
		Offsets:       offsets,
		State: &sgproto.MarkState{
			Kind: sgproto.MarkKind_NotAcknowledged,
		},
	})
}

func (b *Broker) Commit(ctx context.Context, topicName, partitionName, consumerGroup string, offsets ...sandflake.ID) (bool, error) {
	return b.Mark(ctx, &sgproto.MarkRequest{
		Topic:         topicName,
		Partition:     partitionName,
		ConsumerGroup: consumerGroup,
		Offsets:       offsets,
		State: &sgproto.MarkState{
			Kind: sgproto.MarkKind_Commited,
		},
	})
}

func (b *Broker) MarkConsumed(ctx context.Context, topicName, partitionName, consumerGroup string, offsets ...sandflake.ID) (bool, error) {
	return b.Mark(ctx, &sgproto.MarkRequest{
		Topic:         topicName,
		Partition:     partitionName,
		ConsumerGroup: consumerGroup,
		Offsets:       offsets,
		State: &sgproto.MarkState{
			Kind: sgproto.MarkKind_Consumed,
		},
	})
}

func (b *Broker) Mark(ctx context.Context, req *sgproto.MarkRequest) (bool, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	p := topic.ChoosePartitionForKey(partitionKey(req.Topic, req.Partition, req.ConsumerGroup))

	n := b.getPartitionLeader(ConsumerOffsetTopicName, p.Id)
	if n == nil {
		return false, ErrNoLeaderFound
	}

	if n.Name != b.Name() {
		res, err := n.Mark(ctx, req)
		if err != nil {
			return false, err
		}

		return res.Success, nil
	}

	if req.State == nil {
		req.State = &sgproto.MarkState{}
	}

	value, err := proto.Marshal(req.State)
	if err != nil {
		return false, err
	}

	msgs := make([]*sgproto.Message, 0, len(req.Offsets))
	for _, offset := range req.Offsets {
		msgs = append(msgs, &sgproto.Message{
			Offset:        offset,
			Key:           partitionKey(req.Topic, req.Partition, req.ConsumerGroup),
			ClusteringKey: generateClusterKey(offset, req.State.Kind),
			Value:         value,
		})
	}

	res, err := b.Produce(ctx, &sgproto.ProduceMessageRequest{
		Topic:     ConsumerOffsetTopicName,
		Partition: p.Id,
		Messages:  msgs,
	})
	return res != nil, err
}

func (b *Broker) LastOffset(ctx context.Context, topicName, partitionName, consumerGroup string, kind sgproto.MarkKind) (sandflake.ID, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	pk := partitionKey(topicName, partitionName, consumerGroup)
	p := topic.ChoosePartitionForKey(pk)

	n := b.getPartitionLeader(ConsumerOffsetTopicName, p.Id)
	if n == nil {
		return sandflake.Nil, ErrNoLeaderFound
	}

	if n.Name != b.Name() {
		res, err := n.LastOffset(ctx, &sgproto.LastOffsetRequest{
			Topic:         topicName,
			Partition:     partitionName,
			ConsumerGroup: consumerGroup,
			Kind:          kind,
		})
		if err != nil {
			return sandflake.Nil, err
		}

		return res.Offset, nil
	}

	lastKind := byte(kind)

	return b.last(p, pk, lastKind)
}

func (b *Broker) GetMarkStateMessage(ctx context.Context, req *sgproto.GetMarkRequest) (*sgproto.Message, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	pk := partitionKey(req.Topic, req.Partition, req.ConsumerGroup)
	p := topic.ChoosePartitionForKey(pk)

	n := b.getPartitionLeader(ConsumerOffsetTopicName, p.Id)
	if n == nil {
		return nil, ErrNoLeaderFound
	}

	if n.Name != b.Name() {
		res, err := n.GetMarkStateMessage(ctx, req)
		if err != nil {
			return nil, err
		}

		return res, nil
	}

	key := generatePrefixConsumerOffsetKey(pk, req.Offset)

	msg, err := p.GetMessage(sandflake.Nil, key, nil)
	if err != nil {
		return nil, err
	}

	if msg == nil {
		return nil, status.Error(codes.NotFound, "mark state not found")
	}

	return msg, nil
}

func (b *Broker) last(p *topic.Partition, pk []byte, kind byte) (sandflake.ID, error) {
	msg, err := p.GetMessage(sandflake.Nil, pk, []byte{kind})
	if err != nil {
		return sandflake.Nil, err
	}

	if msg == nil {
		return sandflake.Nil, nil
	}

	if len(msg.Value) == 0 {
		return sandflake.Nil, fmt.Errorf("LastCommitedOffset malformed value '%v'", msg.Value)
	}

	return msg.Offset, nil
}

func (b *Broker) isAcknoweldged(ctx context.Context, topicName, partition, consumerGroup string, offset sandflake.ID) (bool, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	if topic == nil {
		return false, ErrTopicNotFound
	}
	pk := partitionKey(topicName, partition, consumerGroup)
	p := topic.ChoosePartitionForKey(pk)
	clusterKey := generateClusterKey(offset, sgproto.MarkKind_Acknowledged)
	return b.hasKeyInPartition(ctx, ConsumerOffsetTopicName, p, pk, clusterKey)
}

func partitionKey(topicName, partitionName, consumerGroup string) []byte {
	return bytes.Join([][]byte{
		[]byte("offsets"),
		[]byte(topicName),
		[]byte(partitionName),
		[]byte(consumerGroup),
	}, storage.Separator)
}

func generateClusterKey(offset sandflake.ID, kind sgproto.MarkKind) []byte {
	return bytes.Join([][]byte{
		offset.Bytes(),
		[]byte{byte(kind)},
	}, storage.Separator)
}
