package broker

import (
	"bytes"
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gogo/protobuf/proto"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/topic"
)

func (b *Broker) mark(ctx context.Context, req *sgproto.MarkRequest) (bool, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	p := topic.ChoosePartitionForKey(partitionKey(req.Topic, req.Partition, req.Channel, req.ConsumerGroup))

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
			Key:           partitionKey(req.Topic, req.Partition, req.Channel, req.ConsumerGroup),
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

func (b *Broker) lastOffset(ctx context.Context, topicName, partitionName, channel string, consumerGroup string, kind sgproto.MarkKind) (sgproto.Offset, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	pk := partitionKey(topicName, partitionName, channel, consumerGroup)
	p := topic.ChoosePartitionForKey(pk)

	n := b.getPartitionLeader(ConsumerOffsetTopicName, p.Id)
	if n == nil {
		return sgproto.Nil, ErrNoLeaderFound
	}

	if n.Name != b.Name() {
		res, err := n.LastOffset(ctx, &sgproto.LastOffsetRequest{
			Topic:         topicName,
			Partition:     partitionName,
			Channel:       channel,
			ConsumerGroup: consumerGroup,
			Kind:          kind,
		})
		if err != nil {
			return sgproto.Nil, err
		}

		return res.Offset, nil
	}

	lastKind := byte(kind)

	return b.last(p, channel, pk, lastKind)
}

func (b *Broker) GetMarkStateMessage(ctx context.Context, req *sgproto.GetMarkRequest) (*sgproto.Message, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	pk := partitionKey(req.Topic, req.Partition, req.Channel, req.ConsumerGroup)
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

	msg, err := p.GetMessage(req.Channel, sgproto.Nil, key, nil)
	if err != nil {
		return nil, err
	}

	if msg == nil {
		return nil, status.Error(codes.NotFound, "mark state not found")
	}

	return msg, nil
}

func (b *Broker) last(p *topic.Partition, channel string, pk []byte, kind byte) (sgproto.Offset, error) {
	msg, err := p.GetMessage(channel, sgproto.Nil, pk, []byte{kind})
	if err != nil {
		return sgproto.Nil, err
	}

	if msg == nil {
		return sgproto.Nil, nil
	}

	if len(msg.Value) == 0 {
		return sgproto.Nil, fmt.Errorf("LastCommitedOffset malformed value '%v'", msg.Value)
	}

	return msg.Offset, nil
}

func (b *Broker) isAcknoweldged(ctx context.Context, topicName, partition, channel string, consumerGroup string, offset sgproto.Offset) (bool, error) {
	topic := b.getTopic(ConsumerOffsetTopicName)
	if topic == nil {
		return false, ErrTopicNotFound
	}
	pk := partitionKey(topicName, partition, channel, consumerGroup)
	p := topic.ChoosePartitionForKey(pk)
	clusterKey := generateClusterKey(offset, sgproto.MarkKind_Acknowledged)
	return b.hasKeyInPartition(ctx, ConsumerOffsetTopicName, p, channel, pk, clusterKey)
}

func partitionKey(topicName, partitionName, channel, consumerGroup string) []byte {
	return bytes.Join([][]byte{
		[]byte("offsets"),
		[]byte(topicName),
		[]byte(partitionName),
		[]byte(channel),
		[]byte(consumerGroup),
	}, storage.Separator)
}

func generateClusterKey(offset sgproto.Offset, kind sgproto.MarkKind) []byte {
	return bytes.Join([][]byte{
		offset.Bytes(),
		[]byte{byte(kind)},
	}, storage.Separator)
}
