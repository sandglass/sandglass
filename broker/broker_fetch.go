package broker

import (
	"bytes"
	"context"
	"io"

	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"

	"github.com/sandglass/sandglass-grpc/go/sgproto"
	"github.com/sandglass/sandglass/storage"
	"github.com/sandglass/sandglass/topic"
	"github.com/grpc/grpc-go/status"
)

func (b *Broker) FetchRangeFn(ctx context.Context, req *sgproto.FetchRangeRequest, fn func(msg *sgproto.Message) error) error {
	topic := b.getTopic(req.Topic)
	if topic == nil {
		return ErrTopicNotFound
	}

	if req.Partition == "" {
		return ErrNoPartitionSet
	}

	leader := b.getPartitionLeader(topic.Name, req.Partition)
	if leader.Name != b.Name() {
		stream, err := leader.FetchRange(ctx, req)
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
	return p.ForRange(req.Channel, req.From, req.To, fn)
}

func (b *Broker) fetchFromSync(topicName, partition string, from []byte, fn func(msg *sgproto.Message) error) error {
	topic := b.getTopic(topicName)
	if topic == nil {
		return ErrTopicNotFound
	}

	if partition == "" {
		return ErrNoPartitionSet
	}

	p := topic.GetPartition(partition)
	if p == nil {
		return ErrPartitionNotFound
	}

	return p.RangeFromWAL(from, fn)
}

func (b *Broker) EndOfLog(ctx context.Context, req *sgproto.EndOfLogRequest) (*sgproto.EndOfLogReply, error) {
	topic := b.getTopic(req.Topic)
	if topic == nil {
		return nil, ErrTopicNotFound
	}

	if req.Partition == "" {
		return nil, ErrNoPartitionSet
	}

	p := topic.GetPartition(req.Partition)
	var index uint64
	msg, err := p.EndOfLog()
	if err != nil {
		return nil, err
	}

	if msg != nil {
		index = msg.Index
	}

	return &sgproto.EndOfLogReply{
		Index: index,
	}, nil
}

func (b *Broker) Get(ctx context.Context, req *sgproto.GetRequest) (*sgproto.Message, error) {
	t := b.getTopic(req.Topic)
	var p *topic.Partition
	if req.Partition != "" {
		p = t.GetPartition(req.Partition)
	} else {
		p = t.ChoosePartitionForKey(req.Key)
	}
	return b.getFromPartition(ctx, req.Topic, p, req.Channel, req.Key)
}

func (b *Broker) hasKey(ctx context.Context, topicName, partition, channel string, key, clusterKey []byte) (bool, error) {
	t := b.getTopic(topicName)
	var p *topic.Partition
	if partition != "" {
		p = t.GetPartition(partition)
	} else {
		p = t.ChoosePartitionForKey(key)
	}
	return b.hasKeyInPartition(ctx, topicName, p, channel, key, clusterKey)
}

func (b *Broker) getFromPartition(ctx context.Context, topic string, p *topic.Partition, channel string, key []byte) (*sgproto.Message, error) {
	leader := b.getPartitionLeader(topic, p.Id)
	if leader == nil {
		return nil, ErrNoLeaderFound
	}

	if leader.Name != b.Name() {
		b.WithFields(logrus.Fields{
			"leader": leader.Name,
			"key":    key,
		}).Debugf("fetch key remotely")
		return leader.GetByKey(ctx, &sgproto.GetRequest{
			Topic:     topic,
			Partition: p.Id,
			Channel:   channel,
			Key:       key,
		})
	}

	msg, err := p.GetMessage(channel, sgproto.Nil, key, nil)
	if err != nil {
		return nil, err
	}

	if msg == nil {
		return nil, status.Error(codes.NotFound, "message not found")
	}

	return msg, nil
}

func (b *Broker) hasKeyInPartition(ctx context.Context, topic string, p *topic.Partition, channel string, key, clusterKey []byte) (bool, error) {
	leader := b.getPartitionLeader(topic, p.Id)
	if leader == nil {
		return false, ErrNoLeaderFound
	}

	if leader.Name != b.Name() {
		b.Debugf("fetch key remotely '%v' from %v", string(key), leader.Name)
		resp, err := leader.HasKey(ctx, &sgproto.GetRequest{
			Topic:         topic,
			Partition:     p.Id,
			Key:           key,
			ClusteringKey: clusterKey,
		})
		if err != nil {
			return false, err
		}

		return resp.Exists, nil
	}

	return p.HasKey(channel, key, clusterKey)
}

func generatePrefixConsumerOffsetKey(partitionKey []byte, offset sgproto.Offset) []byte {
	return bytes.Join([][]byte{
		partitionKey,
		offset.Bytes(),
	}, storage.Separator)
}
