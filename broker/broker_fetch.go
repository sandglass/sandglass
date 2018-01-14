package broker

import (
	"bytes"
	"context"
	"io"

	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/topic"
	"github.com/grpc/grpc-go/status"
)

func (b *Broker) FetchRange(ctx context.Context, req *sgproto.FetchRangeRequest, fn func(msg *sgproto.Message) error) error {
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
	return p.ForRange(req.From, req.To, fn)
}

func (b *Broker) FetchFromSync(topicName, partition string, from []byte, fn func(msg *sgproto.Message) error) error {
	topic := b.getTopic(topicName)
	if topic == nil {
		return ErrTopicNotFound
	}

	if partition == "" {
		return ErrNoPartitionSet
	}

	p := topic.GetPartition(partition)
	return p.RangeFromWAL(from, fn)
}

func (b *Broker) Get(ctx context.Context, topicName string, partition string, key []byte) (*sgproto.Message, error) {
	t := b.getTopic(topicName)
	var p *topic.Partition
	if partition != "" {
		p = t.GetPartition(partition)
	} else {
		p = t.ChoosePartitionForKey(key)
	}
	return b.getFromPartition(ctx, topicName, p, key)
}

func (b *Broker) HasKey(ctx context.Context, topicName string, partition string, key, clusterKey []byte) (bool, error) {
	t := b.getTopic(topicName)
	var p *topic.Partition
	if partition != "" {
		p = t.GetPartition(partition)
	} else {
		p = t.ChoosePartitionForKey(key)
	}
	return b.hasKeyInPartition(ctx, topicName, p, key, clusterKey)
}

func (b *Broker) getFromPartition(ctx context.Context, topic string, p *topic.Partition, key []byte) (*sgproto.Message, error) {
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
			Key:       key,
		})
	}

	msg, err := p.GetMessage(sandflake.Nil, key, nil)
	if err != nil {
		return nil, err
	}

	if msg == nil {
		return nil, status.Error(codes.NotFound, "message not found")
	}

	return msg, nil
}

func (b *Broker) hasKeyInPartition(ctx context.Context, topic string, p *topic.Partition, key, clusterKey []byte) (bool, error) {
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

	return p.HasKey(key, clusterKey)
}

func generatePrefixConsumerOffsetKey(partitionKey []byte, offset sandflake.ID) []byte {
	return bytes.Join([][]byte{
		partitionKey,
		offset.Bytes(),
	}, storage.Separator)
}
