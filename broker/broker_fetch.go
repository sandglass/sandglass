package broker

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc/codes"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/celrenheit/sandglass/topic"
	"github.com/grpc/grpc-go/status"
	"golang.org/x/sync/errgroup"
)

func (b *Broker) FetchRange(topicName, partition string, from, to sandflake.ID, fn func(msg *sgproto.Message) error) error {
	topic := b.getTopic(topicName)
	if topic == nil {
		return ErrTopicNotFound
	}

	if partition != "" {
		p := topic.GetPartition(partition)
		return p.ForRange(from, to, fn)
	}

	var group errgroup.Group
	var mu sync.Mutex

	for _, p := range topic.Partitions {
		p := p
		n := b.getPartitionLeader(topic.Name, p.Id)
		if n.Name == b.Name() {
			b.Debug("fetching locally %v", p.Id)
			group.Go(func() error {
				err := p.ForRange(from, to, func(msg *sgproto.Message) error {
					mu.Lock()
					err := fn(msg)
					mu.Unlock()
					return err
				})
				b.Debug("done locally: %+v %v\n", p.Id, err)

				return err
			})
			continue
		}

		b.Debug("fetching remotely %v from %v", n.Name, p.Id)
		group.Go(func() error {
			stream, err := n.FetchRange(context.TODO(), &sgproto.FetchRangeRequest{
				Topic:     topicName,
				Partition: p.Id,
				From:      from,
				To:        to,
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

				mu.Lock()
				err = fn(msg)
				mu.Unlock()
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	return group.Wait()
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

func (b *Broker) Get(topicName string, partition string, key []byte) (*sgproto.Message, error) {
	t := b.getTopic(topicName)
	var p *topic.Partition
	if partition != "" {
		p = t.GetPartition(partition)
	} else {
		p = t.ChoosePartitionForKey(key)
	}
	return b.getFromPartition(topicName, p, key)
}

func (b *Broker) HasKey(topicName string, partition string, key []byte) (bool, error) {
	t := b.getTopic(topicName)
	var p *topic.Partition
	if partition != "" {
		p = t.GetPartition(partition)
	} else {
		p = t.ChoosePartitionForKey(key)
	}
	return b.hasKeyInPartition(topicName, p, key)
}

func (b *Broker) getFromPartition(topic string, p *topic.Partition, key []byte) (*sgproto.Message, error) {
	leader := b.getPartitionLeader(topic, p.Id)
	if leader == nil {
		return nil, ErrNoLeaderFound
	}

	if leader.Name != b.Name() {
		b.Debug("fetch key remotely '%v' from %v", string(key), leader.Name)
		return leader.GetByKey(context.TODO(), &sgproto.GetRequest{
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

func (b *Broker) hasKeyInPartition(topic string, p *topic.Partition, key []byte) (bool, error) {
	leader := b.getPartitionLeader(topic, p.Id)
	if leader == nil {
		return false, ErrNoLeaderFound
	}

	if leader.Name != b.Name() {
		b.Debug("fetch key remotely '%v' from %v", string(key), leader.Name)
		resp, err := leader.HasKey(context.TODO(), &sgproto.GetRequest{
			Topic:     topic,
			Partition: p.Id,
			Key:       key,
		})
		if err != nil {
			return false, err
		}

		return resp.Exists, nil
	}

	return p.HasKey(key)
}
