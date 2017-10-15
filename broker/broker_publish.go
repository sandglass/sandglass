package broker

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"

	"github.com/celrenheit/sandglass/topic"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass"
	"github.com/celrenheit/sandglass/sgproto"
)

var (
	ErrNoKeySet = errors.New("ErrNoKeySet")
)

func (b *Broker) PublishMessage(msg *sgproto.Message) (*sandflake.ID, error) {
	b.Debug("PublishMessage: %+v\n", msg)
	t := b.getTopic(msg.Topic)
	if t == nil {
		return nil, ErrTopicNotFound
	}

	if msg.Offset == sandflake.Nil {
		msg.Offset = b.idgen.Next()
	}

	var p *topic.Partition
	if msg.Partition != "" { // already specified
		p = t.GetPartition(msg.Partition)
	} else { // choose one
		p = t.ChoosePartition(msg)
	}
	leader := b.getPartitionLeader(msg.Topic, p.Id)
	if leader == nil {
		return nil, ErrNoLeaderFound
	}

	if leader.Name != b.Name() {
		return b.forwardMessage(leader, msg)
	}

	err := p.PutMessage(msg)
	if err != nil {
		return nil, err
	}

	return &msg.Offset, nil
}

func (b *Broker) PublishMessages(msgs []*sgproto.Message) error {
	b.Debug("PublishMessages: %+v\n", len(msgs))
	partitionMessages := map[*topic.Partition][]*sgproto.Message{}
	for _, msg := range msgs {
		t := b.getTopic(msg.Topic)
		if t == nil {
			return ErrTopicNotFound
		}

		if msg.Offset == sandflake.Nil {
			msg.Offset = b.idgen.Next()
		}

		var p *topic.Partition
		if msg.Partition != "" { // already specified
			p = t.GetPartition(msg.Partition)
		} else { // choose one
			p = t.ChoosePartition(msg)
			msg.Partition = p.Id
		}

		partitionMessages[p] = append(partitionMessages[p], msg)

	}

	var group errgroup.Group
	for p, msgs := range partitionMessages {
		p := p
		msgs := msgs
		group.Go(func() error {
			leader := b.getPartitionLeader(msgs[0].Topic, p.Id)
			if leader == nil {
				return ErrNoLeaderFound
			}

			if leader.Name != b.Name() {
				return b.forwardMessages(leader, msgs)
			}

			err := p.BatchPutMessages(msgs)
			if err != nil {
				return err
			}

			return nil
		})
	}

	return group.Wait()
}

func (b *Broker) StoreMessageLocally(msg *sgproto.Message) error {
	if msg.Offset == sandflake.Nil {
		b.Info("No id set")
		return ErrNoKeySet
	}

	topic := b.getTopic(msg.Topic)
	if topic == nil {
		return ErrTopicNotFound
	}

	if err := topic.PutMessage(msg); err != nil {
		return err
	}

	return nil
}

func (b *Broker) StoreMessages(msgs []*sgproto.Message) error {
	topic := b.getTopic(msgs[0].Topic)
	if topic == nil {
		return errors.New("TOPIC NOT FOUND")
	}

	// already forwarded
	if err := topic.BatchPutMessages(msgs); err != nil {
		return err
	}

	return nil
}

func (b *Broker) forwardMessage(leader *sandglass.Node, msg *sgproto.Message) (*sandflake.ID, error) {
	b.Debug("forwarding message '%v' to %v\n", msg.Offset, leader.Name)
	resp, err := leader.Publish(context.Background(), msg)

	if err != nil {
		return nil, err
	}

	return &resp.Id, nil
}

func (b *Broker) forwardMessages(leader *sandglass.Node, msgs []*sgproto.Message) error {
	stream, err := leader.StoreMessagesStream(context.TODO())
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		err := stream.Send(msg)
		if err != nil {
			return err
		}
	}

	return nil
}
