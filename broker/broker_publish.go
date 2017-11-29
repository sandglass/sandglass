package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/celrenheit/sandglass/topic"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
)

var (
	ErrNoKeySet = errors.New("ErrNoKeySet")
)

func (b *Broker) Publish(ctx context.Context, req *sgproto.ProduceMessageRequest) (*sgproto.PublishResponse, error) {
	b.Debug("PublishMessage: %+v\n", req)
	t := b.getTopic(req.Topic)
	if t == nil {
		return nil, ErrTopicNotFound
	}

	var p *topic.Partition
	if req.Partition != "" { // already specified
		if p = t.GetPartition(req.Partition); p == nil {
			return nil, fmt.Errorf("unknown partition '%s'", req.Partition)
		}
	} else { // choose one
		p = t.ChoosePartition(req.Messages[0]) // FIXME: choose randomly
	}

	leader := b.getPartitionLeader(req.Topic, p.Id)
	if leader == nil {
		return nil, ErrNoLeaderFound
	}

	if leader.Name != b.Name() {
		return leader.Publish(ctx, req)
	}

	// FIXME: this is shit, should be after Put
	res := &sgproto.PublishResponse{}
	for _, msg := range req.Messages {
		if msg.Offset == sandflake.Nil {
			msg.Offset = b.idgen.Next()
		}
		res.Offsets = append(res.Offsets, msg.Offset)
	}

	err := p.BatchPutMessages(req.Messages)
	if err != nil {
		return nil, err
	}

	return res, nil
}
