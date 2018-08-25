package broker

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sandglass/sandglass-grpc/go/sgproto"
)

func (b *Broker) GetTopic(ctx context.Context, req *sgproto.GetTopicParams) (*sgproto.GetTopicReply, error) {
	t := b.raft.GetTopic(req.Name)
	if t == nil {
		return nil, status.Errorf(codes.NotFound, "topic '%s' not found")
	}
	partitions := t.ListPartitions()
	res := make([]string, len(partitions))
	for i, p := range partitions {
		res[i] = p.Id
	}

	return &sgproto.GetTopicReply{
		Name:       t.Name,
		Partitions: res,
	}, nil
}

func (b *Broker) FetchFrom(req *sgproto.FetchFromRequest, stream sgproto.BrokerService_FetchFromServer) error {
	partitionReq := &sgproto.FetchRangeRequest{
		Topic:     req.Topic,
		Partition: req.Partition,
		From:      req.From,
		To:        sgproto.MaxOffset,
	}

	return b.FetchRangeFn(stream.Context(), partitionReq, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

func (b *Broker) FetchRange(req *sgproto.FetchRangeRequest, stream sgproto.BrokerService_FetchRangeServer) error {
	return b.FetchRangeFn(stream.Context(), req, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

func (b *Broker) ConsumeFromGroup(req *sgproto.ConsumeFromGroupRequest, stream sgproto.BrokerService_ConsumeFromGroupServer) error {
	return b.Consume(stream.Context(), req, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

func (b *Broker) GetByKey(ctx context.Context, req *sgproto.GetRequest) (*sgproto.Message, error) {
	if len(req.Key) == 0 {
		return nil, fmt.Errorf("can only be used with a key")
	}

	return b.Get(ctx, req)
}

func (b *Broker) HasKey(ctx context.Context, req *sgproto.GetRequest) (*sgproto.HasResponse, error) {
	if len(req.Key) == 0 {
		return nil, fmt.Errorf("can only be used with a key")
	}

	exists, err := b.hasKey(ctx, req.Topic, req.Partition, req.Channel, req.Key, req.ClusteringKey)
	if err != nil {
		return nil, err
	}

	return &sgproto.HasResponse{
		Exists: exists,
	}, nil
}

func (b *Broker) Acknowledge(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	if req.State == nil {
		req.State = &sgproto.MarkState{
			Kind: sgproto.MarkKind_Acknowledged,
		}
	}

	return b.Mark(ctx, req)
}

func (b *Broker) NotAcknowledge(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	if req.State == nil {
		req.State = &sgproto.MarkState{
			Kind: sgproto.MarkKind_NotAcknowledged,
		}
	}

	return b.Mark(ctx, req)
}

func (b *Broker) Commit(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	if req.State == nil {
		req.State = &sgproto.MarkState{
			Kind: sgproto.MarkKind_Commited,
		}
	}

	return b.Mark(ctx, req)
}

func (b *Broker) MarkConsumed(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	if req.State == nil {
		req.State = &sgproto.MarkState{
			Kind: sgproto.MarkKind_Consumed,
		}
	}

	return b.Mark(ctx, req)
}

func (b *Broker) Mark(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	ok, err := b.mark(ctx, req)
	return &sgproto.MarkResponse{
		Success: ok,
	}, err
}

func (b *Broker) LastOffset(ctx context.Context, req *sgproto.LastOffsetRequest) (*sgproto.LastOffsetReply, error) {
	offset, err := b.lastOffset(ctx, req.Topic, req.Partition, req.Channel, req.ConsumerGroup, req.Kind)
	return &sgproto.LastOffsetReply{
		Offset: offset,
	}, err
}

func (b *Broker) FetchFromSync(req *sgproto.FetchFromSyncRequest, stream sgproto.InternalService_FetchFromSyncServer) error {
	return b.fetchFromSync(req.Topic, req.Partition, req.From, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

var _ sgproto.BrokerServiceServer = (*Broker)(nil)
var _ sgproto.InternalServiceServer = (*Broker)(nil)
