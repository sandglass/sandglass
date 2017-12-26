package server

import (
	"fmt"

	"github.com/celrenheit/sandflake"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/broker"
)

type service struct {
	broker *broker.Broker
}

func newService(b *broker.Broker) *service {
	return &service{broker: b}
}

func (s *service) CreateTopic(ctx context.Context, params *sgproto.TopicConfig) (*sgproto.TopicReply, error) {
	err := s.broker.CreateTopic(ctx, params)
	if err != nil {
		return nil, err
	}

	return &sgproto.TopicReply{Success: true}, nil
}

func (s *service) GetTopic(ctx context.Context, req *sgproto.GetTopicParams) (*sgproto.GetTopicReply, error) {
	t := s.broker.GetTopic(req.Name)
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

func (s *service) Produce(ctx context.Context, req *sgproto.ProduceMessageRequest) (*sgproto.ProduceResponse, error) {
	return s.broker.Produce(ctx, req)
}

func (s *service) FetchFrom(req *sgproto.FetchFromRequest, stream sgproto.BrokerService_FetchFromServer) error {
	partitionReq := &sgproto.FetchRangeRequest{
		Topic:     req.Topic,
		Partition: req.Partition,
		From:      req.From,
		To:        sandflake.MaxID,
	}

	return s.broker.FetchRange(stream.Context(), partitionReq, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

func (s *service) FetchRange(req *sgproto.FetchRangeRequest, stream sgproto.BrokerService_FetchRangeServer) error {
	return s.broker.FetchRange(stream.Context(), req, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

func (s *service) ConsumeFromGroup(req *sgproto.ConsumeFromGroupRequest, stream sgproto.BrokerService_ConsumeFromGroupServer) error {
	return s.broker.Consume(stream.Context(), req.Topic, req.Partition, req.ConsumerGroupName, req.ConsumerName, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

func (s *service) GetByKey(ctx context.Context, req *sgproto.GetRequest) (*sgproto.Message, error) {
	if len(req.Key) == 0 {
		return nil, fmt.Errorf("can only be used with a key")
	}

	return s.broker.Get(ctx, req.Topic, req.Partition, req.Key)
}

func (s *service) HasKey(ctx context.Context, req *sgproto.GetRequest) (*sgproto.HasResponse, error) {
	if len(req.Key) == 0 {
		return nil, fmt.Errorf("can only be used with a key")
	}

	exists, err := s.broker.HasKey(ctx, req.Topic, req.Partition, req.Key, req.ClusteringKey)
	if err != nil {
		return nil, err
	}

	return &sgproto.HasResponse{
		Exists: exists,
	}, nil
}

func (s *service) Acknowledge(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	ok, err := s.broker.Mark(ctx, req)
	return &sgproto.MarkResponse{
		Success: ok,
	}, err
}

func (s *service) NotAcknowledge(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	ok, err := s.broker.Mark(ctx, req)
	return &sgproto.MarkResponse{
		Success: ok,
	}, err
}

func (s *service) Commit(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	ok, err := s.broker.Mark(ctx, req)
	return &sgproto.MarkResponse{
		Success: ok,
	}, err
}

func (s *service) MarkConsumed(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	ok, err := s.broker.Mark(ctx, req)
	return &sgproto.MarkResponse{
		Success: ok,
	}, err
}

func (s *service) Mark(ctx context.Context, req *sgproto.MarkRequest) (*sgproto.MarkResponse, error) {
	ok, err := s.broker.Mark(ctx, req)
	return &sgproto.MarkResponse{
		Success: ok,
	}, err
}

func (s *service) LastOffset(ctx context.Context, req *sgproto.LastOffsetRequest) (*sgproto.LastOffsetReply, error) {
	offset, err := s.broker.LastOffset(ctx, req.Topic, req.Partition, req.ConsumerGroup, req.Kind)
	return &sgproto.LastOffsetReply{
		Offset: offset,
	}, err
}

func (s *service) GetMarkStateMessage(ctx context.Context, req *sgproto.GetMarkRequest) (*sgproto.Message, error) {
	return s.broker.GetMarkStateMessage(ctx, req)
}

func (s *service) FetchFromSync(req *sgproto.FetchFromSyncRequest, stream sgproto.InternalService_FetchFromSyncServer) error {
	return s.broker.FetchFromSync(req.Topic, req.Partition, req.From, func(msg *sgproto.Message) error {
		return stream.Send(msg)
	})
}

var _ sgproto.BrokerServiceServer = (*service)(nil)
var _ sgproto.InternalServiceServer = (*service)(nil)
