package server

import (
	"fmt"
	"io"
	"time"

	"github.com/celrenheit/sandflake"

	"google.golang.org/grpc/metadata"

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

func (s *service) CreateTopic(ctx context.Context, params *sgproto.CreateTopicParams) (*sgproto.TopicReply, error) {
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

func (s *service) ProduceMessagesStream(stream sgproto.BrokerService_ProduceMessagesStreamServer) error {
	const n = 10000
	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return fmt.Errorf("no metadata")
	}

	var topic, partition string

	if mds, ok := md["topic"]; ok && len(mds) > 0 {
		topic = mds[0]
	}

	if mds, ok := md["partition"]; ok && len(mds) > 0 {
		partition = mds[0]
	}

	if topic == "" {
		return fmt.Errorf("topic metadata should be set")
	}

	messages := make([]*sgproto.Message, 0)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		messages = append(messages, msg)
		if len(messages) >= n {
			start := time.Now()
			if _, err := s.broker.Produce(ctx, &sgproto.ProduceMessageRequest{
				Topic:     topic,
				Partition: partition,
				Messages:  messages,
			}); err != nil {
				return err
			}
			fmt.Println("Publish Messages took:", time.Since(start))
			messages = messages[:0]
		}
	}

	if len(messages) > 0 {
		_, err := s.broker.Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     topic,
			Partition: partition,
			Messages:  messages,
		})
		return err
	}

	return nil
}

// func (s *service) StoreMessageLocally(ctx context.Context, msg *sgproto.Message) (*sgproto.StoreLocallyReply, error) {
// 	err := s.broker.StoreMessageLocally(msg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &sgproto.StoreLocallyReply{}, nil
// }

// func (s *service) StoreMessagesStream(stream sgproto.BrokerService_StoreMessagesStreamServer) error {
// 	const n = 10000
// 	messages := make([]*sgproto.Message, 0)
// 	for {
// 		msg, err := stream.Recv()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			return err
// 		}

// 		messages = append(messages, msg)
// 		if len(messages) >= n {
// 			if err := s.broker.StoreMessages(messages); err != nil {
// 				return err
// 			}
// 			messages = messages[:0]
// 		}
// 	}

// 	return s.broker.StoreMessages(messages)
// }

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

func (s *service) Acknowledge(ctx context.Context, req *sgproto.OffsetChangeRequest) (*sgproto.OffsetChangeReply, error) {
	ok, err := s.broker.Acknowledge(ctx, req.Topic, req.Partition, req.ConsumerGroup, req.ConsumerName, req.Offset)
	return &sgproto.OffsetChangeReply{
		Success: ok,
	}, err
}

func (s *service) AcknowledgeMessages(ctx context.Context, req *sgproto.MultiOffsetChangeRequest) (*sgproto.OffsetChangeReply, error) {
	err := s.broker.AcknowledgeMessages(ctx, req.Topic, req.Partition, req.ConsumerGroup, req.ConsumerName, req.Offsets)
	if err != nil {
		return nil, err
	}

	return &sgproto.OffsetChangeReply{
		Success: true,
	}, nil
}

func (s *service) Commit(ctx context.Context, req *sgproto.OffsetChangeRequest) (*sgproto.OffsetChangeReply, error) {
	ok, err := s.broker.Commit(ctx, req.Topic, req.Partition, req.ConsumerGroup, req.ConsumerName, req.Offset)
	return &sgproto.OffsetChangeReply{
		Success: ok,
	}, err
}

func (s *service) MarkConsumed(ctx context.Context, req *sgproto.OffsetChangeRequest) (*sgproto.OffsetChangeReply, error) {
	ok, err := s.broker.MarkConsumed(ctx, req.Topic, req.Partition, req.ConsumerGroup, req.ConsumerName, req.Offset)
	return &sgproto.OffsetChangeReply{
		Success: ok,
	}, err
}

func (s *service) LastOffset(ctx context.Context, req *sgproto.LastOffsetRequest) (*sgproto.LastOffsetReply, error) {
	offset, err := s.broker.LastOffset(ctx, req.Topic, req.Partition, req.ConsumerGroup, req.ConsumerName, req.Kind)
	return &sgproto.LastOffsetReply{
		Offset: offset,
	}, err
}

func (s *service) GetMarkStateMessage(ctx context.Context, req *sgproto.OffsetChangeRequest) (*sgproto.Message, error) {
	return s.broker.GetMarkStateMessage(ctx, req.Topic, req.Partition, req.ConsumerGroup, req.ConsumerName, req.Offset)
}

func (s *service) FetchFromSync(req *sgproto.FetchFromSyncRequest, stream sgproto.InternalService_FetchFromSyncServer) error {
	return s.broker.FetchFromSync(req.Topic, req.Partition, req.From, func(msg *sgproto.Message) error {
		// if msg == nil {
		// 	return fmt.Errorf("kikou")
		// }

		return stream.Send(msg)
	})
}

var _ sgproto.BrokerServiceServer = (*service)(nil)
var _ sgproto.InternalServiceServer = (*service)(nil)
