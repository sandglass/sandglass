package broker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/celrenheit/sandflake"
	"github.com/sandglass/sandglass-grpc/go/sgproto"
	"github.com/sandglass/sandglass/topic"
	"github.com/hashicorp/serf/serf"
	"github.com/serialx/hashring"
)

var (
	ErrTopicAlreadyExist      = errors.New("ErrTopicAlreadyExist")
	ErrInvalidTopicName       = errors.New("ErrInvalidTopicName")
	ErrUnableToSelectReplicas = errors.New("ErrUnableToSelectReplicas")
	ErrTopicNotFound          = errors.New("ErrTopicNotFound")
	ErrPartitionNotFound      = errors.New("ErrPartitionNotFound")
	ErrNoPartitionSet         = errors.New("ErrNoPartitionSet")
	ErrNoControllerSet        = errors.New("ErrNoControllerSet")
	ErrNoLeaderFound          = errors.New("ErrNoLeaderFound")
	ErrNoConsumerFound        = errors.New("ErrNoConsumerFound")
)

func (b *Broker) watchTopic() error {
	for {
		select {
		case <-b.shutdownCh:
			return nil
		case topic := <-b.raft.NewTopicChan():
			b.WithField("topic", topic.Name).Debugf("[topic watcher] received new topic")

			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				if err := b.rearrangePartitionsLeadership(); err != nil {
					b.WithError(err).Debugf("error while rearrangeLeadership err=%v", err)
				}
			}()
			b.eventEmitter.Emit("topics:created:"+topic.Name, nil)
		}
	}
}

func (b *Broker) CreateTopic(ctx context.Context, params *sgproto.TopicConfig) (*sgproto.TopicReply, error) {
	if params.Name == "" {
		return nil, ErrInvalidTopicName
	}

	if !b.IsController() {
		leader := b.GetController()
		if leader == nil {
			return nil, ErrNoLeaderFound
		}
		b.WithField("leader", leader).Debugf("forward CreateTopic")
		_, err := leader.CreateTopic(ctx, params)
		return nil, err
	}

	if b.topicExists(params.Name) {
		return nil, ErrTopicAlreadyExist
	}

	t := &topic.Topic{
		Name:              params.Name,
		Kind:              params.Kind,
		ReplicationFactor: int(params.ReplicationFactor),
		NumPartitions:     int(params.NumPartitions),
		StorageDriver:     params.StorageDriver,
	}

	var g sandflake.Generator
	for i := 0; i < t.NumPartitions; i++ {
		p := &topic.Partition{
			Id: g.Next().String(),
		}

		replicas, ok := b.selectReplicasForPartition(t, p)
		if !ok {
			return nil, ErrUnableToSelectReplicas
		}

		p.Replicas = replicas
		t.Partitions = append(t.Partitions, p)
	}
	topicCreatedCh := b.eventEmitter.Once("topics:created:" + t.Name)
	if err := b.raft.CreateTopic(t); err != nil {
		return nil, err
	}

	partitionLeaders := map[string]map[string]string{
		params.Name: map[string]string{},
	}

	for _, p := range t.Partitions {
		leader := p.Replicas[rand.Intn(len(p.Replicas))]
		partitionLeaders[params.Name][p.Id] = leader
	}

	err := b.raft.SetPartitionLeaderBulkOp(partitionLeaders)
	if err != nil {
		return nil, err
	}

	select {
	case <-topicCreatedCh:
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timed out creating topic: %v", t.Name)
	}

	if t.ReplicationFactor > 1 {
		qry, err := b.cluster.Query("wait-for-topic", []byte(t.Name), &serf.QueryParam{})
		if err != nil {
			return nil, err
		}
		defer qry.Close()

		members := t.ReplicationFactor
		acks := 0
		for !qry.Finished() {
			resp := <-qry.ResponseCh()
			if len(resp.Payload) > 0 {
				acks++
			}
		}

		if acks < ((members / 2) + 1) {
			return nil, fmt.Errorf("unable to have quorum")
		}
	}

	return &sgproto.TopicReply{Success: true}, nil
}

func (b *Broker) getTopic(name string) *topic.Topic {
	return b.raft.GetTopic(name)
}

func (b *Broker) topicExists(name string) bool {
	return b.getTopic(name) != nil
}

func (b *Broker) selectReplicasForPartition(topic *topic.Topic, p *topic.Partition) ([]string, bool) {
	b.mu.RLock()
	list := make([]string, 0, len(b.peers))
	for name := range b.peers {
		list = append(list, name)
	}
	b.mu.RUnlock()

	ring := hashring.New(list)
	return ring.GetNodes(p.Id, topic.ReplicationFactor)
}
