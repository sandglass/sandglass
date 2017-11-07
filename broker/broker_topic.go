package broker

import (
	"context"
	"errors"
	"math/rand"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/celrenheit/sandglass/topic"
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
		case <-b.ShutdownCh:
			return nil
		case topic := <-b.raft.NewTopicChan():
			b.Info("[topic watcher] received new topic: %s", topic.Name)
			// exists := b.topicExists(topic.Name)
			// if !exists {
			// 	err := b.setupTopic(topic)
			// 	if err != nil {
			// 		b.Debug("err in setupTopic: %v", err)
			// 	}
			// }

			// b.eventEmitter.Emit("topics:created:"+topic.Name, nil)

			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				if err := b.rearrangePartitionsLeadership(); err != nil {
					b.Debug("error while rearrangeLeadership err=%v", err)
				}
			}()

			if topic.Name == ConsumerOffsetTopicName {
				b.eventEmitter.Emit(consumerOffsetReceivedEvent, nil)
			}
		}
	}
}

func (b *Broker) setupTopic(topic *topic.Topic) error {
	err := topic.InitStore(b.conf.DBPath)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) CreateTopic(ctx context.Context, params *sgproto.CreateTopicParams) error {
	if params.Name == "" {
		return ErrInvalidTopicName
	}

	if !b.IsController() {
		leader := b.GetController()
		if leader == nil {
			return ErrNoLeaderFound
		}
		b.Debug("forward CreateTopic to %v", leader)
		_, err := leader.CreateTopic(ctx, params)
		return err
	}

	if b.topicExists(params.Name) {
		return ErrTopicAlreadyExist
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
			if params.Name != ConsumerOffsetTopicName { // should we allow this for all topics ?
				return ErrUnableToSelectReplicas
			}

			// if we don't have enough replicas we all nodes currently available
			for _, n := range b.Members() {
				replicas = append(replicas, n.Name)
			}
		}

		p.Replicas = replicas
		t.Partitions = append(t.Partitions, p)
	}

	// topicCreatedCh := b.eventEmitter.Once("topics:created:" + t.Name)

	if err := b.raft.CreateTopic(t); err != nil {
		return err
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
		return err
	}

	// select {
	// case <-topicCreatedCh:
	// case <-time.After(10 * time.Second):
	// 	return fmt.Errorf("timed out creating topic: %v", t.Name)
	// }

	return nil
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
