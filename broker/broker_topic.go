package broker

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"encoding/json"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/celrenheit/sandglass/sgutils"
	"github.com/celrenheit/sandglass/topic"
	"github.com/docker/libkv/store"
	"github.com/serialx/hashring"
	"golang.org/x/sync/errgroup"
)

var (
	ErrTopicAlreadyExist      = errors.New("ErrTopicAlreadyExist")
	ErrInvalidTopicName       = errors.New("ErrInvalidTopicName")
	ErrUnableToSelectReplicas = errors.New("ErrUnableToSelectReplicas")
	ErrTopicNotFound          = errors.New("ErrTopicNotFound")
	ErrNoPartitionSet         = errors.New("ErrNoPartitionSet")
	ErrNoControllerSet        = errors.New("ErrNoControllerSet")
	ErrNoLeaderFound          = errors.New("ErrNoLeaderFound")
	ErrNoConsumerFound        = errors.New("ErrNoConsumerFound")
)

func (b *Broker) watchTopic() error {
	pairCh, err := b.store.WatchTree(b.discPrefix+"/topics", b.ShutdownCh)
	if err != nil {
		return err
	}

	for pairs := range pairCh {
		topics := make([]*topic.Topic, 0, len(pairs))
		for _, pair := range pairs {
			var topic topic.Topic
			err := json.Unmarshal(pair.Value, &topic)
			if err != nil {
				b.Debug("[topic watcher] got error: %v", err)
				return err
			}
			b.Info("[topic watcher] received new topic: %s", topic.Name)
			exists := b.topicExists(topic.Name)
			if !exists {
				err := b.setupTopic(&topic)
				if err != nil {
					b.Debug("err in setupTopic: %v", err)
				}
				topics = append(topics, &topic)
			}
		}

		b.mu.Lock()
		b.topics = append(b.topics, topics...)
		b.mu.Unlock()

		for _, t := range topics {
			b.eventEmitter.Emit("topics:created:"+t.Name, nil)
		}

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			if err := b.rearrangePartitionsLeadership(); err != nil {
				b.Debug("error while rearrangeLeadership err=%v", err)
			}
		}()
	}

	return nil
}

func (b *Broker) setupTopic(topic *topic.Topic) error {
	err := topic.InitStore(b.conf.DBPath)
	if err != nil {
		return err
	}

	for _, p := range topic.Partitions {
		if sgutils.StringSliceHasString(p.Replicas, b.Name()) {
			go b.runForPartitionElection(topic.Name, p.Id)
		}
		go b.followControllerElectionForPartition(topic.Name, p.Id)
	}

	return nil
}

func (b *Broker) CreateTopic(params *sgproto.CreateTopicParams) error {
	if params.Name == "" {
		return ErrInvalidTopicName
	}

	if b.topicExists(params.Name) {
		return ErrTopicAlreadyExist
	}

	key := b.discPrefix + "/topics/" + params.Name
	pair, err := b.store.Get(key)
	if err != nil && err != store.ErrKeyNotFound {
		b.Debug("got error while get topic: %v %v", err, err == store.ErrKeyNotFound)
		return err
	} else if pair != nil {
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

	d, err := json.Marshal(t)
	if err != nil {
		return err
	}

	topicCreatedCh := b.eventEmitter.Once("topics:created:" + t.Name)
	if _, _, err := b.store.AtomicPut(key, d, pair, nil); err != nil {
		return err
	}

	var group errgroup.Group

	// set a random replicas as leader
	for _, p := range t.Partitions {
		p := p
		group.Go(func() error {
			leader := p.Replicas[rand.Intn(len(p.Replicas))]
			leaderkey := fmt.Sprintf(b.discPrefix+"/state/leader/topics/%s/partitions/%s", t.Name, p.Id)

			return b.store.Put(leaderkey, []byte(leader), nil)
		})
	}

	err = group.Wait()
	if err != nil {
		return err
	}

	select {
	case <-topicCreatedCh:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timed out creating topic: %v", t.Name)
	}

	return nil
}

func (b *Broker) getTopic(name string) *topic.Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, topic := range b.topics {
		if topic.Name == name {
			return topic
		}
	}

	return nil
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
