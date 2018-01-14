package broker

import (
	"context"
	"sync"
	"time"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/sgutils"
	"github.com/cenkalti/backoff"

	"github.com/celrenheit/sandglass"
)

var (
	leaderElectedEvent = "a leader was elected"
)

func (b *Broker) monitorLeadership() error {
	var once sync.Once

	emitFirstElected := func() {
		once.Do(func() {
			b.eventEmitter.Emit(leaderElectedEvent, nil)
		})
	}

	for {
		if b.raft == nil {
			continue
		}
		select {
		case <-b.shutdownCh:
			return nil
		case <-time.After(1 * time.Second): // reconcile any missing events
			if b.raft.Leader() != "" {
				emitFirstElected()
			}
		case isElected := <-b.raft.LeaderCh():
			emitFirstElected()
			if isElected {
				// Do something
				b.Debugf("elected as controller")
				logger := b.WithField("topic", ConsumerOffsetTopicName)
				exists := b.topicExists(ConsumerOffsetTopicName)
				if !exists {
					operation := func() error {
						logger.Debugf("creating consumer offset topic")
						err := b.CreateTopic(context.TODO(), &sgproto.TopicConfig{
							Name:              ConsumerOffsetTopicName,
							Kind:              sgproto.TopicKind_KVKind,
							NumPartitions:     50,
							ReplicationFactor: int32(b.conf.OffsetReplicationFactor),
							// StorageDriver:     sgproto.StorageDriver_Badger,
						})
						if err != nil {
							logger.WithError(err).Debugf("error while creating consumer offset topic")
							return err
						}
						return nil
					}

					err := backoff.Retry(operation, backoff.NewExponentialBackOff())
					if err != nil {
						logger.WithError(err).Fatal("backoff error while creating consumer offset topic")
					}
				}
				b.rearrangePartitionsLeadership()
			} else {
				// Do something else
				b.Debugf("NOT elected as controller")
			}
		}
	}
}

func (b *Broker) getPartitionLeader(topic, partition string) *sandglass.Node {
	leader, ok := b.raft.GetPartitionLeader(topic, partition)
	if !ok {
		return nil
	}

	return b.getNode(leader)
}

func (b *Broker) isReplicaForTopicPartition(topic, partition string) bool {
	t := b.getTopic(topic)
	if t == nil {
		return false
	}

	p := t.GetPartition(partition)
	if p == nil {
		return false
	}

	return sgutils.StringSliceHasString(p.Replicas, b.Name())
}

func (b *Broker) isLeaderForTopicPartition(topic, partition string) bool {
	if !b.isReplicaForTopicPartition(topic, partition) {
		return false
	}

	leader := b.getPartitionLeader(topic, partition)
	if leader == nil {
		return false
	}

	return leader.Name == b.Name()
}
