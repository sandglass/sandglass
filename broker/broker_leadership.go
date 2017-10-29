package broker

import (
	"context"

	"github.com/celrenheit/sandglass/sgproto"
	"github.com/celrenheit/sandglass/sgutils"

	"github.com/celrenheit/sandglass"
)

func (b *Broker) monitorLeadership() error {
	for {
		if b.raft == nil {
			continue
		}
		select {
		case <-b.ShutdownCh:
			return nil
		case isElected := <-b.raft.LeaderCh():
			if isElected {
				// Do something
				b.Debug("elected as controller %v\n", b.Name())
				exists := b.topicExists(ConsumerOffsetTopicName)
				if !exists {
					for i := 0; i < 10; i++ {
						b.Debug("creating %s topic", ConsumerOffsetTopicName)
						err := b.CreateTopic(context.TODO(), &sgproto.CreateTopicParams{
							Name:              ConsumerOffsetTopicName,
							Kind:              sgproto.TopicKind_CompactedKind,
							NumPartitions:     50,
							ReplicationFactor: 3,
						})
						if err == nil {
							break
						}
						b.Debug("error while creating %v topic err=%v", ConsumerOffsetTopicName, err)
					}
				}
				b.rearrangePartitionsLeadership()
			} else {
				// Do something else
				b.Debug("NOT elected as controller: %v\n", b.Name())
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
