package broker

import (
	"math/rand"

	"github.com/celrenheit/sandglass/sgutils"
	"github.com/sirupsen/logrus"

	"github.com/celrenheit/sandglass"
)

func (b *Broker) IsController() bool {
	return b.raft.Leader() == b.currentNode.RAFTAddr
}

func (b *Broker) GetController() *sandglass.Node {
	return b.getNodeByRaftAddr(b.raft.Leader())
}

func (b *Broker) rearrangePartitionsLeadership() error {
	b.Debugf("rearrangePartitionsLeadership")
	b.mu.RLock()
	defer b.mu.RUnlock()

	var partitionBulkLeaderOp map[string]map[string]string

	setNewLeader := func(topic, partition, newLeader string) {
		if partitionBulkLeaderOp == nil {
			partitionBulkLeaderOp = map[string]map[string]string{}
		}

		if partitionBulkLeaderOp[topic] == nil {
			partitionBulkLeaderOp[topic] = map[string]string{}
		}
		partitionBulkLeaderOp[topic][partition] = newLeader
	}

	for _, t := range b.raft.GetTopics() {
		for _, partition := range t.Partitions {
			oldLeader, ok := b.raft.GetPartitionLeader(t.Name, partition.Id)

			if _, ok := b.peers[oldLeader]; ok { // still alive, nothing to do
				continue
			}
			logger := b.WithFields(logrus.Fields{
				"topic":      t.Name,
				"partition":  partition.Id,
				"replicas":   partition.Replicas,
				"old_leader": oldLeader,
			})

			aliveReplicas := make([]string, 0, len(partition.Replicas))
			for _, r := range partition.Replicas {
				if _, ok := b.peers[r]; ok {
					aliveReplicas = append(aliveReplicas, r)
				}
			}
			logger = logger.WithField("alive_replicas", aliveReplicas)

			if len(aliveReplicas) == 0 {
				logger.Debugf("no leader available")
				setNewLeader(t.Name, partition.Id, "")
				continue
			}

			if ok && sgutils.StringSliceHasString(aliveReplicas, oldLeader) {
				logger.Debugf("old leader still alive")
				continue
			}

			newLeader := aliveReplicas[rand.Intn(len(aliveReplicas))]
			logger.WithField("new_leader", newLeader).Debugf("switch leader")
			setNewLeader(t.Name, partition.Id, newLeader)
		}
	}

	if partitionBulkLeaderOp == nil {
		return nil
	}

	return b.raft.SetPartitionLeaderBulkOp(partitionBulkLeaderOp)
}
