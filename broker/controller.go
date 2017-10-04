package broker

import (
	"math/rand"
	"time"

	"github.com/celrenheit/sandglass/sgutils"

	"github.com/celrenheit/sandglass"
)

var waitTime = 1 * time.Second

func (b *Broker) hasController() bool {
	leader := b.raft.Leader()
	return leader != "" && b.getNodeByRaftAddr(leader) != nil
}

func (b *Broker) IsController() bool {
	return b.raft.Leader() == b.conf.RaftAddr
}

func (b *Broker) GetController() *sandglass.Node {
	return b.getNodeByRaftAddr(b.raft.Leader())
}

func (b *Broker) rearrangePartitionsLeadership() error {
	b.Debug("rearrangePartitionsLeadership")
	b.mu.RLock()
	defer b.mu.RUnlock()

	var partitionBulkLeaderOp map[string]map[string]string
	for _, t := range b.raft.GetTopics() {
		for _, partition := range t.Partitions {
			oldLeader, ok := b.raft.GetPartitionLeader(t.Name, partition.Id)

			if _, ok := b.peers[oldLeader]; ok { // still alive, nothing to do
				continue
			}

			if partition == nil {
				b.Debug("got unknown partition: %v", partition)
				continue
			}
			aliveReplicas := make([]string, 0, len(partition.Replicas))
			for _, r := range partition.Replicas {
				if _, ok := b.peers[r]; ok {
					aliveReplicas = append(aliveReplicas, r)
				}
			}

			if len(aliveReplicas) == 0 {
				b.Debug("NO leader available for %+v (%v)", partition, partitionKey)
				continue
			}

			if ok && sgutils.StringSliceHasString(aliveReplicas, oldLeader) {
				b.Debug("old leader still alive %+v (t:%v p:%v)", oldLeader, t.Name, partition.Id)
				continue
			}

			if partitionBulkLeaderOp == nil {
				partitionBulkLeaderOp = map[string]map[string]string{}
			}

			if partitionBulkLeaderOp[t.Name] == nil {
				partitionBulkLeaderOp[t.Name] = map[string]string{}
			}

			newLeader := aliveReplicas[rand.Intn(len(aliveReplicas))]
			b.Debug("switch leader of topic:%v partition: %v (old=%v -> new=%v)", t.Name, partition.Id, oldLeader, newLeader)
			partitionBulkLeaderOp[t.Name][partition.Id] = newLeader
		}
	}

	if partitionBulkLeaderOp == nil {
		return nil
	}

	return b.raft.SetPartitionLeaderBulkOp(partitionBulkLeaderOp)
}
