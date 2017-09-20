package broker

import (
	"fmt"
	"time"

	"github.com/celrenheit/sandglass/sgutils"

	"github.com/celrenheit/sandglass"
	"github.com/docker/leadership"
)

func (b *Broker) run(prefix string, candidate *leadership.Candidate) {
	electedCh, errCh := candidate.RunForElection()

	for {
		b.Info("[%v] run loop %v\n", prefix, b.Name())
		select {
		case <-b.ShutdownCh:
			return
		case isElected := <-electedCh:
			if isElected {
				// Do something
				b.Info("[%v] elected: %v\n", prefix, b.Name())
			} else {
				// Do something else
				b.Info("[%v] NOT elected: %v\n", prefix, b.Name())
			}
		case err := <-errCh:
			b.Debug("[%v] [candidate: '%s'] err=%v", prefix, b.Name(), err)
			return
		}
	}
}

func (b *Broker) runForPartitionElection(topic, partition string) {
	key := fmt.Sprintf(b.discPrefix+"/state/leader/topics/%s/partitions/%s", topic, partition)

	// TODO: increase this duration
	candidate := leadership.NewCandidate(b.store, key, b.Name(), 2*time.Second)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			b.Info("running for partition election for %v %v %v", topic, partition, b.Name())
			prefix := fmt.Sprintf("t:%v, p:%v", topic, partition)
			b.run(prefix, candidate)
			select {
			case <-b.ShutdownCh:
				candidate.Stop()
				return
			case <-time.After(waitTime):
				// retry
			}
		}
	}()
}

func (b *Broker) followControllerElectionForPartition(topic, partition string) {
	key := fmt.Sprintf(b.discPrefix+"/state/leader/topics/%s/partitions/%s", topic, partition)
	follower := leadership.NewFollower(b.store, key)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			b.watchElectionForPartition(follower, topic, partition)
			select {
			case <-b.ShutdownCh:
				follower.Stop()
				return
			case <-time.After(waitTime):
				// retry
			}
		}
	}()
}

func (b *Broker) watchElectionForPartition(follower *leadership.Follower, topic, partition string) {
	leaderCh, errCh := follower.FollowElection()

	for {
		select {
		case <-b.ShutdownCh:
			return
		case leader := <-leaderCh:
			b.mu.Lock()
			b.setPartitionLeader(topic, partition, leader)
			b.Info("partitionleader is: %+v for t:%v, p:%v\n", leader, topic, partition)
			b.mu.Unlock()
		case err := <-errCh:
			b.Debug("[part %v follower: '%s'] err=%v", partition, b.Name(), err)
			return
		}
	}
}

func (b *Broker) setPartitionLeader(topicName, partition, leader string) {
	if _, ok := b.partitionsLeaders[topicName]; !ok {
		b.partitionsLeaders[topicName] = make(map[string]string)
	}

	b.partitionsLeaders[topicName][partition] = leader
}

func (b *Broker) getPartitionLeader(topic, partition string) *sandglass.Node {
	b.mu.RLock()
	_, ok := b.partitionsLeaders[topic]
	if !ok {
		b.mu.RUnlock()
		return nil
	}
	leaderName := b.partitionsLeaders[topic][partition]
	b.mu.RUnlock()

	return b.getNode(leaderName)
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
