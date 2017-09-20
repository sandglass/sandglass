package broker

import (
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"

	"strings"

	"github.com/celrenheit/sandglass"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/docker/leadership"
	"github.com/docker/libkv/store"
)

var waitTime = 1 * time.Second

func (b *Broker) hasController() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.controller != "" && b.getNode(b.controller) != nil
}

func (b *Broker) IsController() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.controller == b.Name()
}

func (b *Broker) GetController() *sandglass.Node {
	return b.getNode(b.controller)
}

func (b *Broker) runForControllerElection() {
	key := b.discPrefix + "/state/controller"
	// TODO: increase this duration
	candidate := leadership.NewCandidate(b.store, key, b.Name(), 2*time.Second)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			b.run("controller", candidate)
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

func (b *Broker) followControllerElection() {
	key := b.discPrefix + "/state/controller"
	follower := leadership.NewFollower(b.store, key)

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			b.watchControllerElection(follower)
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
func (b *Broker) watchControllerElection(follower *leadership.Follower) {
	leaderCh, errCh := follower.FollowElection()

	for {
		select {
		case <-b.ShutdownCh:
			return
		case leader := <-leaderCh:
			b.mu.Lock()
			b.controller = leader
			b.Info("controller is: %+v\n", leader)
			b.mu.Unlock()

			if b.IsController() {
				_, err := b.store.Get(b.discPrefix + "/topics/" + ConsumerOffsetTopicName)
				if err == store.ErrKeyNotFound {
					for i := 0; i < 10; i++ {
						b.Debug("creating %s topic", ConsumerOffsetTopicName)
						err := b.CreateTopic(&sgproto.CreateTopicParams{
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
			}
		case err := <-errCh:
			b.Debug("[controller follower: '%s'] err=%v", b.Name(), err)
			return
		}
	}
}

func (b *Broker) rearrangePartitionsLeadership() error {
	b.Debug("rearrangePartitionsLeadership")
	b.mu.RLock()
	defer b.mu.RUnlock()

	var group errgroup.Group
	for _, t := range b.topics {
		key := fmt.Sprintf(b.discPrefix+"/state/leader/topics/%s/partitions/", t.Name)
		list, err := b.store.List(key)
		if err != nil {
			return err
		}

		for _, pair := range list {
			pair := pair
			partitionKey := pair.Key
			oldLeader := string(pair.Value)
			partitionId := strings.TrimPrefix(pair.Key, key)

			if _, ok := b.peers[oldLeader]; ok { // still alive, nothing to do
				continue
			}

			partition := t.GetPartition(partitionId)
			if partition == nil {
				b.Debug("got unknown partition: %v", pair.Key)
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

			newLeader := aliveReplicas[rand.Intn(len(aliveReplicas))]
			b.Debug("switch leader of topic:%v partition: %v (old=%v -> new=%v)", t.Name, partition.Id, oldLeader, newLeader)
			group.Go(func() error {
				_, _, err := b.store.AtomicPut(partitionKey, []byte(newLeader), pair, nil)
				return err
			})
		}
	}

	return group.Wait()
}
