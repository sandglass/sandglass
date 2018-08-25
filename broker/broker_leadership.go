package broker

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sandglass/sandglass-grpc/go/sgproto"
	"github.com/sandglass/sandglass/sgutils"
	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"

	"github.com/sandglass/sandglass"
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

	var (
		weAreLeaderCh chan struct{}
		leaderLoop    sync.WaitGroup
	)

	for {
		if b.raft == nil {
			continue
		}

		select {
		case <-b.shutdownCh:
			if weAreLeaderCh != nil {
				close(weAreLeaderCh)
				leaderLoop.Wait()
			}
			return nil
		case <-time.After(1 * time.Second): // reconcile any missing events
			if b.raft.Leader() != "" {
				emitFirstElected()
			}
		case isElected := <-b.raft.LeaderCh():
			emitFirstElected()
			if isElected {
				if weAreLeaderCh != nil {
					continue
				}
				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					b.leaderLoop(ch)
				}(weAreLeaderCh)
			} else {
				if weAreLeaderCh == nil {
					continue
				}

				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				// Do something else
				b.Debugf("NOT elected as controller")
			}
		}
	}
}

func (b *Broker) leaderLoop(leaderStop chan struct{}) {
	// Do something
	b.Debugf("elected as controller")
	logger := b.WithField("topic", ConsumerOffsetTopicName)
	exists := b.topicExists(ConsumerOffsetTopicName)
	if !exists {
		operation := func() error {
			logger.Debugf("creating consumer offset topic")
			_, err := b.CreateTopic(context.TODO(), &sgproto.TopicConfig{
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

	ctx, cancel := context.WithCancel(context.Background())
	rearrangeTimer := time.NewTicker(15 * time.Second)
	defer rearrangeTimer.Stop()

	var wg sync.WaitGroup
	for {
		select {
		case <-leaderStop:
			cancel()
			wg.Wait()
			return
		case <-time.After(1 * time.Second):
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := b.TryAdvanceHWMark(ctx); err != nil {
					b.Logger.WithError(err).
						Printf("unable to advance hw mark")
				}
			}()
		case <-rearrangeTimer.C:
			b.rearrangePartitionsLeadership()
		}

	}
}

func (b *Broker) TryAdvanceHWMark(ctx context.Context) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	b.Debugf("TryAdvanceHWMark")

	var op map[string]map[string]uint64
	var mu sync.Mutex
	setHWMark := func(topic, partition string, index uint64) {
		mu.Lock()
		defer mu.Unlock()

		if op == nil {
			op = map[string]map[string]uint64{}
		}

		if op[topic] == nil {
			op[topic] = map[string]uint64{}
		}
		op[topic][partition] = index
	}

	getEndOfLog := func(ctx context.Context, replica string, topic, partition string) (uint64, error) {
		var (
			resp *sgproto.EndOfLogReply
			err  error
		)

		if replica == b.Name() {
			resp, err = b.EndOfLog(ctx, &sgproto.EndOfLogRequest{
				Topic:     topic,
				Partition: partition,
			})
		} else {
			n := b.peers[replica]
			resp, err = n.EndOfLog(ctx, &sgproto.EndOfLogRequest{
				Topic:     topic,
				Partition: partition,
			})
		}
		if err != nil {
			return 0, err
		}

		return resp.Index, nil
	}

	group, ctx := errgroup.WithContext(ctx)
	for _, t := range b.raft.GetTopics() {
		t := t
		for _, partition := range t.Partitions {
			partition := partition
			group.Go(func() error {

				hwMark := b.raft.GetHWMark(t.Name, partition.Id)
				logger := b.WithFields(logrus.Fields{
					"topic":     t.Name,
					"partition": partition.Id,
					"replicas":  partition.Replicas,
					"hw_mark":   hwMark,
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
					return nil
				}

				var newHWMark uint64
				for _, r := range aliveReplicas {
					index, err := getEndOfLog(ctx, r, t.Name, partition.Id)
					if err != nil {
						logger.WithError(err).Printf("error getting EndOfLog")
						return err
					}

					if index > hwMark && (index < newHWMark || newHWMark == 0) {
						newHWMark = index
					}
				}

				if newHWMark != 0 {
					setHWMark(t.Name, partition.Id, newHWMark)
				}

				return nil
			})
		}
	}

	if err := group.Wait(); err != nil {
		return err
	}

	if len(op) == 0 {
		return nil
	}

	return b.raft.SetPartitionHWMark(op)
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
