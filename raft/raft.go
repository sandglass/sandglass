package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/celrenheit/sandglass"

	"github.com/celrenheit/sandglass/logy"
	"github.com/celrenheit/sandglass/topic"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 1 * time.Second
)

const (
	CreateTopicOp            = "CreateTopicOp"
	SetPartitionLeaderOp     = "SetPartitionLeader"
	SetPartitionLeaderBulkOp = "SetPartitionLeaderBulkOp"
)

type Config struct {
	Name          string
	Addr          string
	Dir           string
	StartAsLeader bool
}

type Store struct {
	conf   Config
	raft   *raft.Raft
	logger logy.Logger
	mu     sync.RWMutex

	state            *state
	leaderChangeChan chan bool
	newTopicChan     chan *topic.Topic

	wg sync.WaitGroup
}

type state struct {
	topics           map[string]*topic.Topic
	partitionLeaders map[string]map[string]string
}

func newState() *state {
	return &state{
		topics:           map[string]*topic.Topic{},
		partitionLeaders: map[string]map[string]string{},
	}
}

func New(conf Config, logger logy.Logger) *Store {
	return &Store{
		conf:         conf,
		logger:       logger,
		state:        newState(),
		newTopicChan: make(chan *topic.Topic),
	}
}

func (s *Store) Init(bootstrap bool) error {
	config := raft.DefaultConfig()

	address := s.conf.Addr
	serverId := s.conf.Name
	if serverId == "" {
		serverId = address
	}
	config.Logger = log.New(os.Stdout, "raft["+serverId+"] ", log.LstdFlags)
	config.LocalID = raft.ServerID(serverId)
	config.StartAsLeader = s.conf.StartAsLeader

	addr, err := net.ResolveTCPAddr("tcp", s.conf.Addr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(s.conf.Addr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(s.conf.Dir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.conf.Dir, "raft.db"))
	if err != nil {
		return err
	}

	if bootstrap {
		hasState, err := raft.HasExistingState(logStore, logStore, snapshots)
		if err != nil {
			return err
		}

		if !hasState {
			configuration := raft.Configuration{}
			configuration.Servers = append(configuration.Servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(serverId),
				Address:  raft.ServerAddress(address),
			})
			err := raft.BootstrapCluster(config, logStore, logStore, snapshots, transport, configuration)
			if err != nil {
				return err
			}
		}
	}

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra
	return nil
}

func (s *Store) AddNode(n *sandglass.Node) error {
	s.logger.Info("adding node: %+v", n)
	return s.raft.AddVoter(raft.ServerID(n.Name), raft.ServerAddress(n.RAFTAddr), 0, raftTimeout).Error()
}

func (s *Store) RemoveNode(n *sandglass.Node) error {
	s.logger.Info("removing node: %+v", n)
	return s.raft.RemoveServer(raft.ServerID(n.Name), 0, raftTimeout).Error()
}

func (s *Store) Stop() error {
	return s.raft.Shutdown().Error()
}

func (s *Store) Leader() string {
	return string(s.raft.Leader())
}

func (s *Store) LeaderCh() <-chan bool {
	return s.raft.LeaderCh()
}

func (s *Store) NewTopicChan() chan *topic.Topic {
	return s.newTopicChan
}

func (s *Store) GetTopics() []*topic.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*topic.Topic, 0, len(s.state.topics))
	for _, t := range s.state.topics {
		out = append(out, t)
	}

	return out
}

func (s *fsm) HasTopic(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.state.topics {
		if t.Name == name {
			return true
		}
	}

	return false
}

type fsm Store

func (f *fsm) Apply(l *raft.Log) (value interface{}) {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	var val interface{}
	switch c.Op {
	case CreateTopicOp:
		val = f.applySetTopic(c.Payload)
	case SetPartitionLeaderBulkOp:
		val = f.applySetPartitionLeaderBulk(c.Payload)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}

	if val != nil {
		f.logger.Debug("Apply err: %v", val)
		return val
	}

	return nil
}

func (f *fsm) applySetTopic(b []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(b, &t); err != nil {
		return err
	}
	f.logger.Info("applyCreateTopic %v", t.Name)

	if !f.HasTopic(t.Name) {
		err := t.InitStore(f.conf.Dir)
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.state.topics[t.Name] = &t
		f.mu.Unlock()
	}

	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	state := newState()
	for _, topic := range f.state.topics {
		state.topics[topic.Name] = topic
	}

	for topic, partitions := range f.state.partitionLeaders {
		state.partitionLeaders[topic] = make(map[string]string)
		for part, leader := range partitions {
			state.partitionLeaders[topic][part] = leader
		}
	}

	return &fsmSnapshot{state}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	var restoredState state
	if err := json.NewDecoder(rc).Decode(&f); err != nil {
		return err
	}

	*f.state = restoredState
	return nil
}

func (f *fsm) applySetPartitionLeaderBulk(d []byte) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var p setPartitionLeaderBulk
	if err := json.Unmarshal(d, &p); err != nil {
		return err
	}

	for topic, partitions := range p.State {
		if f.state.partitionLeaders[topic] == nil {
			f.state.partitionLeaders[topic] = map[string]string{}
		}

		for part, leader := range partitions {
			f.state.partitionLeaders[topic][part] = leader
		}
	}

	return nil
}

type command struct {
	Op      string          `json:"op,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (s *Store) CreateTopic(t *topic.Topic) error {
	return s.raftApplyCommand(CreateTopicOp, t)
}

func (s *Store) SetPartitionLeaderOp(topic, partition, leader string) error {
	return s.raftApplyCommand(SetPartitionLeaderOp, &setPartitionLeaderPayload{
		topic: topic, partition: partition, leader: leader,
	})
}

func (s *Store) GetTopic(name string) *topic.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.state.topics[name]
}

type setPartitionLeaderBulk struct {
	State map[string]map[string]string
}

func (s *Store) SetPartitionLeaderBulkOp(v map[string]map[string]string) error {
	return s.raftApplyCommand(SetPartitionLeaderBulkOp, setPartitionLeaderBulk{
		State: v,
	})
}

func (s *Store) GetPartitionLeader(topic, partition string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	partitions, ok := s.state.partitionLeaders[topic]
	if !ok {
		return "", false
	}

	leader, ok := partitions[partition]
	return leader, ok
}

type setPartitionLeaderPayload struct {
	topic, partition, leader string
}

func (s *Store) raftApplyCommand(op string, v interface{}) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	d, err := json.Marshal(v)
	if err != nil {
		return err
	}

	cmd := &command{
		Op:      op,
		Payload: d,
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

type fsmSnapshot struct {
	state *state
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.state)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
