package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/celrenheit/sandglass"
	"github.com/celrenheit/sandglass/topic"
	"github.com/sirupsen/logrus"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 1 * time.Second
	barrierWriteTimeout = 2 * time.Minute
)

const (
	CreateTopicOp            = "CreateTopicOp"
	SetPartitionLeaderOp     = "SetPartitionLeader"
	SetPartitionLeaderBulkOp = "SetPartitionLeaderBulkOp"
	AddNode                  = "AddNode"
	DeleteNode               = "DeleteNode"
)

type Config struct {
	Name          string
	BindAddr      string
	AdvAddr       string
	Dir           string
	StartAsLeader bool
}

type Store struct {
	conf   Config
	raft   *raft.Raft
	logger *logrus.Entry
	mu     sync.RWMutex

	state            *state
	leaderChangeChan chan bool
	newTopicChan     chan *topic.Topic

	notifyCh   chan bool
	shutdownCh chan struct{}
	wg         sync.WaitGroup

	serf        *serf.Serf
	reconcileCh chan serf.Member

	transport *raft.NetworkTransport
}

type state struct {
	Members          map[string]struct{}
	Topics           map[string]*topic.Topic
	PartitionLeaders map[string]map[string]string
}

func newState() *state {
	return &state{
		Members:          map[string]struct{}{},
		Topics:           map[string]*topic.Topic{},
		PartitionLeaders: map[string]map[string]string{},
	}
}

func New(conf Config, logger *logrus.Entry) *Store {
	return &Store{
		conf:             conf,
		logger:           logger.WithField("package", "raft"),
		state:            newState(),
		newTopicChan:     make(chan *topic.Topic, 10),
		leaderChangeChan: make(chan bool, 10),
		shutdownCh:       make(chan struct{}),
	}
}

func (s *Store) Init(bootstrap bool, serf *serf.Serf, reconcileCh chan serf.Member) error {
	s.serf = serf
	s.reconcileCh = reconcileCh
	config := raft.DefaultConfig()

	address := s.conf.AdvAddr
	serverId := s.conf.Name
	if serverId == "" {
		serverId = address
	}

	if s.logger.Level < logrus.DebugLevel {
		config.LogOutput = ioutil.Discard
	} else {
		config.Logger = log.New(os.Stdout, "raft["+serverId+"] ", log.LstdFlags)
	}

	config.LocalID = raft.ServerID(serverId)
	config.StartAsLeader = s.conf.StartAsLeader
	config.ShutdownOnRemove = false
	config.SnapshotInterval = 5 * time.Second
	s.notifyCh = make(chan bool, 1)
	config.NotifyCh = s.notifyCh

	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(s.conf.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}
	s.transport = transport

	snapshots, err := raft.NewFileSnapshotStore(s.conf.Dir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.conf.Dir, "raft.db"))
	if err != nil {
		return err
	}

	configuration := raft.Configuration{}
	configuration.Servers = append(configuration.Servers, raft.Server{
		Suffrage: raft.Voter,
		ID:       raft.ServerID(serverId),
		Address:  raft.ServerAddress(address),
	})

	hasState, err := raft.HasExistingState(logStore, logStore, snapshots)
	if err != nil {
		return err
	}
	if bootstrap {
		if !hasState {
			err := raft.BootstrapCluster(config, logStore, logStore, snapshots, transport, configuration)
			if err != nil {
				return err
			}
		}
	} else if hasState {
		err := raft.RecoverCluster(config, (*fsm)(s), logStore, logStore, snapshots, transport, configuration)
		if err != nil {
			return err
		}
	}

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	s.wg.Add(1)
	go s.monitorLeadership()

	return nil
}

func (s *Store) monitorLeadership() {
	defer s.wg.Done()
	raftNotifyCh := s.notifyCh

	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		select {
		case isLeader := <-raftNotifyCh:
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					continue
				}

				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					s.leaderLoop(ch)
				}(weAreLeaderCh)
			default:
				if weAreLeaderCh == nil {
					continue
				}

				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
			}

			s.leaderChangeChan <- isLeader

		case <-s.shutdownCh:
			leaderLoop.Wait()
			return
		}
	}
}

func (s *Store) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member
	establishedLeader := false

	// reassert := func() error {
	// 	if !establishedLeader {
	// 		return fmt.Errorf("leadership has not been established")
	// 	}
	// 	if err := s.revokeLeadership(); err != nil {
	// 		return err
	// 	}
	// 	if err := s.establishLeadership(); err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }

RECONCILE:
	// Setup a reconciliation timer
	reconcileCh = nil
	interval := time.After(5 * time.Second)

	// Apply a raft barrier to ensure our FSM is caught up
	barrier := s.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		s.logger.Debug("failed to wait for barrier: %v", err)
		goto WAIT
	}

	// Check if we need to handle initial leadership actions
	if !establishedLeader {
		if err := s.establishLeadership(); err != nil {
			s.logger.Debug("failed to establish leadership: %v", err)
			goto WAIT
		}
		establishedLeader = true
		defer func() {
			if err := s.revokeLeadership(); err != nil {
				s.logger.Debug("failed to revoke leadership: %v", err)
			}
		}()
	}

	// Reconcile any missing data
	if err := s.reconcile(); err != nil {
		s.logger.Debug("failed to reconcile: %v", err)
		goto WAIT
	}

	reconcileCh = s.reconcileCh

WAIT:
	select {
	case <-stopCh:
		return
	default:
	}

	for {
		select {
		case <-stopCh:
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			s.reconcileMember(member)
		}
	}
}

func (s *Store) reconcileMember(member serf.Member) error {
	if member.Name == s.conf.Name {
		return nil
	}

	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = s.AddVoter(member.Name, member.Tags["raft_addr"], 0)
	case serf.StatusFailed, serf.StatusLeft:
		err = s.RemoveServer(member.Name, 0)
	}
	if err != nil {
		s.logger.Debug("failed to reconcile member: %v: %v",
			member, err)
	}
	return nil
}

func (s *Store) AddVoter(id, address string, prevIndex uint64) error {
	err := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, raftTimeout).Error()
	if err != nil {
		return err
	}

	return s.raftApplyCommand(AddNode, id)
}

func (s *Store) RemoveServer(id string, prevIndex uint64) error {
	if s.conf.Name == id {
		return nil
	}

	err := s.raft.RemoveServer(raft.ServerID(id), 0, raftTimeout).Error()
	if err != nil {
		return err
	}

	return s.raftApplyCommand(DeleteNode, id)
}

func (s *Store) revokeLeadership() error {
	return nil
}

func (s *Store) reconcile() error {
	members := s.serf.Members()
	knownMembers := make(map[string]struct{})
	for _, member := range members {
		if err := s.reconcileMember(member); err != nil {
			return err
		}

		if member.Status == serf.StatusAlive {
			knownMembers[member.Name] = struct{}{}
		}
	}

	return nil
}

func (s *Store) establishLeadership() error {
	return nil
}

func (s *Store) AddNode(n *sandglass.Node) error {
	s.logger.Info("adding node: %+v", n)
	return s.AddVoter(n.Name, n.RAFTAddr, 0)
}

func (s *Store) RemoveNode(n *sandglass.Node) error {
	s.logger.Info("removing node: %+v", n)
	return s.RemoveServer(n.Name, 0)
}

func (s *Store) Stop() error {
	close(s.shutdownCh)
	s.wg.Wait()
	if err := s.raft.Shutdown().Error(); err != nil {
		return err
	}

	return s.transport.Close()
}

func (s *Store) Leader() string {
	return string(s.raft.Leader())
}

func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Store) LeaderCh() <-chan bool {
	return s.leaderChangeChan
}

func (s *Store) NewTopicChan() chan *topic.Topic {
	return s.newTopicChan
}

func (s *Store) GetTopics() []*topic.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*topic.Topic, 0, len(s.state.Topics))
	for _, t := range s.state.Topics {
		out = append(out, t)
	}

	return out
}

func (s *fsm) HasTopic(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.state.Topics {
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
	case AddNode:
		val = f.applyAddOrDeleteNode(true, c.Payload)
	case DeleteNode:
		val = f.applyAddOrDeleteNode(false, c.Payload)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}

	if val != nil {
		f.logger.Debug("Apply err: %v", val)
		return val
	}

	return nil
}

func (f *fsm) applyAddOrDeleteNode(add bool, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.state.Members == nil {
		f.state.Members = map[string]struct{}{}
	}
	if add {
		f.state.Members[string(payload)] = struct{}{}
	} else {
		delete(f.state.Members, string(payload))
	}

	return nil
}

func (f *fsm) applySetTopic(b []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(b, &t); err != nil {
		return err
	}
	f.logger.Debug("applyCreateTopic %v", t.Name)

	if !f.HasTopic(t.Name) {
		err := t.InitStore(f.conf.Dir)
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.state.Topics[t.Name] = &t
		f.mu.Unlock()
		f.newTopicChan <- &t
	}

	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	state := newState()
	for _, topic := range f.state.Topics {
		state.Topics[topic.Name] = topic
	}

	for topic, partitions := range f.state.PartitionLeaders {
		state.PartitionLeaders[topic] = make(map[string]string)
		for part, leader := range partitions {
			state.PartitionLeaders[topic][part] = leader
		}
	}

	for k, v := range f.state.Members {
		state.Members[k] = v
	}

	return &fsmSnapshot{state}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	restoredState := newState()

	if err := json.NewDecoder(rc).Decode(restoredState); err != nil {
		return err
	}
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, t := range f.state.Topics {
		if err := t.Close(); err != nil {
			return err
		}
	}

	f.state = restoredState
	for _, t := range f.state.Topics {
		if err := t.InitStore(f.conf.Dir); err != nil {
			return err
		}
	}
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
		if f.state.PartitionLeaders[topic] == nil {
			f.state.PartitionLeaders[topic] = map[string]string{}
		}

		for part, leader := range partitions {
			f.state.PartitionLeaders[topic][part] = leader
		}
	}

	return nil
}

type command struct {
	Op      string          `json:"op,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func (s *Store) CreateTopic(t *topic.Topic) error {
	if err := t.Validate(); err != nil {
		return err
	}

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

	return s.state.Topics[name]
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
	partitions, ok := s.state.PartitionLeaders[topic]
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
