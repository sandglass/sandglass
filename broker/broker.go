package broker

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass"
	"github.com/celrenheit/sandglass/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"net/http"
	_ "net/http/pprof"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/topic"
	"github.com/celrenheit/sandglass/watchy"
)

const (
	ConsumerOffsetTopicName = "consumer_offsets"
)

var DefaultStateCheckInterval = 1 * time.Second

func init() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	rand.Seed(time.Now().UnixNano())
}

type Config struct {
	Name                    string        `yaml:"name,omitempty"`
	DCName                  string        `yaml:"dc_name,omitempty"`
	BindAddr                string        `yaml:"bind_addr,omitempty"`
	AdvertiseAddr           string        `yaml:"advertise_addr,omitempty"`
	DBPath                  string        `yaml:"db_path,omitempty"`
	GossipPort              string        `yaml:"gossip_port,omitempty"`
	HTTPPort                string        `yaml:"http_port,omitempty"`
	GRPCPort                string        `yaml:"grpc_port,omitempty"`
	RaftPort                string        `yaml:"raft_port,omitempty"`
	InitialPeers            []string      `yaml:"initial_peers,omitempty"`
	BootstrapRaft           bool          `yaml:"bootstrap_raft,omitempty"`
	LoggingLevel            *logrus.Level `yaml:"-"`
	OffsetReplicationFactor int           `yaml:"-"`
}

type Broker struct {
	*logrus.Entry
	cluster    *serf.Serf
	conf       *Config
	eventCh    chan serf.Event
	ShutdownCh chan struct{}

	nodes       map[string]string
	mu          sync.RWMutex
	peers       map[string]*sandglass.Node
	currentNode *sandglass.Node

	idgen     sandflake.Generator
	consumers map[string]*ConsumerGroup

	eventEmitter   *watchy.EventEmitter
	readyListeners []chan interface{}
	wg             sync.WaitGroup
	raft           *raft.Store

	reconcileCh chan serf.Member
}

func New(conf *Config) (*Broker, error) {
	if conf.Name == "" {
		conf.Name = uuid.NewV4().String()
	}

	if conf.DCName == "" {
		conf.DCName = "dc1"
	}

	if _, err := os.Stat(conf.DBPath); os.IsNotExist(err) {
		if err := os.Mkdir(conf.DBPath, 0755); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	level := logrus.InfoLevel
	if conf.LoggingLevel != nil {
		level = *conf.LoggingLevel
	}

	logrus.SetLevel(level)
	logger := logrus.WithField("broker", conf.Name)

	b := &Broker{
		conf:         conf,
		ShutdownCh:   make(chan struct{}),
		Entry:        logger,
		nodes:        make(map[string]string),
		peers:        map[string]*sandglass.Node{},
		consumers:    map[string]*ConsumerGroup{},
		eventEmitter: watchy.New(),
		reconcileCh:  make(chan serf.Member, 64),
	}
	return b, nil
}

func (b *Broker) Stop(ctx context.Context) error {
	gracefulCh := make(chan error)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	go func() {
		close(b.ShutdownCh)

		if err := b.cluster.Leave(); err != nil {
			gracefulCh <- errors.Wrap(err, "error while leaving cluster")
			return
		}

		if err := b.cluster.Shutdown(); err != nil {
			gracefulCh <- errors.Wrap(err, "error while leaving cluster")
			return
		}

		err := b.raft.Stop()
		if err != nil {
			b.WithError(err).Debugf("error while stopping raft")
		}

		b.wg.Wait()

		for _, peer := range b.peers {
			if err := peer.Close(); err != nil {
				gracefulCh <- errors.Wrapf(err, "error while closing peer: %v", peer.Name)
				return
			}
		}

		close(gracefulCh)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("timeout for gracefull shutdown")
	case err := <-gracefulCh:
		if err != nil {
			return err
		}
		// return nil
	}

	b.Debugf("closing topics dbs...")
	// closing topics
	for _, t := range b.raft.GetTopics() {
		if err := t.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (b *Broker) Members() []*sandglass.Node {
	b.mu.RLock()
	defer b.mu.RUnlock()
	peers := make([]*sandglass.Node, 0, len(b.peers))
	for _, n := range b.peers {
		peers = append(peers, n)
	}
	return peers
}

func (b *Broker) Conf() *Config {
	return b.conf
}

func (b *Broker) LaunchWatchers() error {
	var group errgroup.Group
	group.Go(func() error {
		for {
			select {
			case <-b.ShutdownCh:
				return nil
			case <-time.After(300 * time.Millisecond):
			}
			b.Debugf("launching watchTopic")
			err := b.watchTopic()
			if err == nil {
				return nil
			}
			b.WithError(err).Debugf("error in watchTopic")
		}
	})

	group.Go(b.monitorLeadership)
	return group.Wait()
}

func (b *Broker) getNode(name string) *sandglass.Node {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.peers[name]
}

func (b *Broker) getNodeByRaftAddr(addr string) *sandglass.Node {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, p := range b.peers {
		if p.RAFTAddr == addr {
			return p
		}
	}

	return nil
}

func (b *Broker) Bootstrap() error {
	b.Debugf("Bootstrapping %s...", b.Name())
	b.Debugf("config: %+v", b.Conf())
	b.readyListeners = append(b.readyListeners,
		b.eventEmitter.Once(leaderElectedEvent),
		b.eventEmitter.Once("topics:created:"+ConsumerOffsetTopicName),
	)

	conf := serf.DefaultConfig()
	conf.Init()

	port, err := strconv.Atoi(b.conf.GossipPort)
	if err != nil {
		return err
	}

	var advAddr string

	if b.conf.AdvertiseAddr != "" {
		advAddr, err = resolveAddr(b.conf.AdvertiseAddr)
		if err != nil {
			return err
		}
	}

	b.currentNode = &sandglass.Node{
		Name:     b.conf.Name,
		HTTPAddr: net.JoinHostPort(advAddr, b.conf.HTTPPort),
		GRPCAddr: net.JoinHostPort(advAddr, b.conf.GRPCPort),
		RAFTAddr: net.JoinHostPort(advAddr, b.conf.RaftPort),
	}

	conf.MemberlistConfig.BindAddr = b.conf.BindAddr
	conf.MemberlistConfig.BindPort = port
	conf.MemberlistConfig.AdvertiseAddr = advAddr
	conf.MemberlistConfig.AdvertisePort = port
	conf.NodeName = b.Name()
	conf.Tags["id"] = b.currentNode.ID
	conf.Tags["http_addr"] = net.JoinHostPort(advAddr, b.conf.HTTPPort)
	conf.Tags["grpc_addr"] = net.JoinHostPort(advAddr, b.conf.GRPCPort)
	conf.Tags["raft_addr"] = net.JoinHostPort(advAddr, b.conf.RaftPort)
	if b.Logger.Level < logrus.DebugLevel {
		conf.LogOutput = ioutil.Discard
		conf.MemberlistConfig.LogOutput = ioutil.Discard
	} else {
		serfLogger := log.New(os.Stdout, "serf["+b.Name()+"] ", log.LstdFlags)
		conf.Logger = serfLogger
		conf.MemberlistConfig.Logger = serfLogger
	}

	b.eventCh = make(chan serf.Event, 64)
	conf.EventCh = b.eventCh

	cluster, err := serf.Create(conf)
	if err != nil {
		return errors.Wrap(err, "Couldn't create cluster")
	}

	b.cluster = cluster

	b.raft = raft.New(raft.Config{
		Name:     b.conf.Name,
		BindAddr: net.JoinHostPort(b.conf.BindAddr, b.conf.RaftPort),
		AdvAddr:  net.JoinHostPort(advAddr, b.conf.RaftPort),
		Dir:      b.conf.DBPath,
	}, b.Entry)

	if err := b.raft.Init(b.conf.BootstrapRaft, cluster, b.reconcileCh); err != nil {
		return err
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		err := b.LaunchWatchers()
		if err != nil {
			b.WithError(err).Fatal("error launch watchers")
		}
		b.Debugf("Stopped watchers")
	}()

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.eventLoop()
	}()

	b.syncWatcher()

	return nil
}

func (b *Broker) WaitForIt() error {
	// return nil
	readyCh := make(chan struct{})

	var wg sync.WaitGroup

	for i := range b.readyListeners {
		ch := b.readyListeners[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ch
		}()
	}

	go func() {
		wg.Wait()
		close(readyCh)
	}()

	select {
	case <-readyCh:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("bootstrap: timing out waiting for it to be ready")
	}
}

func resolveAddr(host string) (string, error) {
	// FIXME: this is shit
	var port string
	if strings.Contains(host, ":") {
		var err error
		host, port, err = net.SplitHostPort(host)
		if err != nil {
			return "", err
		}
	}

	if ip := net.ParseIP(host); ip == nil {
		addr, err := net.ResolveUDPAddr("udp", host+":0")
		if err != nil {
			return "", err
		}
		host = addr.IP.String()
	}

	if port != "" {
		host = net.JoinHostPort(host, port)
	}

	return host, nil
}

func (b *Broker) Join(clusterAddrs ...string) (err error) {
	resolved := make([]string, len(clusterAddrs))
	for i, addr := range clusterAddrs {
		resolved[i], err = resolveAddr(addr)
		if err != nil {
			return err
		}
	}

	_, err = b.cluster.Join(clusterAddrs, false)
	if err != nil {
		b.WithError(err).Debugf("Couldn't join cluster, starting own")
		// return err
	}

	return nil
}

func (b *Broker) Name() string {
	return b.conf.Name
}

func (b *Broker) eventLoop() {
	shutdownCh := b.cluster.ShutdownCh()
loop:
	for {
		select {
		case <-b.ShutdownCh:
			break loop
		case <-shutdownCh:
			break loop
		case e := <-b.eventCh:
			b.WithField("event", e).Debugf("received gossip event")
			switch e.EventType() {
			case serf.EventMemberJoin:
				ev := e.(serf.MemberEvent)
				b.addPeer(ev)
				for _, member := range ev.Members {
					b.reconcileCh <- member
				}
			case serf.EventMemberLeave, serf.EventMemberFailed:
				ev := e.(serf.MemberEvent)
				b.removePeer(ev)
				for _, member := range ev.Members {
					b.reconcileCh <- member
				}
				if b.IsController() {
					b.wg.Add(1)
					go func() {
						defer b.wg.Done()
						if err := b.rearrangePartitionsLeadership(); err != nil {
							b.WithError(err).Debugf("error while rearrangeLeadership")
						}
					}()
				}
			case serf.EventQuery:
				qry := e.(*serf.Query)
				b.Debugf("received query: %v", qry)
				topicName := string(qry.Payload)
				if b.getTopic(topicName) != nil {
					err := qry.Respond([]byte("OK"))
					if err != nil {
						b.WithError(err).Info("got error responding")
					}
					continue
				}
				ch := b.eventEmitter.Once("topics:created:" + topicName)
				go func() {
					<-ch
					err := qry.Respond([]byte("OK"))
					if err != nil {
						b.WithError(err).Info("got error responding")
					}
				}()
			}
		}
	}
}

func (b *Broker) syncWatcher() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()

		for {
			select {
			case <-b.ShutdownCh:
				return
			case <-time.After(DefaultStateCheckInterval):
				err := b.TriggerSyncRequest()
				if err != nil {
					b.WithError(err).Debugf("error while making sync request")
				}
			}
		}
	}()
}

func (b *Broker) TriggerSyncRequest() error {
	ctx := context.TODO()
	var group errgroup.Group
	for _, t := range b.Topics() {
		t := t
		for _, p := range t.ListPartitions() {
			p := p
			if !b.isReplicaForTopicPartition(t.Name, p.Id) || b.isLeaderForTopicPartition(t.Name, p.Id) {
				continue
			}

			last := p.LastWALEntry()

			leader := b.getPartitionLeader(t.Name, p.Id)
			if leader == nil {
				// b.Debugf("sync no leader (t:%s p:%s)", t.Name, p.Id)
				continue
			}

			logger := b.WithFields(logrus.Fields{
				"leader":    leader.Name,
				"topic":     t.Name,
				"partition": p.Id,
			})
			if !leader.IsAlive() {
				logger.Debugf("skipping leader")
				continue
			}

			// b.Debugf("syncing with %v for (t:%s p:%s) last=%v", leader.Name, t.Name, p.Id, last)

			group.Go(func() error {
				stream, err := leader.FetchFromSync(ctx, &sgproto.FetchFromSyncRequest{
					Topic:     t.Name,
					Partition: p.Id,
					From:      last,
				})
				if err != nil {
					return err
				}

				var (
					n    int
					msgs = []*sgproto.Message{}
				)

				for {
					msg, err := stream.Recv()
					if err == io.EOF {
						break
					}

					if err != nil {
						return err
					}

					msgs = append(msgs, msg)
					if len(msgs) == 1000 {
						n += len(msgs)
						if err := p.BatchPutMessages(msgs); err != nil {
							return err
						}
					}
				}

				if len(msgs) > 0 {
					n += len(msgs)
					if err := p.BatchPutMessages(msgs); err != nil {
						return err
					}
				}

				if n > 0 {
					logger.WithField("msgs_synced", n).Debugf("synced %d messages", n)
				}

				return nil
			})
		}
	}

	return group.Wait()
}

func (b *Broker) addPeer(ev serf.MemberEvent) error {

	for _, m := range ev.Members {
		peer, err := extractPeer(m)
		if err != nil {
			return err
		}
		b.Debugf("adding peer: %v", peer.Name)
		if m.Name != b.Name() {
			if err := peer.Dial(); err != nil {
				b.WithError(err).WithFields(logrus.Fields{
					"peer": peer.Name,
				}).Debugf("addPeer error while dialing peer")
			}

		}

		b.mu.Lock()
		b.peers[peer.Name] = peer
		b.mu.Unlock()
	}
	return nil
}

func (b *Broker) removePeer(ev serf.MemberEvent) error {

	for _, m := range ev.Members {
		b.mu.RLock()
		peer, ok := b.peers[m.Name]
		b.mu.RUnlock()
		if ok {
			logger := b.WithFields(logrus.Fields{
				"peer": peer.Name,
			})
			if err := peer.Close(); err != nil {
				logger.WithError(err).Debugf("error while closing peer")
			}

			b.mu.Lock()
			delete(b.peers, peer.Name)
			b.mu.Unlock()
			logger.Debugf("removed peer")
		}

	}
	return nil
}

func extractPeer(m serf.Member) (*sandglass.Node, error) {

	peer := &sandglass.Node{
		ID:       m.Tags["id"],
		IP:       m.Addr.String(),
		Name:     m.Name,
		GRPCAddr: m.Tags["grpc_addr"],
		HTTPAddr: m.Tags["http_addr"],
		RAFTAddr: m.Tags["raft_addr"],
		Status:   m.Status,
	}

	return peer, nil
}

func (b *Broker) Topics() []*topic.Topic {
	return b.raft.GetTopics()
}

func (b *Broker) GetTopic(name string) *topic.Topic {
	return b.raft.GetTopic(name)
}
