package broker

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"net/http"
	_ "net/http/pprof"

	"github.com/celrenheit/libkv/store/etcd" // registering custom fork with fixes
	"github.com/celrenheit/sandglass/logy"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/celrenheit/sandglass/topic"
	"github.com/celrenheit/sandglass/watchy"
	"github.com/docker/libkv/store/consul"
)

const (
	ConsumerOffsetTopicName = "consumer_offsets"
	ETCDBasePrefix          = "/sandglass"
)

var DefaultStateCheckInterval = 1 * time.Second

func init() {
	consul.Register()
	etcd.Register()
	// grpclog.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags))
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	rand.Seed(time.Now().UnixNano())
}

type Config struct {
	Name             string   `yaml:"name,omitempty"`
	DCName           string   `yaml:"dc_name,omitempty"`
	AdvertiseAddr    string   `yaml:"advertise_addr,omitempty"`
	DiscoveryBackend string   `yaml:"discovery_backend,omitempty"`
	DiscoveryAddrs   []string `yaml:"discovery_addrs,omitempty"`
	DBPath           string   `yaml:"db_path,omitempty"`
	HTTPAddr         string   `yaml:"http_addr,omitempty"`
	GRPCAddr         string   `yaml:"grpc_addr,omitempty"`
	InitialPeers     []string `yaml:"initial_peers,omitempty"`
}

type Broker struct {
	logy.Logger
	cluster    *serf.Serf
	conf       *Config
	store      store.Store
	eventCh    chan serf.Event
	ShutdownCh chan struct{}
	doneCh     chan struct{}

	nodes       map[string]string
	topics      []*topic.Topic
	mu          sync.RWMutex
	peers       map[string]*sandglass.Node
	currentNode *sandglass.Node

	partitionsLeaders map[string]map[string]string

	controller string
	idgen      sandflake.Generator
	consumers  map[string]*ConsumerGroup

	eventEmitter   *watchy.EventEmitter
	readyListeners []chan interface{}
	wg             sync.WaitGroup

	discPrefix string
}

func New(conf *Config) (*Broker, error) {
	if conf.Name == "" {
		conf.Name = uuid.NewV4().String()
	}

	if conf.DCName == "" {
		conf.DCName = "dc1"
	}

	if _, err := os.Stat(conf.DBPath); os.IsNotExist(err) {
		if os.Mkdir(conf.DBPath, 0755); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	store, err := libkv.NewStore(store.Backend(conf.DiscoveryBackend), conf.DiscoveryAddrs, nil)
	if err != nil {
		return nil, err
	}

	b := &Broker{
		currentNode: &sandglass.Node{
			Name:     conf.Name,
			HTTPAddr: conf.HTTPAddr,
			GRPCAddr: conf.GRPCAddr,
		},
		conf:              conf,
		ShutdownCh:        make(chan struct{}),
		store:             store,
		topics:            []*topic.Topic{},
		doneCh:            make(chan struct{}),
		Logger:            logy.NewWithLogger(log.New(os.Stdout, fmt.Sprintf("[broker: %v] ", conf.Name), log.LstdFlags), logy.DEBUG),
		nodes:             make(map[string]string),
		peers:             map[string]*sandglass.Node{},
		partitionsLeaders: map[string]map[string]string{},
		consumers:         map[string]*ConsumerGroup{},
		eventEmitter:      watchy.New(),
		discPrefix:        fmt.Sprintf("%s/%s", ETCDBasePrefix, conf.DCName),
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

		<-b.doneCh
		b.wg.Wait()
		// closing connection to etcd
		b.store.Close()
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

	b.Info("closing topics dbs...")
	// closing topics
	for _, t := range b.topics {
		for _, p := range t.ListPartitions() {
			if err := p.Close(); err != nil {
				return err
			}
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
			b.Debug("launching watchTopic")
			err := b.watchTopic()
			if err == nil {
				return nil
			}
			b.Debug("error in watchTopic: %v", err)
		}
	})
	return group.Wait()
}

func (b *Broker) getNode(name string) *sandglass.Node {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.peers[name]
}

func (b *Broker) Bootstrap() error {
	b.Info("Bootstrapping...")
	b.readyListeners = append(b.readyListeners,
		b.eventEmitter.Once("topics:created:"+ConsumerOffsetTopicName),
	)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		err := b.LaunchWatchers()
		if err != nil {
			b.Fatal("error launch watchers: %v", err)
		}
		b.Debug("Stopped watchers")
	}()

	conf := serf.DefaultConfig()
	conf.Init()

	host, portStr, err := net.SplitHostPort(b.conf.AdvertiseAddr)
	if err != nil {
		return err
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}

	conf.MemberlistConfig.BindAddr = host
	conf.MemberlistConfig.BindPort = port
	conf.NodeName = b.Name()
	conf.Tags["id"] = b.currentNode.ID
	conf.Tags["http_addr"] = b.currentNode.HTTPAddr
	conf.Tags["grpc_addr"] = b.currentNode.GRPCAddr

	b.eventCh = make(chan serf.Event, 64)
	conf.EventCh = b.eventCh

	cluster, err := serf.Create(conf)
	if err != nil {
		return errors.Wrap(err, "Couldn't create cluster")
	}

	b.cluster = cluster

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.eventLoop()
	}()

	b.runForControllerElection()
	b.followControllerElection()

	b.syncWatcher()

	return nil
}

func (b *Broker) WaitForIt() error {
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

func (b *Broker) Join(clusterAddrs ...string) error {
	_, err := b.cluster.Join(clusterAddrs, false)
	if err != nil {
		b.Debug("Couldn't join cluster, starting own: %v\n", err)
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
			switch e.EventType() {
			case serf.EventMemberJoin:
				ev := e.(serf.MemberEvent)
				b.addPeer(ev)
			case serf.EventMemberLeave, serf.EventMemberFailed:
				ev := e.(serf.MemberEvent)
				b.removePeer(ev)
				if b.IsController() {
					b.wg.Add(1)
					go func() {
						defer b.wg.Done()
						if err := b.rearrangePartitionsLeadership(); err != nil {
							b.Debug("error while rearrangeLeadership err=%v", err)
						}
					}()
				}
			case serf.EventQuery:
				qry := e.(*serf.Query)
				b.Debug("received query: %v", qry)
			}
		}
	}

	close(b.doneCh)
}

func (b *Broker) syncWatcher() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()

		for {
			select {
			case <-b.doneCh:
				return
			case <-time.After(DefaultStateCheckInterval):
				err := b.TriggerSyncRequest()
				if err != nil {
					b.Debug("error while making sync request: %v", err)
				}
			}
		}
	}()
}

func (b *Broker) TriggerSyncRequest() error {
	ctx := context.Background()
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
				b.Debug("sync no leader (t:%s p:%s): %v", t.Name, p.Id)
				continue
			}

			if !leader.IsAlive() {
				b.Debug("skipping leader '%s' (t:%s p:%s): %v", leader.Name, t.Name, p.Id)
				continue
			}

			b.Debug("syncing with %v for (t:%s p:%s) last=%v", leader.Name, t.Name, p.Id, last)

			group.Go(func() error {
				stream, err := leader.FetchFromSync(ctx, &sgproto.FetchFromSyncRequest{
					Topic:     t.Name,
					Partition: p.Id,
					From:      last,
				})
				if err != nil {
					return err
				}

				for {
					msg, err := stream.Recv()
					if err == io.EOF {
						break
					}

					if err != nil {
						return err
					}

					b.Debug("sync received (t:%s p:%s): %v", t.Name, p.Id, msg.Index)

					if err := b.StoreMessageLocally(msg); err != nil {
						return err
					}
				}

				return nil
			})
		}
	}

	return group.Wait()
}

func (b *Broker) addPeer(ev serf.MemberEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, m := range ev.Members {
		peer, err := extractPeer(m)
		if err != nil {
			return err
		}

		if m.Name != b.Name() {
			if err := peer.Dial(); err != nil {
				b.Debug("addPeer error while dialing peer '%s' err=%v", peer.Name, err)
			}
		}
		b.peers[peer.Name] = peer

	}
	return nil
}

func (b *Broker) removePeer(ev serf.MemberEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, m := range ev.Members {
		if peer, ok := b.peers[m.Name]; ok {
			if err := peer.Close(); err != nil {
				b.Debug("error while closing peer '%s' err=%v", peer.Name, err)
			}
			delete(b.peers, peer.Name)
			b.Debug("removed peer: %v", m.Name)
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
		Status:   m.Status,
	}

	return peer, nil
}

func (b *Broker) Topics() []*topic.Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics
}
