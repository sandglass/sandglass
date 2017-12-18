package broker

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/celrenheit/sandglass/sgutils"
	"golang.org/x/sync/errgroup"

	"github.com/celrenheit/sandglass/topic"

	"fmt"

	"io/ioutil"

	"os"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/broker"
	"github.com/celrenheit/sandglass/logy"
	"github.com/celrenheit/sandglass/server"
	"github.com/stretchr/testify/require"
)

// func TestLeak(t *testing.T) {
// 	defer leaktest.Check(t)()
// 	n := 1
// 	_, destroyFn := makeNBrokers(t, n)
// 	time.Sleep(1 * time.Second)
// 	destroyFn()
// }

var ctx = context.TODO()

func TestSandglass(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.CreateTopicParams{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(t, err)

	err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(t, err)

	require.Len(t, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(t, brokers[i].Topics(), 2)
	}

	part := getTopicFromBroker(brokers[0], "payments").Partitions[0].Id
	for i := 0; i < 1000; i++ {
		_, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: part,
			Messages: []*sgproto.Message{
				{
					Value: []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(t, err)
	}

	var count int
	req := &sgproto.FetchRangeRequest{
		Topic:     "payments",
		Partition: part,
		From:      sandflake.Nil,
		To:        sandflake.MaxID,
	}
	err = brokers[0].FetchRange(ctx, req, func(keymsg *sgproto.Message) error {
		count++
		return nil
	})
	require.Nil(t, err)

	require.Equal(t, 1000, count)
}

func TestKVTopic(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.CreateTopicParams{
		Name:              "payments",
		Kind:              sgproto.TopicKind_KVKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(t, err)

	err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(t, err)

	require.Len(t, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(t, brokers[i].Topics(), 2)
	}

	part := getTopicFromBroker(brokers[0], "payments").Partitions[0].Id
	for i := 0; i < 1000; i++ {
		_, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: part,
			Messages: []*sgproto.Message{
				{
					Key:   []byte("my_key"),
					Value: []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(t, err)
	}

	var count int
	req := &sgproto.FetchRangeRequest{
		Topic:     "payments",
		Partition: part,
		From:      sandflake.Nil,
		To:        sandflake.MaxID,
	}
	err = brokers[0].FetchRange(ctx, req, func(msg *sgproto.Message) error {
		require.Equal(t, "my_key", string(msg.Key))
		count++
		return nil
	})
	require.Nil(t, err)

	require.Equal(t, 1, count)

	msg, err := brokers[0].Get(ctx, "payments", part, []byte("my_key"))
	require.NoError(t, err)
	require.Equal(t, "999", string(msg.Value))
}

func TestACK(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.CreateTopicParams{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(t, err)

	err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(t, err)

	require.Len(t, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(t, brokers[i].Topics(), 2)
	}

	b := brokers[2]
	var topic *topic.Topic
	for _, t := range brokers[0].Topics() {
		if t.Name == createTopicParams.Name {
			topic = t
		}
	}

	var g sandflake.Generator
	offset := g.Next()
	ok, err := b.Acknowledge(ctx, topic.Name, topic.Partitions[0].Id, "group1", "cons1", offset)
	require.Nil(t, err)
	require.True(t, ok)

	got, err := b.LastOffset(ctx, topic.Name, topic.Partitions[0].Id, "group1", "cons1",
		sgproto.MarkKind_Commited)
	require.Nil(t, err)
	require.Equal(t, sandflake.Nil, got)

	got, err = b.LastOffset(ctx, topic.Name, topic.Partitions[0].Id, "group1", "cons1",
		sgproto.MarkKind_Acknowledged)
	require.Nil(t, err)
	require.Equal(t, offset, got)

	offset2 := g.Next()
	ok, err = b.Commit(ctx, topic.Name, topic.Partitions[0].Id, "group1", "cons1", offset2)
	require.Nil(t, err)
	require.True(t, ok)

	got, err = b.LastOffset(ctx, topic.Name, topic.Partitions[0].Id, "group1", "cons1",
		sgproto.MarkKind_Commited)
	require.Nil(t, err)
	require.Equal(t, offset2, got)

	got, err = b.LastOffset(ctx, topic.Name, topic.Partitions[0].Id, "group1", "cons1",
		sgproto.MarkKind_Acknowledged)
	require.Nil(t, err)
	require.Equal(t, offset, got)
}

func TestConsume(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.CreateTopicParams{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(t, err)

	err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(t, err)

	require.Len(t, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(t, brokers[i].Topics(), 2)
	}

	b := brokers[2]
	var topic *topic.Topic
	for _, t := range brokers[0].Topics() {
		if t.Name == createTopicParams.Name {
			topic = t
		}
	}

	var gen sandflake.Generator
	var want sandflake.ID
	var ids []sandflake.ID
	for i := 0; i < 30; i++ {
		want = gen.Next()
		_, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: topic.Partitions[0].Id,
			Messages: []*sgproto.Message{
				{
					Offset: want,
					Key:    []byte("my_key"),
					Value:  []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(t, err)
		ids = append(ids, want)
	}

	fmt.Println("-----------------------------")
	var count int
	var got sandflake.ID
	err = b.Consume(ctx, "payments", topic.Partitions[0].Id, "group1", "cons1", func(msg *sgproto.Message) error {
		count++
		ok, err := b.Acknowledge(ctx, topic.Name, topic.Partitions[0].Id, "group1", "cons1", msg.Offset)
		require.True(t, ok)
		got = msg.Offset
		return err
	})
	require.Nil(t, err)
	require.Equal(t, 30, count)
	require.Equal(t, want, got)

	for i := 0; i < 20; i++ {
		res, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: topic.Partitions[0].Id,
			Messages: []*sgproto.Message{
				{
					Value: []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(t, err)
		want = res.Offsets[len(res.Offsets)-1]
	}

	fmt.Println("-----------------------------")
	count = 0
	err = b.Consume(ctx, "payments", topic.Partitions[0].Id, "group1", "cons1", func(msg *sgproto.Message) error {
		count++
		got = msg.Offset
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, 20, count)
	require.Equal(t, want, got)

	count = 0
	err = b.Consume(ctx, "payments", topic.Partitions[0].Id, "group1", "cons1", func(msg *sgproto.Message) error {
		count++
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, count)

	broker.RedeliveryTimeout = 100 * time.Millisecond // this should trigger redelivery
	broker.MaxRedeliveryCount = 3
	for i := 0; i < broker.MaxRedeliveryCount; i++ {

		time.Sleep(150 * time.Millisecond)

		count = 0
		err = b.Consume(ctx, "payments", topic.Partitions[0].Id, "group1", "cons1", func(msg *sgproto.Message) error {
			count++
			return nil
		})
		require.Nil(t, err)
		require.Equal(t, 20, count)
	}

	time.Sleep(150 * time.Millisecond)

	count = 0
	err = b.Consume(ctx, "payments", topic.Partitions[0].Id, "group1", "cons1", func(msg *sgproto.Message) error {
		count++
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, count)
}

func TestSyncRequest(t *testing.T) {
	broker.DefaultStateCheckInterval = 300 * time.Second
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.CreateTopicParams{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(t, err)

	err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(t, err)

	require.Len(t, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(t, brokers[i].Topics(), 2)
	}

	topic := getTopicFromBroker(brokers[0], createTopicParams.Name)

	part := topic.Partitions[0]

	var gen sandflake.Generator
	var lastPublishedID sandflake.ID
	for i := 0; i < 5; i++ {
		lastPublishedID = gen.Next()
		_, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: part.Id,
			Messages: []*sgproto.Message{
				{
					Offset: lastPublishedID,
					Value:  []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(t, err)
	}

	for _, b := range brokers {
		err := b.TriggerSyncRequest()
		require.NoError(t, err)
	}

	lastOffsets := make(map[string]sandflake.ID)
	for _, b := range brokers {
		if sgutils.StringSliceHasString(part.Replicas, b.Name()) {
			tt := getTopicFromBroker(b, createTopicParams.Name)
			p := tt.GetPartition(part.Id)
			lastMsg, err := p.LastMessage()
			require.NoError(t, err)
			var lastOffset sandflake.ID
			if lastMsg != nil {
				lastOffset = lastMsg.Offset
			}
			lastOffsets[b.Name()] = lastOffset
		}
	}
	fmt.Printf("lastPublishedID: %+v\n", lastPublishedID)
	fmt.Printf("replicas: %+v\n", part.Replicas)
	fmt.Printf("partition: %+v\n", part.Id)
	fmt.Printf("lastOffsets: %+v\n", lastOffsets)
	require.Len(t, lastOffsets, len(part.Replicas))
	for host, offset := range lastOffsets {
		require.Equal(t, lastPublishedID, offset, "host '%v' does not match", host)
	}
}

func getTopicFromBroker(b *broker.Broker, topic string) *topic.Topic {
	for _, t := range b.Topics() {
		if t.Name == topic {
			return t
		}
	}

	return nil
}

func BenchmarkKVTopicGet(b *testing.B) {
	n := 3
	brokers, destroyFn := makeNBrokers(b, n)
	defer destroyFn()

	createTopicParams := &sgproto.CreateTopicParams{
		Name:              "payments",
		Kind:              sgproto.TopicKind_KVKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(b, err)

	err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(b, err)

	require.Len(b, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(b, brokers[i].Topics(), 1)
	}

	for i := 0; i < 30; i++ {
		_, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic: "payments",
			Messages: []*sgproto.Message{
				{
					Key:   []byte("my_key"),
					Value: []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg, err := brokers[0].Get(ctx, "payments", "", []byte("my_key"))
			require.NoError(b, err)
			require.Equal(b, "29", string(msg.Value))
		}
	})
}

func BenchmarkConsume(b *testing.B) {
	n := 3
	brokers, destroyFn := makeNBrokers(b, n)
	defer destroyFn()

	createTopicParams := &sgproto.CreateTopicParams{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
		StorageDriver:     sgproto.StorageDriver_Badger,
	}
	err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(b, err)

	err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(b, err)

	require.Len(b, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(b, brokers[i].Topics(), 2)
	}

	payments := getTopicFromBroker(brokers[0], "payments")
	require.NotNil(b, payments)

	N := 1000
	for i := 0; i < N; i++ {
		_, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: payments.Partitions[0].Id,
			Messages: []*sgproto.Message{
				{
					Key:   []byte("my_key"),
					Value: []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.Run("consumption", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var gen sandflake.Generator
		for i := 0; i < b.N; i++ {
			count := 0
			err := brokers[0].Consume(context.Background(),
				payments.Name,
				payments.Partitions[0].Id,
				gen.Next().String(),
				"consumerName",
				func(msg *sgproto.Message) error {
					count++
					return nil
				})
			require.NoError(b, err)
			require.Equal(b, N, count)
		}
	})
}

func makeNBrokers(tb testing.TB, n int) (brokers []*broker.Broker, destroyFn func()) {
	var g sandflake.Generator
	dc := g.Next()
	paths := []string{}
	for i := 0; i < n; i++ {
		basepath, err := ioutil.TempDir("", "")
		require.Nil(tb, err)
		paths = append(paths, basepath)
		bind_addr := "localhost"
		advertise_addr := "127.0.0.1"
		_, gossip_port, err := net.SplitHostPort(RandomAddr())
		require.NoError(tb, err)
		_, grpc_port, err := net.SplitHostPort(RandomAddr())
		require.NoError(tb, err)
		_, http_port, err := net.SplitHostPort(RandomAddr())
		require.NoError(tb, err)
		_, raft_port, err := net.SplitHostPort(RandomAddr())
		require.NoError(tb, err)
		brokers = append(brokers, newBroker(tb, i, dc.String(), bind_addr, advertise_addr, gossip_port, grpc_port, http_port, raft_port, basepath))
	}

	servers := []*server.Server{}
	var doneServers sync.WaitGroup
	for i := 0; i < n; i++ {
		grpc_addr := net.JoinHostPort(brokers[i].Conf().BindAddr, brokers[i].Conf().GRPCPort)
		http_addr := net.JoinHostPort(brokers[i].Conf().BindAddr, brokers[i].Conf().HTTPPort)

		server := server.New(brokers[i], grpc_addr, http_addr, logy.NewWithLogger(logger, logy.INFO))
		doneServers.Add(1)
		go func() {
			defer doneServers.Done()
			server.Start()
			// require.Nil(t, err)
		}()

		servers = append(servers, server)
	}

	peers := []string{}
	for _, b := range brokers[1:] {
		peers = append(peers, net.JoinHostPort(b.Conf().AdvertiseAddr, b.Conf().GossipPort))
		err := b.Join(net.JoinHostPort(brokers[0].Conf().AdvertiseAddr, brokers[0].Conf().GossipPort))
		require.Nil(tb, err)
	}

	err := brokers[0].Join(peers...)
	require.Nil(tb, err)

	var group errgroup.Group
	for _, b := range brokers {
		b := b
		group.Go(b.WaitForIt)
	}

	err = group.Wait()
	require.Nil(tb, err)

	destroyFn = func() {
		for _, b := range brokers {
			err := b.Stop(context.Background())
			require.Nil(tb, err)
		}
		for _, s := range servers {
			err := s.Shutdown(context.Background())
			require.Nil(tb, err)
		}
		doneServers.Wait()
		for _, p := range paths {
			os.RemoveAll(p)
		}
	}
	return
}

var logger = log.New(os.Stdout, "", log.LstdFlags)

func newBroker(tb testing.TB, i int, dc, bind_addr, adv_addr, gossip_port, grpc_port, http_port, raft_port, basepath string) *broker.Broker {
	lvl := logy.INFO
	conf := &broker.Config{
		Name:                    "broker" + strconv.Itoa(i),
		DCName:                  dc,
		BindAddr:                bind_addr,
		AdvertiseAddr:           adv_addr,
		DBPath:                  basepath,
		GossipPort:              gossip_port,
		GRPCPort:                grpc_port,
		HTTPPort:                http_port,
		RaftPort:                raft_port,
		BootstrapRaft:           i == 0,
		LoggingLevel:            &lvl,
		OffsetReplicationFactor: 2,
	}
	fmt.Printf("conf: %+v\n", conf)
	fmt.Printf("basepath: %+v\n", basepath)

	b, err := broker.New(conf)
	require.Nil(tb, err)

	err = b.Bootstrap()
	require.Nil(tb, err)

	return b
}

func RandomAddr() string {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}
