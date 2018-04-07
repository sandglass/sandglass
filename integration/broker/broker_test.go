package broker

import (
	"context"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/celrenheit/sandglass/sgutils"
	"golang.org/x/sync/errgroup"

	"github.com/celrenheit/sandglass/topic"

	"fmt"

	"io/ioutil"

	"os"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/broker"
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

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	createTopic(t, brokers, createTopicParams)

	part := getTopicFromBroker(brokers[0], "payments").Partitions[0]
	for i := 0; i < 1000; i++ {
		_, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: part.Id,
			Messages: []*sgproto.Message{
				{
					Value: []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(t, err)
	}

	syncAndAdvance(t, brokers)

	var count int
	req := &sgproto.FetchRangeRequest{
		Topic:     "payments",
		Partition: part.Id,
		From:      sgproto.Nil,
		To:        sgproto.MaxOffset,
	}
	err := brokers[0].FetchRangeFn(ctx, req, func(keymsg *sgproto.Message) error {
		count++
		return nil
	})
	require.Nil(t, err)

	require.Equal(t, 1000, count)
}

func syncAndAdvance(t *testing.T, brokers []*broker.Broker) {
	for _, b := range brokers {
		err := b.TriggerSyncRequest()
		require.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond)

	b := getController(brokers)
	err := b.TryAdvanceHWMark(context.TODO())
	require.NoError(t, err)

	time.Sleep(1100 * time.Millisecond)
}

func getController(brokers []*broker.Broker) *broker.Broker {
	return getBrokerByName(brokers, brokers[0].GetController().Name)
}

func getBrokerByName(brokers []*broker.Broker, name string) *broker.Broker {
	for _, b := range brokers {
		if b.Name() == name {
			return b
		}
	}

	panic("getBrokerByName('" + name + "') not found")
}

func TestKVTopic(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_KVKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	createTopic(t, brokers, createTopicParams)

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

	syncAndAdvance(t, brokers)

	var count int
	req := &sgproto.FetchRangeRequest{
		Topic:     "payments",
		Partition: part,
		From:      sgproto.Nil,
		Channel:   "master",
		To:        sgproto.MaxOffset,
	}
	err := brokers[0].FetchRangeFn(ctx, req, func(msg *sgproto.Message) error {
		require.Equal(t, "my_key", string(msg.Key))
		count++
		return nil
	})
	require.Nil(t, err)

	require.Equal(t, 1, count)

	msg, err := brokers[0].Get(ctx, &sgproto.GetRequest{
		Topic:     "payments",
		Partition: part,
		Channel:   "master",
		Key:       []byte("my_key"),
	})
	require.NoError(t, err)
	require.Equal(t, "999", string(msg.Value))
}

func TestACK(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	topic := createTopic(t, brokers, createTopicParams)

	b := brokers[2]

	date := time.Now()
	offset := sgproto.NewOffset(1, date)
	ack(t, b, topic.Name, topic.Partitions[0].Id, "master", "group1", offset)

	syncAndAdvance(t, brokers)

	got := lastOffset(t, b, topic.Name, topic.Partitions[0].Id, "master", "group1",
		sgproto.MarkKind_Commited)
	require.Equal(t, sgproto.Nil, got)

	got = lastOffset(t, b, topic.Name, topic.Partitions[0].Id, "master", "group1",
		sgproto.MarkKind_Acknowledged)
	require.Equal(t, offset, got)

	offset2 := sgproto.NewOffset(2, date)
	commit(t, b, topic.Name, topic.Partitions[0].Id, "master", "group1", offset2)

	syncAndAdvance(t, brokers)

	got = lastOffset(t, b, topic.Name, topic.Partitions[0].Id, "master", "group1",
		sgproto.MarkKind_Commited)
	require.Equal(t, offset2, got)

	got = lastOffset(t, b, topic.Name, topic.Partitions[0].Id, "master", "group1",
		sgproto.MarkKind_Acknowledged)
	require.Equal(t, offset, got)
}

func createTopic(t *testing.T, brokers []*broker.Broker, createTopicParams *sgproto.TopicConfig) *topic.Topic {
	_, err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(t, err)

	_, err = brokers[0].CreateTopic(ctx, createTopicParams)
	require.NotNil(t, err)

	n := len(brokers)
	require.Len(t, brokers[0].Members(), n)
	for i := 0; i < n; i++ {
		require.Len(t, brokers[i].Topics(), 2)
	}

	var topic *topic.Topic
	for _, t := range brokers[0].Topics() {
		if t.Name == createTopicParams.Name {
			topic = t
		}
	}

	require.NotNil(t, topic)
	return topic
}

func TestConsume(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	topic := createTopic(t, brokers, createTopicParams)

	b := brokers[2]

	var want sgproto.Offset
	for i := 0; i < 30; i++ {
		res, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: topic.Partitions[0].Id,
			Messages: []*sgproto.Message{
				{
					Channel: "master",
					Key:     []byte("my_key"),
					Value:   []byte(strconv.Itoa(i)),
				},
			},
		})
		require.Nil(t, err)
		want = res.Offsets[0]
	}

	syncAndAdvance(t, brokers)

	fmt.Println("-----------------------------")
	var count int
	var got sgproto.Offset
	err := b.Consume(ctx, &sgproto.ConsumeFromGroupRequest{
		Topic:             "payments",
		Partition:         topic.Partitions[0].Id,
		Channel:           "master",
		ConsumerGroupName: "group1",
		ConsumerName:      "cons1",
	}, func(msg *sgproto.Message) error {
		count++
		ack(t, b, topic.Name, topic.Partitions[0].Id, "master", "group1", msg.Offset)
		got = msg.Offset
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, 30, count)
	require.Equal(t, want, got)

	syncAndAdvance(t, brokers)
	for i := 0; i < 20; i++ {
		res, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: topic.Partitions[0].Id,
			Messages: []*sgproto.Message{
				{
					Value:   []byte(strconv.Itoa(i)),
					Channel: "master",
				},
			},
		})
		require.Nil(t, err)
		want = res.Offsets[len(res.Offsets)-1]
	}

	syncAndAdvance(t, brokers)

	fmt.Println("-----------------------------")
	count = 0
	err = b.Consume(ctx, &sgproto.ConsumeFromGroupRequest{
		Topic:             "payments",
		Partition:         topic.Partitions[0].Id,
		Channel:           "master",
		ConsumerGroupName: "group1",
		ConsumerName:      "cons1",
	}, func(msg *sgproto.Message) error {
		count++
		got = msg.Offset
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, 20, count)
	require.Equal(t, want, got)

	syncAndAdvance(t, brokers)

	count = 0
	err = b.Consume(ctx, &sgproto.ConsumeFromGroupRequest{
		Topic:             "payments",
		Partition:         topic.Partitions[0].Id,
		Channel:           "master",
		ConsumerGroupName: "group1",
		ConsumerName:      "cons1",
	}, func(msg *sgproto.Message) error {
		count++
		return nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, count)

	syncAndAdvance(t, brokers)

	broker.RedeliveryTimeout = 100 * time.Millisecond // this should trigger redelivery
	broker.MaxRedeliveryCount = 3
	for i := 0; i < broker.MaxRedeliveryCount; i++ {

		time.Sleep(150 * time.Millisecond)

		count = 0
		err = b.Consume(ctx, &sgproto.ConsumeFromGroupRequest{
			Topic:             "payments",
			Partition:         topic.Partitions[0].Id,
			Channel:           "master",
			ConsumerGroupName: "group1",
			ConsumerName:      "cons1",
		}, func(msg *sgproto.Message) error {
			count++
			return nil
		})
		require.Nil(t, err)
		require.Equal(t, 20, count)

		syncAndAdvance(t, brokers)
	}

	count = 0
	err = b.Consume(ctx, &sgproto.ConsumeFromGroupRequest{
		Topic:             "payments",
		Partition:         topic.Partitions[0].Id,
		Channel:           "master",
		ConsumerGroupName: "group1",
		ConsumerName:      "cons1",
	}, func(msg *sgproto.Message) error {
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

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	createTopic(t, brokers, createTopicParams)

	topic := getTopicFromBroker(brokers[0], createTopicParams.Name)

	part := topic.Partitions[0]

	var lastPublishedID sgproto.Offset
	for i := 0; i < 5; i++ {
		res, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
			Topic:     "payments",
			Partition: part.Id,
			Messages: []*sgproto.Message{
				{
					Offset:  lastPublishedID,
					Value:   []byte(strconv.Itoa(i)),
					Channel: "master",
				},
			},
		})
		require.Nil(t, err)
		lastPublishedID = res.Offsets[0]
	}

	for _, b := range brokers {
		err := b.TriggerSyncRequest()
		require.NoError(t, err)
	}

	lastOffsets := make(map[string]sgproto.Offset)
	for _, b := range brokers {
		if sgutils.StringSliceHasString(part.Replicas, b.Name()) {
			tt := getTopicFromBroker(b, createTopicParams.Name)
			p := tt.GetPartition(part.Id)
			lastMsg, err := p.EndOfLog()
			require.NoError(t, err)
			var lastOffset sgproto.Offset
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

func TestChannels(t *testing.T) {
	n := 3
	brokers, destroyFn := makeNBrokers(t, n)
	defer destroyFn()

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	topic := createTopic(t, brokers, createTopicParams)

	b := brokers[2]

	//

	produce := func(channel string, n int) sgproto.Offset {
		var want sgproto.Offset
		for i := 0; i < n; i++ {
			res, err := brokers[0].Produce(ctx, &sgproto.ProduceMessageRequest{
				Topic:     "payments",
				Partition: topic.Partitions[0].Id,
				Messages: []*sgproto.Message{
					{
						Channel: channel,
						Key:     []byte("my_key"),
						Value:   []byte(strconv.Itoa(i)),
					},
				},
			})
			require.Nil(t, err)
			want = res.Offsets[0]
		}
		syncAndAdvance(t, brokers)
		return want
	}

	consume := func(channel, group string) (int, sgproto.Offset) {
		var count int
		var got sgproto.Offset
		err := b.Consume(ctx, &sgproto.ConsumeFromGroupRequest{
			Topic:             "payments",
			Partition:         topic.Partitions[0].Id,
			Channel:           channel,
			ConsumerGroupName: group,
			ConsumerName:      "cons1",
		}, func(msg *sgproto.Message) error {
			count++
			ack(t, b, topic.Name, topic.Partitions[0].Id, channel, group, msg.Offset)
			got = msg.Offset
			return nil
		})
		require.Nil(t, err)
		syncAndAdvance(t, brokers)
		return count, got
	}

	//
	{
		want := produce("master", 30)

		count, got := consume("master", "group1")
		require.Equal(t, 30, count)
		require.Equal(t, want, got)
	}

	//

	{
		want := produce("chan1", 15)

		{
			count, got := consume("chan1", "group1")
			require.Equal(t, 15, count)
			require.Equal(t, want, got)
		}
		{
			count, _ := consume("chan1", "group1")
			require.Equal(t, 0, count)
		}

		{
			count, got := consume("chan1", "group2")
			require.Equal(t, 15, count)
			require.Equal(t, want, got)
		}
		{
			count, _ := consume("chan1", "group2")
			require.Equal(t, 0, count)
		}
	}

	//

	{
		want := produce("chan2", 15)

		{
			count, got := consume("chan2", "group1")
			require.Equal(t, 15, count)
			require.Equal(t, want, got)
		}
		{
			count, _ := consume("chan2", "group1")
			require.Equal(t, 0, count)
		}
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

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_KVKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
	}
	_, err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(b, err)

	_, err = brokers[0].CreateTopic(ctx, createTopicParams)
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
			msg, err := brokers[0].Get(ctx, &sgproto.GetRequest{
				Topic:     "payments",
				Partition: "",
				Key:       []byte("my_key"),
			})
			require.NoError(b, err)
			require.Equal(b, "29", string(msg.Value))
		}
	})
}

func BenchmarkConsume(b *testing.B) {
	n := 3
	brokers, destroyFn := makeNBrokers(b, n)
	defer destroyFn()

	createTopicParams := &sgproto.TopicConfig{
		Name:              "payments",
		Kind:              sgproto.TopicKind_TimerKind,
		ReplicationFactor: 2,
		NumPartitions:     3,
		StorageDriver:     sgproto.StorageDriver_Badger,
	}
	_, err := brokers[0].CreateTopic(ctx, createTopicParams)
	require.Nil(b, err)

	_, err = brokers[0].CreateTopic(ctx, createTopicParams)
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
			err := brokers[0].Consume(context.Background(), &sgproto.ConsumeFromGroupRequest{
				Topic:             payments.Name,
				Partition:         payments.Partitions[0].Id,
				ConsumerGroupName: gen.Next().String(),
				ConsumerName:      "consumerName",
			}, func(msg *sgproto.Message) error {
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
		time.Sleep(200 * time.Millisecond)
		for _, p := range paths {
			os.RemoveAll(p)
		}
	}
	return
}

var logger = log.New(os.Stdout, "", log.LstdFlags)

func newBroker(tb testing.TB, i int, dc, bind_addr, adv_addr, gossip_port, grpc_port, http_port, raft_port, basepath string) *broker.Broker {
	lvl := logrus.InfoLevel
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
		BootstrapRaft:           true,
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

func ack(t *testing.T, b *broker.Broker, topic, partition, channel, group string, offset sgproto.Offset) {
	resp, err := b.Acknowledge(ctx, &sgproto.MarkRequest{
		Topic:         topic,
		Partition:     partition,
		Channel:       channel,
		ConsumerGroup: group,
		Offsets:       []sgproto.Offset{offset},
	})
	require.Nil(t, err)
	require.True(t, resp.Success)
}

func commit(t *testing.T, b *broker.Broker, topic, partition, channel, group string, offset sgproto.Offset) {
	resp, err := b.Commit(ctx, &sgproto.MarkRequest{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: group,
		Channel:       channel,
		Offsets:       []sgproto.Offset{offset},
	})
	require.Nil(t, err)
	require.True(t, resp.Success)
}

func lastOffset(t *testing.T, b *broker.Broker, topic, partition, channel, group string, kind sgproto.MarkKind) sgproto.Offset {
	resp, err := b.LastOffset(ctx, &sgproto.LastOffsetRequest{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: group,
		Channel:       channel,
		Kind:          kind,
	})
	require.Nil(t, err)
	return resp.Offset
}
