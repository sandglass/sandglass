package raft

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/sandglass/sandglass/topic"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
)

var logger = logrus.WithField("mode", "test")

func TestRaft(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	tmpDir2, _ := ioutil.TempDir("", "store_test2")
	defer os.RemoveAll(tmpDir2)

	s := New(Config{
		BindAddr: "127.0.0.1:1234",
		AdvAddr:  "127.0.0.1:1234",
		Dir:      tmpDir,
		// StartAsLeader: true,
	}, logger)

	err := s.Init(true, &serf.Serf{}, nil)
	require.NoError(t, err)

	s2 := New(Config{
		BindAddr: "127.0.0.1:12345",
		AdvAddr:  "127.0.0.1:12345",
		Dir:      tmpDir2,
	}, logger)

	err = s2.Init(false, &serf.Serf{}, nil)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	future := s.raft.AddVoter(raft.ServerID("127.0.0.1:12345"), raft.ServerAddress("127.0.0.1:12345"), 0, raftTimeout)
	require.NoError(t, future.Error())

	err = s.CreateTopic(&topic.Topic{
		Name:              "hello",
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	topic := s.GetTopic("hello")
	require.NotNil(t, topic)
	require.Equal(t, "hello", topic.Name)

	state := map[string]map[string]string{
		"hello": map[string]string{
			"part1": "127.0.0.1:1234",
		},
	}
	err = s.SetPartitionLeaderBulkOp(state)
	require.NoError(t, err)
}
