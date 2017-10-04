package raft

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/celrenheit/sandglass/logy"
	"github.com/celrenheit/sandglass/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

var logger = logy.NewStdoutLogger(logy.DEBUG)

func TestRaft(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	tmpDir2, _ := ioutil.TempDir("", "store_test2")
	defer os.RemoveAll(tmpDir2)

	s := New(Config{
		Addr: "127.0.0.1:1234",
		Dir:  tmpDir,
		// StartAsLeader: true,
	}, logger)

	err := s.Init()
	require.NoError(t, err)

	s2 := New(Config{
		Addr: "127.0.0.1:12345",
		Dir:  tmpDir2,
	}, logger)

	err = s2.Init()
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	future := s.raft.AddVoter(raft.ServerID("127.0.0.1:12345"), raft.ServerAddress("127.0.0.1:12345"), 0, raftTimeout)
	require.NoError(t, future.Error())

	err = s.CreateTopic(&topic.Topic{
		Name: "hello",
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

	// time.Sleep(500 * time.Millisecond)

	// value, err = s.Get("foo")
	// require.NoError(t, err)
	// require.Equal(t, "", value)
}
