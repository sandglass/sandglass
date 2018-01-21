package topic

import (
	"sync"
	"testing"
	"time"

	"io/ioutil"

	"os"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/stretchr/testify/require"
)

func TestTimerStorage(t *testing.T) {
	p := &Partition{
		Id: "test",
		topic: &Topic{
			Kind: sgproto.TopicKind_TimerKind,
		},
	}
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	err = p.InitStore(dir)
	require.Nil(t, err)

	key := []byte("batman")
	value := []byte("value")
	id := sgproto.NewOffset(1, time.Unix(0, 0))
	err = p.PutMessage(&sgproto.Message{
		Offset: id,
		Key:    key,
		Value:  value,
	})
	require.Nil(t, err)

	gotMsg, err := p.GetMessage(id, nil, nil)
	require.Nil(t, err)
	require.Equal(t, id, gotMsg.Offset)
	require.Equal(t, string(value), string(gotMsg.Value))
}

func TestKVStorage(t *testing.T) {
	p := &Partition{
		Id: "test",
		topic: &Topic{
			Kind: sgproto.TopicKind_KVKind,
		},
	}
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	err = p.InitStore(dir)
	require.Nil(t, err)

	key := []byte("batman")
	value := []byte("value")
	id := sgproto.NewOffset(1, time.Unix(0, 0))
	err = p.PutMessage(&sgproto.Message{
		Offset: id,
		Key:    key,
		Value:  value,
	})
	require.Nil(t, err)

	gotMsg, err := p.GetMessage(sgproto.Nil, key, nil)
	require.Nil(t, err)
	require.NotNil(t, gotMsg)
	require.Equal(t, string(key), string(gotMsg.Key))
	require.Equal(t, string(value), string(gotMsg.Value))
}

func TestLastMessage(t *testing.T) {
	p := &Partition{
		Id: "test",
		topic: &Topic{
			Kind: sgproto.TopicKind_KVKind,
		},
	}
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	err = p.InitStore(dir)
	require.Nil(t, err)

	key := []byte("batman")
	value := []byte("value")
	id := sgproto.NewOffset(1, time.Unix(0, 0))
	err = p.PutMessage(&sgproto.Message{
		Offset: id,
		Key:    key,
		Value:  value,
	})
	require.Nil(t, err)

	gotKey := p.LastWALEntry()
	require.Nil(t, err)
	require.NotNil(t, gotKey)

	gotMsg, err := p.LastMessage()
	require.Nil(t, err)
	require.NotNil(t, gotMsg)
	require.Equal(t, string(key), string(gotMsg.Key))
	require.Equal(t, string(value), string(gotMsg.Value))
}

func BenchmarkStorageDrivers(b *testing.B) {
	for stDriver, stDriverName := range sgproto.StorageDriver_name {
		b.Run(stDriverName, func(b *testing.B) {
			p := &Partition{
				Id: "test",
				topic: &Topic{
					Kind:          sgproto.TopicKind_TimerKind,
					StorageDriver: sgproto.StorageDriver(stDriver),
				},
			}
			dir, err := ioutil.TempDir("", "")
			require.Nil(b, err)
			defer os.RemoveAll(dir)

			err = p.InitStore(dir)
			require.Nil(b, err)
			// defer p.Close()

			key := []byte("batman")
			value := []byte("value")

			var firstId sgproto.Offset
			var once sync.Once
			b.Run("put", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						once.Do(func() {
							firstId = sgproto.NewOffset(p.NextIndex(), time.Unix(0, 0))
						})
						err := p.PutMessage(&sgproto.Message{
							Key:   key,
							Value: value,
						})
						require.Nil(b, err)
					}
				})
			})

			b.Run("get", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						msg, err := p.GetMessage(firstId, nil, nil)
						require.Nil(b, err)
						require.NotNil(b, msg)
					}
				})
			})

			b.Run("range", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						err := p.ForRange(sgproto.Nil, sgproto.MaxOffset, func(msg *sgproto.Message) error {
							return nil
						})
						require.Nil(b, err)
					}
				})
			})
		})
	}
}
