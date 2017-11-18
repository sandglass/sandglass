package topic

import (
	"sync"
	"testing"

	"io/ioutil"

	"os"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/celrenheit/sandglass/storage/scommons"
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
	id := p.idgen.Next()
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
	id := p.idgen.Next()
	err = p.PutMessage(&sgproto.Message{
		Offset: id,
		Key:    key,
		Value:  value,
	})
	require.Nil(t, err)

	gotMsg, err := p.GetMessage(sandflake.Nil, key, nil)
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
	id := p.idgen.Next()
	err = p.PutMessage(&sgproto.Message{
		Offset: id,
		Key:    key,
		Value:  value,
	})
	require.Nil(t, err)

	gotKey := p.LastWALEntry()
	require.Nil(t, err)
	require.NotNil(t, gotKey)
	require.Equal(t, string(key), string(gotKey[len(scommons.WalPrefix)+1+sandflake.Size+1+len(scommons.ViewPrefix)+1:]))

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

			var firstId sandflake.ID
			var once sync.Once
			b.Run("put", func(b *testing.B) {
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						id := p.idgen.Next()
						once.Do(func() {
							firstId = id
						})
						err := p.PutMessage(&sgproto.Message{
							Offset: id,
							Key:    key,
							Value:  value,
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
						err := p.ForRange(sandflake.Nil, sandflake.MaxID, func(msg *sgproto.Message) error {
							return nil
						})
						require.Nil(b, err)
					}
				})
			})
		})
	}
}
