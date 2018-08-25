package topic

import (
	"math/rand"
	"path/filepath"
	"sync"

	"fmt"

	"github.com/sandglass/sandglass-grpc/go/sgproto"
	"github.com/sandglass/sandglass/sgutils"
	"github.com/sandglass/sandglass/storage"
	"github.com/sandglass/sandglass/storage/badger"
	"github.com/sandglass/sandglass/storage/rocksdb"
	"golang.org/x/sync/errgroup"
)

type Topic struct {
	Name              string
	Kind              sgproto.TopicKind
	ReplicationFactor int
	NumPartitions     int
	Partitions        []*Partition
	StorageDriver     sgproto.StorageDriver

	basepath string
	db       storage.Storage
}

func (t *Topic) Validate() error {
	if t.Name == "" { // TODO: check non ascii chars
		return fmt.Errorf("topic name should not be empty")
	}
	if t.ReplicationFactor < 1 {
		return fmt.Errorf("replication factor should not be > 0")
	}
	if t.NumPartitions < 1 {
		return fmt.Errorf("number of partitions should not be > 0")
	}

	return nil
}

func (t *Topic) InitStore(basePath string) error {
	msgdir := filepath.Join(basePath, t.Name)
	t.basepath = msgdir

	if err := sgutils.MkdirIfNotExist(t.basepath); err != nil {
		return err
	}

	if t.NumPartitions <= 0 {
		return fmt.Errorf("Num partitions should be > 0")
	}

	var err error
	switch t.StorageDriver {
	case sgproto.StorageDriver_Badger:
		t.db, err = badger.NewStorage(msgdir)
	case sgproto.StorageDriver_RocksDB:
		t.db, err = rocksdb.NewStorage(msgdir)
	default:
		return fmt.Errorf("unknown storage driver: %v for topic: %v", t.StorageDriver, t.Name)
	}
	if err != nil {
		return err
	}

	for _, p := range t.Partitions {
		err := t.initPartition(p)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Topic) initPartition(p *Partition) error {
	p.topic = t
	err := p.InitStore(t.db)
	if err != nil {
		return err
	}

	return nil
}

func (t *Topic) ListPartitions() []*Partition { return t.Partitions }

func (t *Topic) GetPartition(id string) *Partition {
	for _, p := range t.Partitions {
		if p.Id == id {
			return p
		}
	}

	return nil
}

func (t *Topic) ChoosePartitionForKey(key []byte) *Partition {
	var id []byte
	switch t.Kind {
	case sgproto.TopicKind_TimerKind:
		panic(fmt.Sprintf("not for timer kind %v %v", t.Name, t.Partitions))
	case sgproto.TopicKind_KVKind:
		id = key
	}

	idx := sgutils.Hash(id, len(t.Partitions))
	return t.Partitions[idx]
}

func (t *Topic) ChoosePartition(msg *sgproto.Message) *Partition {
	var id []byte
	switch t.Kind {
	case sgproto.TopicKind_TimerKind:
		id = msg.Offset.Bytes()
	case sgproto.TopicKind_KVKind:
		id = msg.Key
	}

	idx := sgutils.Hash(id, len(t.Partitions))
	return t.Partitions[idx]
}

func (t *Topic) ChooseRandomPartition() *Partition {
	idx := rand.Int() % len(t.Partitions)
	return t.Partitions[idx]
}

func (t *Topic) PutMessage(partition string, msg *sgproto.Message) error {
	var p *Partition
	if partition != "" {
		p = t.GetPartition(partition)
	} else {
		p = t.ChoosePartition(msg)
	}

	return p.PutMessage(msg)
}

func (t *Topic) BatchPutMessages(msgs []*sgproto.Message) error {
	msgsByPartitions := make([][]*sgproto.Message, len(t.Partitions))
	for _, m := range msgs {
		pidx := sgutils.HashString(m.Offset.String(), len(t.Partitions))
		msgsByPartitions[pidx] = append(msgsByPartitions[pidx], m)
	}

	var group errgroup.Group
	for pidx, msgs := range msgsByPartitions {
		p := t.Partitions[pidx]
		msgs := msgs
		group.Go(func() error {
			return p.BatchPutMessages(msgs)
		})
	}

	return group.Wait()
}

func (t *Topic) Close() error {
	var group errgroup.Group
	for _, p := range t.Partitions {
		group.Go(p.Close)
	}
	if err := group.Wait(); err != nil {
		return err
	}

	return t.db.Close()
}

func (t *Topic) ForEach(channel string, fn func(msg *sgproto.Message) error) error {
	return t.ForRange(channel, sgproto.Nil, sgproto.MaxOffset, fn)
}

func (t *Topic) ForRange(channel string, min, max sgproto.Offset, fn func(msg *sgproto.Message) error) error {
	// FIXME
	var mu sync.Mutex
	var group errgroup.Group
	for _, p := range t.Partitions {
		p := p
		group.Go(func() error {
			return p.ForRange(channel, min, max, func(msg *sgproto.Message) error {
				mu.Lock()
				err := fn(msg)
				mu.Unlock()
				return err
			})
		})
	}

	return group.Wait()
}
