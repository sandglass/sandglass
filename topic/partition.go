package topic

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/tylertreat/BoomFilters"

	"github.com/celrenheit/sandflake"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/sgutils"
	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/storage/badger"
	"github.com/celrenheit/sandglass/storage/rocksdb"
	"github.com/celrenheit/sandglass/storage/scommons"
	"github.com/gogo/protobuf/proto"
	"github.com/willf/bloom"
)

var (
	ErrNoKeySet = errors.New("ErrNoKeySet")
)

type Partition struct {
	db       storage.Storage
	Id       string
	Replicas []string
	idgen    sandflake.Generator
	basepath string
	topic    *Topic

	bf  *bloom.BloomFilter // TODO: persist bloom filter
	ibf boom.Filter
}

func (t *Partition) InitStore(basePath string) error {
	t.bf = bloom.NewWithEstimates(1e3, 1e-2)
	t.ibf = boom.NewInverseBloomFilter(1e3)
	msgdir := filepath.Join(basePath, t.Id)
	if err := sgutils.MkdirIfNotExist(msgdir); err != nil {
		return err
	}

	var err error
	switch t.topic.StorageDriver {
	case sgproto.StorageDriver_Badger:
		t.db, err = badger.NewStorage(msgdir)
	case sgproto.StorageDriver_RocksDB:
		t.db, err = rocksdb.NewStorage(msgdir)
	default:
		panic(fmt.Sprintf("unknown storage driver: %v for topic: %v", t.topic.StorageDriver, t.topic.Name))
	}
	if err != nil {
		return err
	}

	t.basepath = basePath
	return nil
}

func (t *Partition) String() string {
	return t.Id
}

func (t *Partition) getStorageKey(msg *sgproto.Message) []byte {
	var storekey []byte
	switch t.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		storekey = msg.Offset[:]
	case sgproto.TopicKind_KVKind:
		storekey = msg.Key
		if len(msg.ClusteringKey) > 0 {
			storekey = joinKeys(msg.Key, msg.ClusteringKey)
		}
	default:
		panic("INVALID STORAGE KIND: " + t.topic.Kind.String())
	}
	return scommons.PrependPrefix(scommons.ViewPrefix, storekey)
}

func (s *Partition) GetMessage(offset sandflake.ID, k, suffix []byte) (*sgproto.Message, error) {
	switch s.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		val, err := s.db.Get(scommons.PrependPrefix(scommons.ViewPrefix, offset[:]))
		if err != nil {
			return nil, err
		}

		var msg sgproto.Message
		err = proto.Unmarshal(val, &msg)
		if err != nil {
			return nil, err
		}

		return &msg, nil
	case sgproto.TopicKind_KVKind:
		val := s.db.LastKVForPrefix(scommons.PrependPrefix(scommons.ViewPrefix, k), suffix)
		if val == nil {
			return nil, nil
		}

		var msg sgproto.Message
		err := proto.Unmarshal(val, &msg)
		if err != nil {
			return nil, err
		}

		return &msg, nil
	default:
		panic("INVALID STORAGE KIND: " + s.topic.Kind.String())
	}
}

func (t *Partition) HasKey(key, clusterKey []byte) (bool, error) {
	switch t.topic.Kind {
	case sgproto.TopicKind_KVKind:
	default:
		panic("not kv topic")
	}

	pk := joinKeys(key, clusterKey)
	existKey := scommons.PrependPrefix(scommons.ViewPrefix, pk)

	if t.ibf.Test(existKey) {
		return true, nil
	}

	if !t.bf.Test(existKey) {
		return false, nil
	}

	msg, err := t.GetMessage(sandflake.Nil, pk, nil)
	if err != nil {
		return false, err
	}

	if msg == nil {
		return false, nil
	}

	return msg.Offset != sandflake.Nil, nil
}

func (t *Partition) newWALKey(index sandflake.ID, key []byte) []byte {
	return bytes.Join([][]byte{scommons.WalPrefix, index.Bytes(), key}, storage.Separator)
}

func (t *Partition) PutMessage(msg *sgproto.Message) error {
	return t.BatchPutMessages([]*sgproto.Message{msg})
}

func (t *Partition) BatchPutMessages(msgs []*sgproto.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	dataEntries := make([]*storage.Entry, len(msgs))
	walEntries := make([]*storage.Entry, len(msgs))
	for i, msg := range msgs {
		if msg.Offset == sandflake.Nil {
			return ErrNoKeySet
		}
		if msg.Index == sandflake.Nil {
			msg.Index = t.NextID()
		}
		storagekey := t.getStorageKey(msg)
		val, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		t.bf.Add(storagekey)
		t.ibf.Add(storagekey)
		dataEntries[i] = &storage.Entry{
			Key:   storagekey,
			Value: val,
		}
		walEntries[i] = &storage.Entry{
			Key:   t.newWALKey(msg.Index, storagekey),
			Value: val,
		}
	}

	entries := make([]*storage.Entry, 2*len(msgs))
	copy(entries[:len(msgs)], dataEntries)
	copy(entries[len(msgs):], walEntries)

	return t.db.BatchPut(entries)
}

func (t *Partition) NextID() sandflake.ID {
	return t.idgen.Next()
}

func (p *Partition) ForRange(min, max sandflake.ID, fn func(msg *sgproto.Message) error) error {
	var lastKey []byte
	switch p.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		return p.db.ForRange(min, max, func(msg *sgproto.Message) error {
			err := fn(msg)
			return err
		})
	case sgproto.TopicKind_KVKind:
		it := scommons.NewMessageIterator(p.db, &storage.IterOptions{
			FetchValues: true,
			Reverse:     true,
		})
		defer it.Close()
		for m := it.Rewind(); it.Valid(); m = it.Next() {
			if lastKey == nil || !bytes.Equal(m.Key, lastKey) {
				err := fn(m)
				if err != nil {
					return err
				}
				lastKey = m.Key
			}
		}
	default:
		panic("unknown topic kind: " + p.topic.Kind.String())
	}
	return nil
}

func (p *Partition) Close() error {
	return p.db.Close()
}

func (p *Partition) Iter() storage.MessageIterator {
	return scommons.NewMessageIterator(p.db, &storage.IterOptions{
		FetchValues: true,
		Reverse:     false,
	})
}

func (p *Partition) RangeFromWAL(min []byte, fn func(*sgproto.Message) error) error {
	return p.db.ForEachKey(min, func(k []byte) error {
		if len(k) == 0 {
			panic("empty wal key")
		}

		k = k[len(scommons.WalPrefix)+1+sandflake.Size+1:]
		msg, err := p.getMessageByStorageKey(k)
		if err != nil {
			return err
		}

		return fn(msg)
	})
}

func (p *Partition) LastWALEntry() []byte {
	return p.db.LastKeyForPrefix(scommons.WalPrefix)
}

func (p *Partition) LastMessage() (*sgproto.Message, error) {
	value := p.db.LastKVForPrefix(scommons.WalPrefix, nil)
	if len(value) == 0 {
		return nil, nil
	}

	var msg sgproto.Message
	if err := proto.Unmarshal(value, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}

func (p *Partition) getMessageByStorageKey(k []byte) (*sgproto.Message, error) {
	b, err := p.db.Get(k)
	if err != nil {
		return nil, err
	}

	if b == nil {
		panic("should not happend, key in wal should always be also in msgs")
	}

	var msg sgproto.Message
	err = proto.Unmarshal(b, &msg)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

func joinKeys(key, clusterKey []byte) []byte {
	return bytes.Join([][]byte{
		key,
		clusterKey,
	}, storage.Separator)
}
