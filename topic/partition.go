package topic

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/tylertreat/BoomFilters"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
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

func (t *Partition) getStorageKey(offset sandflake.ID, key []byte) []byte {
	var storekey []byte
	switch t.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		storekey = offset[:]
	case sgproto.TopicKind_CompactedKind:
		storekey = key
	default:
		panic("INVALID STORAGE KIND: " + t.topic.Kind.String())
	}
	return scommons.PrependPrefix(scommons.MsgPrefix, storekey)
}

func (s *Partition) GetMessage(offset sandflake.ID, k, suffix []byte) (*sgproto.Message, error) {
	switch s.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		val, err := s.db.Get(scommons.PrependPrefix(scommons.MsgPrefix, offset[:]))
		if err != nil {
			return nil, err
		}

		var msg sgproto.Message
		err = proto.Unmarshal(val, &msg)
		if err != nil {
			return nil, err
		}

		return &msg, nil
	case sgproto.TopicKind_CompactedKind:
		val := s.db.LastKVForPrefix(scommons.PrependPrefix(scommons.MsgPrefix, k), suffix)
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

func (t *Partition) PutMessage(msg *sgproto.Message) error {
	storagekey := t.getStorageKey(msg.Offset, msg.Key)
	if msg.Index == sandflake.Nil {
		msg.Index = t.NextID()
	}

	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	entries := []*storage.Entry{
		{Key: storagekey, Value: val},                                 // msg
		{Key: t.newWALKey(msg.Index, storagekey), Value: []byte("X")}, // wal
	}

	err = t.db.BatchPut(entries)
	if err != nil {
		return err
	}

	t.bf.Add(msg.Key)
	t.ibf.Add(msg.Key)

	return nil
}

func (t *Partition) HasKey(key []byte) (bool, error) {
	switch t.topic.Kind {
	case sgproto.TopicKind_CompactedKind:
	default:
		panic("not compacted topic")
	}

	if t.ibf.Test(key) {
		return true, nil
	}

	if !t.bf.Test(key) {
		return false, nil
	}

	msg, err := t.GetMessage(sandflake.Nil, key, nil)
	if err != nil {
		return false, err
	}

	if msg == nil {
		return false, nil
	}

	return msg.Offset != sandflake.Nil, nil
}

func (t *Partition) newWALKey(index sandflake.ID, key []byte) []byte {
	return bytes.Join([][]byte{scommons.WalPrefix, index.Bytes(), key}, []byte("/"))
}

func (t *Partition) BatchPutMessages(msgs []*sgproto.Message) error {
	entries := make([]*storage.Entry, len(msgs))
	for i, msg := range msgs {
		if msg.Offset == sandflake.Nil {
			return ErrNoKeySet
		}
		if msg.Index == sandflake.Nil {
			msg.Index = t.NextID()
		}
		storagekey := t.getStorageKey(msg.Offset, msg.Key)
		val, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		t.bf.Add(msg.Key)
		t.ibf.Add(msg.Key)
		entries[i] = &storage.Entry{
			Key:   storagekey,
			Value: val,
		}
	}

	if len(entries) == 0 {
		return nil
	}

	err := t.db.BatchPut(entries)
	if err != nil {
		return err
	}

	for i, e := range entries {
		e.Key = t.newWALKey(msgs[i].Index, e.Key)
		e.Value = nil
	}

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
	case sgproto.TopicKind_CompactedKind:
		it := scommons.NewMessageIterator(p.db, &storage.IterOptions{
			FetchValues: true,
			Reverse:     true,
		})
		defer it.Close()
		for m := it.Rewind(); it.Valid(); m = it.Next() {
			if lastKey == nil || bytes.Compare(m.Key, lastKey) != 0 {
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
	key := p.db.LastKeyForPrefix(scommons.WalPrefix)
	if key == nil {
		return nil, nil
	}

	if len(key) == 0 {
		panic("empty wal key")
	}

	key = key[len(scommons.WalPrefix)+1+sandflake.Size+1:]
	return p.getMessageByStorageKey(key)
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
