package scommons

import (
	"bytes"
	"encoding/binary"

	"github.com/gogo/protobuf/proto"

	"github.com/sandglass/sandglass-grpc/go/sgproto"
	"github.com/sandglass/sandglass/storage"
)

var (
	PendingPrefix = []byte{1, 'p'}
	ViewPrefix    = []byte{1, 'v'}
	WalPrefix     = []byte{1, 'w'}
)

type StorageCommons struct {
	storage.Storage
}

func (s *StorageCommons) LastKeyForPrefix(prefix []byte) []byte {
	it := s.Iter(&storage.IterOptions{
		Reverse:     true,
		FetchValues: false,
	})
	defer it.Close()

	if len(prefix) == 0 {
		it.Rewind()
	} else {
		it.Seek(append(prefix, 0xFF, 0xFF, 0xFF))
	}

	if !it.ValidForPrefix(prefix) {
		return nil
	}

	return it.Item().Key
}

func (s *StorageCommons) LastKVForPrefix(prefix, suffix []byte) []byte {
	it := s.Iter(&storage.IterOptions{
		Reverse:     true,
		FetchValues: true,
	})
	defer it.Close()

	// FIXME: use maxid instead of '~'
	if len(prefix) == 0 {
		it.Rewind()
	} else {
		it.Seek(append(prefix, bytes.Repeat([]byte{0xFF}, 3)...))
	}

	for ; it.ValidForPrefix(prefix); it.Next() {
		if suffix == nil || bytes.HasSuffix(it.Item().Key, suffix) {
			return it.Item().Value
		}
	}

	return nil
}

func (s *StorageCommons) ForEach(prefix []byte, fn func(msg *sgproto.Message) error) error {
	return s.ForRange(prefix, sgproto.Nil, sgproto.MaxOffset, fn)
}

func (s *StorageCommons) ForRange(prefix []byte, min, max sgproto.Offset, fn func(msg *sgproto.Message) error) error {
	it := NewMessageIterator(prefix, s, &storage.IterOptions{
		Reverse:     false,
		FetchValues: true,
	})
	defer it.Close()

	var m *sgproto.Message
	if min == sgproto.Nil {
		m = it.Rewind()
	} else {
		m = it.Seek(min)
	}

	for ; it.Valid(); m = it.Next() {
		if m.Offset.After(max) {
			break
		}

		if err := fn(m); err != nil {
			return err
		}
	}

	return nil
}

func (s *StorageCommons) ForEachWALEntry(prefix []byte, min []byte, fn func(msg *sgproto.Message) error) error {
	it := s.Iter(&storage.IterOptions{
		FetchValues: true,
	})
	defer it.Close()

	if len(min) == 0 {
		it.Seek(prefix)
	} else {
		it.Seek(min)
		if it.ValidForPrefix(prefix) && bytes.Compare(min, it.Item().Key) == 0 { // skipping first since it is already in the replica
			it.Next()
		}
	}

	for ; it.ValidForPrefix(prefix); it.Next() {
		value := it.Item().Value

		var msg sgproto.Message
		if err := proto.Unmarshal(value, &msg); err != nil {
			return err
		}

		if err := fn(&msg); err != nil {
			return err
		}
	}

	return nil
}

func (s *StorageCommons) ForRangeWAL(prefix []byte, min, max uint64, fn func(msg *sgproto.Message) error) error {
	it := s.Iter(&storage.IterOptions{
		FetchValues: true,
	})
	defer it.Close()

	var (
		minKey, maxKey []byte
	)
	if min == 0 {
		it.Seek(prefix)
	} else {
		minKey, maxKey = make([]byte, 8), make([]byte, 8)
		binary.BigEndian.PutUint64(minKey[:], min)
		binary.BigEndian.PutUint64(maxKey[:], max)
		minKey = Join(prefix, minKey)
		maxKey = Join(prefix, maxKey)
		it.Seek(minKey)
		if it.ValidForPrefix(prefix) && bytes.Compare(minKey, it.Item().Key) == 0 { // skipping first since it is already in the replica
			it.Next()
		}
	}

	for ; it.ValidForPrefix(prefix); it.Next() {
		if maxKey != nil && bytes.Compare(it.Item().Key, maxKey) > 0 {
			break
		}

		value := it.Item().Value
		var msg sgproto.Message
		if err := proto.Unmarshal(value, &msg); err != nil {
			return err
		}

		if err := fn(&msg); err != nil {
			return err
		}
	}

	return nil
}

func Join(keys ...[]byte) []byte {
	return bytes.Join(keys, storage.Separator)
}
