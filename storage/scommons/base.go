package scommons

import (
	"bytes"

	"github.com/gogo/protobuf/proto"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
)

var (
	ViewPrefix = []byte("v")
	WalPrefix  = []byte("w")
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

func (s *StorageCommons) ForEach(fn func(msg *sgproto.Message) error) error {
	return s.ForRange(sandflake.Nil, sandflake.MaxID, fn)
}

func (s *StorageCommons) ForRange(min, max sandflake.ID, fn func(msg *sgproto.Message) error) error {
	it := NewMessageIterator(s, &storage.IterOptions{
		Reverse:     false,
		FetchValues: true,
	})
	defer it.Close()

	var m *sgproto.Message
	if min == sandflake.Nil {
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

func (s *StorageCommons) ForEachWALEntry(min []byte, fn func(msg *sgproto.Message) error) error {
	it := s.Iter(&storage.IterOptions{
		FetchValues: true,
	})
	defer it.Close()

	if len(min) == 0 {
		it.Seek(WalPrefix)
	} else {
		it.Seek(min)
		if it.ValidForPrefix(WalPrefix) && bytes.Compare(min, it.Item().Key) == 0 { // skipping first since it is already in the replica
			it.Next()
		}
	}

	for ; it.ValidForPrefix(WalPrefix); it.Next() {
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

func PrependPrefix(prefix, key []byte) []byte {
	return bytes.Join([][]byte{prefix, key}, storage.Separator)
}
