package storage

import (
	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
)

type Storage interface {
	Get(key []byte) ([]byte, error)
	Put(key, val []byte) error
	BatchPut(entries []*Entry) error
	Iter(*IterOptions) Iterator
	Close() error
	LastKeyForPrefix(prefix []byte) []byte
	LastKVForPrefix(prefix, suffix []byte) []byte
	ForEach(fn func(msg *sgproto.Message) error) error
	ForRange(min, max sandflake.ID, fn func(msg *sgproto.Message) error) error
	ForEachKey(min []byte, fn func(k []byte) error) error
}

type Entry struct {
	Key   []byte
	Value []byte
}

type IterOptions struct {
	Reverse     bool
	FetchValues bool
	FillCache   bool
}
