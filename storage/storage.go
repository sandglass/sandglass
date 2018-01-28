package storage

import (
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
)

var Separator = []byte{'/'}

type Storage interface {
	Get(key []byte) ([]byte, error)
	Put(key, val []byte) error
	BatchPut(entries []*Entry) error
	Merge(key, operation []byte) error
	ProcessMergedKey(key []byte, fn func(val []byte) ([]*Entry, []byte, error)) error
	Iter(*IterOptions) Iterator
	Close() error
	LastKeyForPrefix(prefix []byte) []byte
	LastKVForPrefix(prefix, suffix []byte) []byte
	ForEach(fn func(msg *sgproto.Message) error) error
	ForRange(min, max sgproto.Offset, fn func(msg *sgproto.Message) error) error
	ForEachWALEntry(min []byte, fn func(msg *sgproto.Message) error) error
	ForRangeWAL(min, max uint64, fn func(msg *sgproto.Message) error) error
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

type MergeOperator struct {
	Key       []byte
	MergeFunc func(existing, value []byte) ([]byte, bool)
}
