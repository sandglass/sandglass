package storage

import (
	"github.com/sandglass/sandglass-grpc/go/sgproto"
)

var Separator = []byte{'/'}

type Storage interface {
	Get(key []byte) ([]byte, error)
	Put(key, val []byte) error
	BatchPut(entries []*Entry) error
	Merge(key, operation []byte) error
	ProcessMergedKey(key []byte, fn func(val []byte) ([]*Entry, []byte, error)) error
	Iter(*IterOptions) Iterator
	Truncate(prefix, min []byte, batchSize int) error
	BatchDelete(keys [][]byte) error
	Delete(key []byte) error
	Close() error
	LastKeyForPrefix(prefix []byte) []byte
	LastKVForPrefix(prefix, suffix []byte) []byte
	ForEach(prefix []byte, fn func(msg *sgproto.Message) error) error
	ForRange(prefix []byte, min, max sgproto.Offset, fn func(msg *sgproto.Message) error) error
	ForEachWALEntry(prefix []byte, min []byte, fn func(msg *sgproto.Message) error) error
	ForRangeWAL(prefix []byte, min, max uint64, fn func(msg *sgproto.Message) error) error
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
