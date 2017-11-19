package storage

import (
	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
)

type Iterator interface {
	Rewind()
	Seek([]byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
	Item() *Entry
	Next()
	Close() error
}

type MessageIterator interface {
	Rewind() *sgproto.Message
	Seek(sandflake.ID) *sgproto.Message
	Valid() bool
	Next() *sgproto.Message
	Close() error
}
