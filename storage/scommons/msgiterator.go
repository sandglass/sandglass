package scommons

import (
	"github.com/sandglass/sandglass-grpc/go/sgproto"
	"github.com/sandglass/sandglass/storage"
	"github.com/gogo/protobuf/proto"
)

type messageIter struct {
	prefix []byte
	iter   storage.Iterator
	opts   *storage.IterOptions
}

func NewMessageIterator(prefix []byte, s storage.Storage, opts *storage.IterOptions) storage.MessageIterator {
	return &messageIter{prefix, s.Iter(opts), opts}
}

func (i *messageIter) Rewind() *sgproto.Message {
	i.iter.Rewind()
	if !i.opts.Reverse {
		i.iter.Seek(i.prefix)
	} else {
		i.iter.Seek(Join(i.prefix, []byte{'~', '~'}))
	}

	if i.Valid() {
		return i.getCurrent()
	}

	return nil
}

func (i *messageIter) Seek(id sgproto.Offset) *sgproto.Message {
	i.iter.Seek(Join(i.prefix, id[:]))
	if i.Valid() {
		return i.getCurrent()
	}

	return nil
}

func (i *messageIter) Valid() bool {
	return i.iter.ValidForPrefix(i.prefix)
}

func (i *messageIter) ValidForPrefix(prefix []byte) bool {
	return i.iter.ValidForPrefix(Join(i.prefix, prefix))
}

func (i *messageIter) Next() *sgproto.Message {
	i.iter.Next()
	return i.getCurrent()
}

func (i *messageIter) getCurrent() *sgproto.Message {
	item := i.iter.Item()
	if item == nil {
		return nil
	}

	var msg sgproto.Message
	err := proto.Unmarshal(item.Value, &msg)
	if err != nil {
		return nil
	}

	return &msg
}

func (i *messageIter) Close() error {
	return i.iter.Close()
}

var _ storage.MessageIterator = (*messageIter)(nil)
