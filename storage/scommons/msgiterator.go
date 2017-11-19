package scommons

import (
	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
	"github.com/gogo/protobuf/proto"
)

type messageIter struct {
	iter storage.Iterator
	opts *storage.IterOptions
}

func NewMessageIterator(s storage.Storage, opts *storage.IterOptions) storage.MessageIterator {
	return &messageIter{s.Iter(opts), opts}
}

func (i *messageIter) Rewind() *sgproto.Message {
	i.iter.Rewind()
	if !i.opts.Reverse {
		i.iter.Seek(ViewPrefix)
	} else {
		i.iter.Seek(PrependPrefix(ViewPrefix, []byte{'~', '~'}))
	}

	if i.Valid() {
		return i.getCurrent()
	}

	return nil
}

func (i *messageIter) Seek(id sandflake.ID) *sgproto.Message {
	i.iter.Seek(PrependPrefix(ViewPrefix, id[:]))
	if i.Valid() {
		return i.getCurrent()
	}

	return nil
}

func (i *messageIter) Valid() bool {
	return i.iter.ValidForPrefix(ViewPrefix)
}

func (i *messageIter) ValidForPrefix(prefix []byte) bool {
	return i.iter.ValidForPrefix(PrependPrefix(ViewPrefix, prefix))
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
