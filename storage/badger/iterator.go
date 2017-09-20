package badger

import (
	"github.com/celrenheit/sandglass/sgutils"
	"github.com/celrenheit/sandglass/storage"
	"github.com/dgraph-io/badger"
)

type iterator struct {
	iter *badger.Iterator
}

func (i *iterator) Rewind() {
	i.iter.Rewind()
}

func (i *iterator) Seek(at []byte) {
	i.iter.Seek(at)
}

func (i *iterator) Valid() bool {
	return i.iter.Valid()
}

func (i *iterator) ValidForPrefix(prefix []byte) bool {
	return i.iter.ValidForPrefix(prefix)
}

func (i *iterator) Next() {
	i.iter.Next()
}

func (i *iterator) Item() *storage.Entry {
	item := i.iter.Item()
	if item == nil {
		return nil
	}

	key := sgutils.CopyBytes(item.Key())
	var val []byte
	err := item.Value(func(v []byte) error {
		val = sgutils.CopyBytes(v)
		return nil
	})
	if err != nil {
		panic(err)
	}

	return &storage.Entry{
		Key:   key,
		Value: val,
	}
}

func (i *iterator) Close() error {
	i.iter.Close()
	return nil
}

var _ storage.Iterator = (*iterator)(nil)
