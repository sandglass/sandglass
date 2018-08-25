package badger

import (
	"github.com/sandglass/sandglass/sgutils"
	"github.com/sandglass/sandglass/storage"
	"github.com/dgraph-io/badger"
)

type iterator struct {
	iter *badger.Iterator
	txn  *badger.Txn
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
	v, err := item.Value()
	if err != nil {
		panic(err)
	}

	val := sgutils.CopyBytes(v)
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
	i.txn.Discard()
	return nil
}

var _ storage.Iterator = (*iterator)(nil)
