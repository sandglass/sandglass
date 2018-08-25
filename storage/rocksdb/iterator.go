// +build cgo

package rocksdb

import (
	"github.com/sandglass/sandglass/sgutils"
	"github.com/sandglass/sandglass/storage"
	"github.com/tecbot/gorocksdb"
)

type iterator struct {
	iter *gorocksdb.Iterator
	opts *storage.IterOptions
}

func (i *iterator) Rewind() {
	if i.opts.Reverse {
		i.iter.SeekToLast()
		return
	}
	i.iter.SeekToFirst()
}

func (i *iterator) Seek(at []byte) {
	if i.opts.Reverse {
		i.iter.SeekForPrev(at)
		return
	}
	i.iter.Seek(at)
}

func (i *iterator) Valid() bool {
	return i.iter.Valid()
}

func (i *iterator) ValidForPrefix(prefix []byte) bool {
	return i.iter.ValidForPrefix(prefix)
}

func (i *iterator) Next() {
	if i.opts.Reverse {
		i.iter.Prev()
		return
	}
	i.iter.Next()
}

func (i *iterator) Item() *storage.Entry {
	k := i.iter.Key()
	defer k.Free()

	v := i.iter.Value()
	defer v.Free()

	key := sgutils.CopyBytes(k.Data())
	val := sgutils.CopyBytes(v.Data())

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
