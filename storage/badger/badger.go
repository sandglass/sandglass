package badger

import (
	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/storage/scommons"
	"github.com/dgraph-io/badger"
)

type Storage struct {
	db *badger.KV
	scommons.StorageCommons
}

func NewStorage(path string) (*Storage, error) {
	opt := badger.DefaultOptions
	opt.Dir = path
	opt.ValueDir = path
	opt.SyncWrites = true
	db, err := badger.NewKV(&opt)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		db: db,
	}

	s.StorageCommons = scommons.StorageCommons{s}

	return s, nil
}

func (s *Storage) Get(key []byte) ([]byte, error) {
	var item badger.KVItem
	err := s.db.Get(key, &item)
	if err != nil {
		return nil, err
	}
	var val []byte
	err = item.Value(func(v []byte) error {
		val = v
		return nil
	})
	return val, err
}

func (s *Storage) Put(key, val []byte) error {
	return s.db.Set(key, val, 0)
}

func (s *Storage) BatchPut(entries []*storage.Entry) error {
	badgerEntries := make([]*badger.Entry, 0, len(entries))
	for _, e := range entries {
		badgerEntries = append(badgerEntries, &badger.Entry{
			Key:   e.Key,
			Value: e.Value,
		})
	}
	return s.db.BatchSet(badgerEntries)
}

func (s *Storage) Iter(opts *storage.IterOptions) storage.Iterator {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = opts.FetchValues
	opt.Reverse = opts.Reverse
	return &iterator{iter: s.db.NewIterator(opt)}
}

func (s *Storage) IterReverse() storage.Iterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = true
	return &iterator{iter: s.db.NewIterator(opt)}
}

func (s *Storage) Close() error {
	return s.db.Close()
}

var _ storage.Storage = (*Storage)(nil)
