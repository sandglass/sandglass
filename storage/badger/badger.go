package badger

import (
	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/storage/scommons"
	"github.com/dgraph-io/badger"
)

type Storage struct {
	db *badger.DB
	scommons.StorageCommons
}

func NewStorage(path string) (*Storage, error) {
	opt := badger.DefaultOptions
	opt.Dir = path
	opt.ValueDir = path
	opt.SyncWrites = true
	db, err := badger.Open(opt)
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
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		val, err = item.Value()
		return err
	})
	return val, err
}

func (s *Storage) Put(key, val []byte) error {
	return s.BatchPut([]*storage.Entry{{Key: key, Value: val}})
}

func (s *Storage) BatchPut(entries []*storage.Entry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			if err := txn.Set(e.Key, e.Value); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *Storage) Iter(opts *storage.IterOptions) storage.Iterator {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = opts.FetchValues
	opt.Reverse = opts.Reverse
	txn := s.db.NewTransaction(false)

	return &iterator{iter: txn.NewIterator(opt), txn: txn}
}

func (s *Storage) IterReverse() storage.Iterator {
	opt := badger.DefaultIteratorOptions
	opt.Reverse = true

	txn := s.db.NewTransaction(false)
	return &iterator{iter: txn.NewIterator(opt), txn: txn}
}

func (s *Storage) Close() error {
	return s.db.Close()
}

var _ storage.Storage = (*Storage)(nil)
