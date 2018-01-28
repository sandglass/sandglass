package badger

import (
	"time"

	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/storage/scommons"
	"github.com/dgraph-io/badger"
)

type Storage struct {
	db *badger.DB
	scommons.StorageCommons
	operators map[string]*badger.MergeOperator
}

func NewStorage(path string, operators ...*storage.MergeOperator) (*Storage, error) {
	opt := badger.DefaultOptions
	opt.Dir = path
	opt.ValueDir = path
	opt.SyncWrites = true
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		db:        db,
		operators: make(map[string]*badger.MergeOperator),
	}

	s.StorageCommons = scommons.StorageCommons{s}

	for _, operator := range operators {
		fn := func(operator *storage.MergeOperator) badger.MergeFunc {
			return func(existing, value []byte) []byte {
				newValue, ok := operator.MergeFunc(existing, value)
				if !ok {
					return existing
				}
				return newValue
			}
		}(operator)

		op := s.db.GetMergeOperator(operator.Key, fn, time.Minute)
		s.operators[string(operator.Key)] = op
	}

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

func (s *Storage) Merge(key, operation []byte) error {
	return s.operators[string(key)].Add(operation)
}

func (s *Storage) ProcessMergedKey(key []byte, fn func(val []byte) ([]*storage.Entry, []byte, error)) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		return err
	}

	val, err := item.Value()
	if err != nil {
		return err
	}

	entries, operation, err := fn(val)
	for _, e := range entries {
		if err := txn.Set(e.Key, e.Value); err != nil {
			return err
		}
	}

	if err := txn.Set(key, operation); err != nil {
		return err
	}

	return txn.Commit(nil)
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
