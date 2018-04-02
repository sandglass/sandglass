package topic

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/tylertreat/BoomFilters"

	"github.com/celrenheit/sandflake"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/storage"
	"github.com/celrenheit/sandglass/storage/scommons"
	"github.com/gogo/protobuf/proto"
	"github.com/willf/bloom"
)

var (
	ErrNoKeySet = errors.New("ErrNoKeySet")
)

type Partition struct {
	db       storage.Storage
	Id       string
	Replicas []string
	idgen    sandflake.Generator
	basepath string
	topic    *Topic

	bf  *bloom.BloomFilter // TODO: persist bloom filter
	ibf boom.Filter

	lastIndex uint64

	ctxPending    context.Context
	cancelPending context.CancelFunc
	wg            sync.WaitGroup

	logger *logrus.Entry

	incomming chan *incommingRequest
}

type incommingRequest struct {
	messages []*sgproto.Message
	resp     chan error
}

func (t *Partition) InitStore(db storage.Storage) error {
	t.logger = logrus.WithFields(logrus.Fields{
		"component": "partition",
		"partition": t.Id,
	})
	t.bf = bloom.NewWithEstimates(1e3, 1e-2)
	t.ibf = boom.NewInverseBloomFilter(1e3)
	t.db = db

	var index uint64
	msg, err := t.EndOfLog()
	if err != nil {
		return fmt.Errorf("unable to fetch last message for init partition: %v", err)
	}

	if msg != nil {
		index = msg.Index
	}
	t.lastIndex = index

	if t.ctxPending == nil {
		t.incomming = make(chan *incommingRequest)
		t.applyPendingToWalLoop()
	}

	return nil
}

func (p *Partition) applyPendingToWalLoop() {
	p.ctxPending, p.cancelPending = context.WithCancel(context.Background())
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.ctxPending.Done():
				return
			case req := <-p.incomming:
				req.resp <- p.storeMessages(req.messages)
			}
		}
	}()
}

func (p *Partition) storeMessages(msgs []*sgproto.Message) error {
	index := p.lastIndex + 1

	var entries []*storage.Entry
	for _, msg := range msgs {
		msg.Index = index
		if msg.Offset == sgproto.Nil {
			msg.Offset = sgproto.NewOffset(msg.Index, msg.ProducedAt.Add(msg.ConsumeIn))
		}
		val, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		entries = append(entries, &storage.Entry{
			Key:   p.newWALKey(msg),
			Value: val,
		})

		index++
	}

	if len(entries) == 0 {
		return nil
	}

	if err := p.db.BatchPut(entries); err != nil {
		return err
	}

	p.lastIndex += uint64(len(msgs))
	return nil
}

func (p *Partition) WalToView(start, end uint64) error {
	entries := []*storage.Entry{}
	err := p.db.ForRangeWAL(p.prependPrefixWAL(), start, end, func(msg *sgproto.Message) error {
		storagekey := p.getStorageKey(msg)

		b, err := proto.Marshal(msg)
		if err != nil {
			return err
		}

		entries = append(entries, &storage.Entry{
			Key:   storagekey,
			Value: b,
		})

		return nil
	})
	if err != nil {
		return err
	}

	return p.db.BatchPut(entries)
}

func (t *Partition) String() string {
	return t.Id
}

func (t *Partition) getStorageKey(msg *sgproto.Message) []byte {
	var storekey []byte
	switch t.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		storekey = msg.Offset[:]
	case sgproto.TopicKind_KVKind:
		storekey = msg.Key
		if len(msg.ClusteringKey) > 0 {
			storekey = joinKeys(msg.Key, msg.ClusteringKey)
		}
	default:
		panic("INVALID STORAGE KIND: " + t.topic.Kind.String())
	}
	return t.prependPrefixView(storekey)
}

func (s *Partition) GetMessage(offset sgproto.Offset, k, suffix []byte) (*sgproto.Message, error) {
	switch s.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		val, err := s.db.Get(s.prependPrefixView(offset[:]))
		if err != nil {
			return nil, err
		}

		var msg sgproto.Message
		err = proto.Unmarshal(val, &msg)
		if err != nil {
			return nil, err
		}

		return &msg, nil
	case sgproto.TopicKind_KVKind:
		val := s.db.LastKVForPrefix(s.prependPrefixView(k), suffix)
		if val == nil {
			return nil, nil
		}

		var msg sgproto.Message
		err := proto.Unmarshal(val, &msg)
		if err != nil {
			return nil, err
		}

		return &msg, nil
	default:
		return nil, fmt.Errorf("invalid storage kind: %s", s.topic.Kind.String())
	}
}

func (s *Partition) prependPrefixView(keys ...[]byte) []byte {
	base := [][]byte{scommons.ViewPrefix, []byte(s.topic.Name), []byte(s.Id)}
	return scommons.Join(append(base, keys...)...)
}

func (s *Partition) prependPrefixWAL(keys ...[]byte) []byte {
	base := [][]byte{scommons.WalPrefix, []byte(s.topic.Name), []byte(s.Id)}
	return scommons.Join(append(base, keys...)...)
}

func (t *Partition) HasKey(key, clusterKey []byte) (bool, error) {
	switch t.topic.Kind {
	case sgproto.TopicKind_KVKind:
	default:
		return false, errors.New("HasKey should be used only with a KV topic")
	}

	pk := joinKeys(key, clusterKey)
	existKey := t.prependPrefixView(pk)

	if t.ibf.Test(existKey) {
		return true, nil
	}

	if !t.bf.Test(existKey) {
		return false, nil
	}

	msg, err := t.GetMessage(sgproto.Nil, pk, nil)
	if err != nil {
		return false, err
	}

	if msg == nil {
		return false, nil
	}

	return msg.Offset != sgproto.Nil, nil
}

func (t *Partition) newWALKey(msg *sgproto.Message) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, msg.Index)
	return t.prependPrefixWAL(b)
}

func (t *Partition) PutMessage(msg *sgproto.Message) error {
	return t.BatchPutMessages([]*sgproto.Message{msg})
}

func (t *Partition) BatchPutMessages(msgs []*sgproto.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	now := time.Now().UTC()

	for _, msg := range msgs {
		msg.ProducedAt = now
	}

	req := &incommingRequest{
		messages: msgs,
		resp:     make(chan error, 1),
	}

	t.incomming <- req

	return <-req.resp
}

func (p *Partition) WALBatchPutMessages(msgs []*sgproto.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	entries := []*storage.Entry{}
	for _, msg := range msgs {
		val, err := proto.Marshal(msg)
		if err != nil {
			return err
		}

		entries = append(entries, &storage.Entry{
			Key:   p.newWALKey(msg),
			Value: val,
		})
	}

	return p.db.BatchPut(entries)
}

func (p *Partition) ForRange(min, max sgproto.Offset, fn func(msg *sgproto.Message) error) error {
	var lastKey []byte
	switch p.topic.Kind {
	case sgproto.TopicKind_TimerKind:
		return p.db.ForRange(p.prependPrefixView(), min, max, func(msg *sgproto.Message) error {
			err := fn(msg)
			return err
		})
	case sgproto.TopicKind_KVKind:
		it := scommons.NewMessageIterator(p.prependPrefixView(), p.db, &storage.IterOptions{
			FetchValues: true,
			Reverse:     true,
		})
		defer it.Close()
		for m := it.Rewind(); it.Valid(); m = it.Next() {
			if lastKey == nil || !bytes.Equal(m.Key, lastKey) {
				err := fn(m)
				if err != nil {
					return err
				}
				lastKey = m.Key
			}
		}
	default:
		panic("unknown topic kind: " + p.topic.Kind.String())
	}
	return nil
}

func (p *Partition) Close() error {
	if p.cancelPending != nil {
		p.cancelPending()
	}
	p.wg.Wait()
	return nil
}

func (p *Partition) Iter() storage.MessageIterator {
	return scommons.NewMessageIterator(p.prependPrefixView(), p.db, &storage.IterOptions{
		FetchValues: true,
		Reverse:     false,
	})
}

func (p *Partition) RangeFromWAL(min []byte, fn func(*sgproto.Message) error) error {
	return p.db.ForEachWALEntry(p.prependPrefixWAL(), min, fn)
}

func (p *Partition) LastWALEntry() []byte {
	return p.db.LastKeyForPrefix(p.prependPrefixWAL())
}

func (p *Partition) EndOfLog() (*sgproto.Message, error) {
	value := p.db.LastKVForPrefix(p.prependPrefixWAL(), nil)
	if len(value) == 0 {
		return nil, nil
	}

	var msg sgproto.Message
	if err := proto.Unmarshal(value, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}

func (p *Partition) getMessageByStorageKey(k []byte) (*sgproto.Message, error) {
	b, err := p.db.Get(k)
	if err != nil {
		return nil, err
	}

	if b == nil {
		panic("should not happend, key in wal should always be also in msgs")
	}

	var msg sgproto.Message
	err = proto.Unmarshal(b, &msg)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

func joinKeys(key, clusterKey []byte) []byte {
	return bytes.Join([][]byte{
		key,
		clusterKey,
	}, storage.Separator)
}
