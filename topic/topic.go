package topic

import (
	"path/filepath"
	"sync"

	"fmt"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/celrenheit/sandglass/sgutils"
	"golang.org/x/sync/errgroup"
)

type Topic struct {
	Name              string
	Kind              sgproto.TopicKind
	ReplicationFactor int
	NumPartitions     int
	Partitions        []*Partition
	StorageDriver     sgproto.StorageDriver

	basepath string
}

func (t *Topic) Validate() error {
	if t.Name == "" { // TODO: check non ascii chars
		return fmt.Errorf("topic name should not be empty")
	}
	if t.ReplicationFactor < 1 {
		return fmt.Errorf("replication factor should not be > 0")
	}
	if t.NumPartitions < 1 {
		return fmt.Errorf("number of partitions should not be > 0")
	}

	return nil
}

func (t *Topic) InitStore(basePath string) error {
	msgdir := filepath.Join(basePath, t.Name)
	t.basepath = msgdir

	if err := sgutils.MkdirIfNotExist(t.basepath); err != nil {
		return err
	}

	if t.NumPartitions <= 0 {
		return fmt.Errorf("Num partitions should be > 0")
	}

	for _, p := range t.Partitions {
		err := t.initPartition(p)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Topic) initPartition(p *Partition) error {
	p.topic = t
	err := p.InitStore(t.basepath)
	if err != nil {
		return err
	}

	return nil
}

func (t *Topic) ListPartitions() []*Partition { return t.Partitions }

func (t *Topic) GetPartition(id string) *Partition {
	for _, p := range t.Partitions {
		if p.Id == id {
			return p
		}
	}

	return nil
}

func (t *Topic) ChoosePartitionForKey(key []byte) *Partition {
	var id []byte
	switch t.Kind {
	case sgproto.TopicKind_TimerKind:
		panic(fmt.Sprintf("not for timer kind %v %v", t.Name, t.Partitions))
	case sgproto.TopicKind_KVKind:
		id = key
	}

	idx := sgutils.Hash(id, len(t.Partitions))
	return t.Partitions[idx]
}

func (t *Topic) ChoosePartition(msg *sgproto.Message) *Partition {
	var id []byte
	switch t.Kind {
	case sgproto.TopicKind_TimerKind:
		id = msg.Offset.Bytes()
	case sgproto.TopicKind_KVKind:
		id = msg.Key
	}

	idx := sgutils.Hash(id, len(t.Partitions))
	return t.Partitions[idx]
}

func (t *Topic) PutMessage(msg *sgproto.Message) error {
	var p *Partition
	if msg.Partition != "" {
		p = t.GetPartition(msg.Partition)
	} else {
		p = t.ChoosePartition(msg)
	}
	if msg.Offset == sandflake.Nil {
		msg.Offset = p.NextID()
	}

	return p.PutMessage(msg)
}

func (t *Topic) BatchPutMessages(msgs []*sgproto.Message) error {
	msgsByPartitions := make([][]*sgproto.Message, len(t.Partitions))
	for _, m := range msgs {
		pidx := sgutils.HashString(m.Offset.String(), len(t.Partitions))
		msgsByPartitions[pidx] = append(msgsByPartitions[pidx], m)
	}

	var group errgroup.Group
	for pidx, msgs := range msgsByPartitions {
		p := t.Partitions[pidx]
		msgs := msgs
		group.Go(func() error {
			return p.BatchPutMessages(msgs)
		})
	}

	return group.Wait()
}

func (t *Topic) Close() error {
	var group errgroup.Group
	for _, p := range t.Partitions {
		group.Go(p.Close)
	}
	return group.Wait()
}

func (t *Topic) ForEach(fn func(msg *sgproto.Message) error) error {
	return t.ForRange(sandflake.Nil, sandflake.MaxID, fn)
}

func (t *Topic) ForRange(min, max sandflake.ID, fn func(msg *sgproto.Message) error) error {
	// FIXME
	var mu sync.Mutex
	var group errgroup.Group
	for _, p := range t.Partitions {
		p := p
		group.Go(func() error {
			return p.ForRange(min, max, func(msg *sgproto.Message) error {
				mu.Lock()
				err := fn(msg)
				mu.Unlock()
				return err
			})
		})
	}

	return group.Wait()
}
