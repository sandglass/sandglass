package topic

import (
	"path/filepath"
	"sync"

	"fmt"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
	"github.com/celrenheit/sandglass/sgutils"
	"github.com/serialx/hashring"
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
	ring     *hashring.HashRing
}

func (t *Topic) InitStore(basePath string) error {
	t.ring = hashring.New([]string{})

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
	t.ring = t.ring.AddNode(p.Id)

	return nil
}

func (t *Topic) AddPartition(p *Partition) error {
	err := t.initPartition(p)
	if err != nil {
		return err
	}
	t.Partitions = append(t.Partitions, p)
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
	var id string
	switch t.Kind {
	case sgproto.TopicKind_TimerKind:
		panic(fmt.Sprintf("not for timer kind %v %v", t.Name, t.Partitions))
	case sgproto.TopicKind_CompactedKind:
		id = string(key)
	}
	pid, ok := t.ring.GetNode(id)
	if !ok {
		panic(fmt.Sprintf("not ok %v %v", t.Name, t.Partitions))
	}

	return t.getPartition(pid)
}

func (t *Topic) ChoosePartition(msg *sgproto.Message) *Partition {
	var id string
	switch t.Kind {
	case sgproto.TopicKind_TimerKind:
		id = msg.Offset.String()
	case sgproto.TopicKind_CompactedKind:
		id = string(msg.Key)
	}

	pid, ok := t.ring.GetNode(id)
	if !ok {
		panic(fmt.Sprintf("not ok %v %v", t.Name, t.Partitions))
	}

	return t.getPartition(pid)
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
		pidx, ok := t.ring.GetNodePos(m.Offset.String())
		pidx = pidx % len(t.Partitions)
		if ok {
			msgsByPartitions[pidx] = append(msgsByPartitions[pidx], m)
		}
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

func (t *Topic) getPartition(id string) *Partition {
	for _, p := range t.Partitions {
		if p.Id == id {
			return p
		}
	}

	return nil
}
