package sg

import (
	"context"
	"sync"
	"time"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"golang.org/x/sync/errgroup"
)

type Handler interface {
	Consume(msg *sgproto.Message) error
}

type HandlerFunc func(msg *sgproto.Message) error

func (h HandlerFunc) Consume(msg *sgproto.Message) error {
	return h(msg)
}

type Mux struct {
	handlers map[string]Handler
}

func NewMux() *Mux {
	return &Mux{
		handlers: make(map[string]Handler),
	}
}

func (m *Mux) Subscribe(topic string, h Handler) {
	m.handlers[topic] = h
}

func (m *Mux) SubscribeFunc(topic string, h func(msg *sgproto.Message) error) {
	m.handlers[topic] = HandlerFunc(h)
}

type MuxManager struct {
	Mux                  *Mux
	Client               *Client
	ConsumerGroup        string
	ConsumerName         string
	SynchronousCompution bool
	ReFetchSleepDuration time.Duration

	ctx       context.Context
	cancel    context.CancelFunc
	group     *errgroup.Group
	handlerwg sync.WaitGroup
}

func (m *MuxManager) Start() error {
	if m.ReFetchSleepDuration == 0 {
		m.ReFetchSleepDuration = 100 * time.Millisecond
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.group, m.ctx = errgroup.WithContext(m.ctx)
	for topic, handler := range m.Mux.handlers {
		parts, err := m.Client.ListPartitions(m.ctx, topic)
		if err != nil {
			return err
		}

		for _, part := range parts {
			m.subscribe(topic, part, handler)
		}
	}

	if err := m.group.Wait(); err != nil {
		return err
	}
	m.handlerwg.Wait()
	return nil
}

func (m *MuxManager) subscribe(topic, part string, handler Handler) {
	consumer := m.Client.NewConsumer(topic, part, m.ConsumerGroup, m.ConsumerName)
	m.group.Go(func() error {
		for {
			select {
			case <-m.ctx.Done():
				return nil
			default:
			}

			msgCh, err := consumer.Consume(m.ctx)
			if err != nil {
				return err
			}

			for msg := range msgCh {
				if m.SynchronousCompution {
					m.handleMessage(msg, consumer, handler)
				} else {
					m.handlerwg.Add(1)
					go func(msg *sgproto.Message) {
						defer m.handlerwg.Done()
						m.handleMessage(msg, consumer, handler)
					}(msg)
				}
			}

			time.Sleep(m.ReFetchSleepDuration)
		}

		return nil
	})
}

func (m *MuxManager) handleMessage(msg *sgproto.Message, consumer *Consumer, handler Handler) {
	err := handler.Consume(msg)
	if err != nil {
		consumer.NotAcknowledge(context.Background(), msg)
		return
	}

	err = consumer.Acknowledge(context.Background(), msg)
}

func (m *MuxManager) Shutdown(ctx context.Context) error {
	m.cancel()
	return nil
}
