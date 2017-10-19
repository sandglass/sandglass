package client

import (
	"context"

	"github.com/celrenheit/sandglass/sgproto"
	"google.golang.org/grpc"
)

type Client struct {
	opts   *options
	conn   *grpc.ClientConn
	client sgproto.BrokerServiceClient
}

type options struct {
	addrs []string
}

type optionFn func(*Client) error

func WithAddresses(addrs ...string) optionFn {
	return func(c *Client) (err error) {
		c.opts.addrs = addrs
		return err
	}
}

func New(opts ...optionFn) (c *Client, err error) {
	c = &Client{opts: &options{}}

	for _, o := range opts {
		err := o(c)
		if err != nil {
			return nil, err
		}
	}

	// TODO: use grpc loadbalancer
	for _, addr := range c.opts.addrs {
		c.conn, err = grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		break
	}

	c.client = sgproto.NewBrokerServiceClient(c.conn)

	return c, nil
}

func (c *Client) CreateTopic(ctx context.Context, params *sgproto.CreateTopicParams) error {
	_, err := c.client.CreateTopic(ctx, params)
	return err
}

func (c *Client) ListPartitions(ctx context.Context, topic string) ([]string, error) {
	res, err := c.client.GetTopic(ctx, &sgproto.GetTopicParams{
		Name: topic,
	})
	if err != nil {
		return nil, err
	}

	return res.Partitions, nil
}

func (c *Client) ProduceMessage(ctx context.Context, msg *sgproto.Message) error {
	_, err := c.client.Publish(ctx, msg)
	return err
}

func (c *Client) ProduceMessageCh(ctx context.Context) (chan<- *sgproto.Message, <-chan error) {
	errCh := make(chan error, 1)
	stream, err := c.client.PublishMessagesStream(ctx)
	if err != nil {
		errCh <- err
		return nil, errCh
	}

	msgCh := make(chan *sgproto.Message)

	go func() {
		for msg := range msgCh {
			err := stream.Send(msg)
			if err != nil {
				errCh <- err
				return
			}
		}
		err := stream.CloseSend()
		if err != nil {
			errCh <- err
		}
	}()

	return msgCh, errCh
}

func (c *Client) NewConsumer(topic, partition, group, name string) *Consumer {
	return &Consumer{
		client:    c,
		topic:     topic,
		partition: partition,
		group:     group,
		name:      name,
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}
