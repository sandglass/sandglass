package sg

import (
	"context"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
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

func WithGRPCClientConn(conn *grpc.ClientConn) optionFn {
	return func(c *Client) (err error) {
		c.conn = conn
		return err
	}
}

func NewClient(opts ...optionFn) (c *Client, err error) {
	c = &Client{opts: &options{}}

	for _, o := range opts {
		err := o(c)
		if err != nil {
			return nil, err
		}
	}

	if c.conn == nil {
		// TODO: use grpc loadbalancer
		for _, addr := range c.opts.addrs {
			c.conn, err = grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			break
		}
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

func (c *Client) ProduceMessage(ctx context.Context, topic, partition string, msg *sgproto.Message) error {
	_, err := c.client.Produce(ctx, &sgproto.ProduceMessageRequest{
		Topic:     topic,
		Partition: partition,
		Messages:  []*sgproto.Message{msg},
	})
	return err
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
