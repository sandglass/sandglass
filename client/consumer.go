package client

import (
	"context"
	"io"
	"log"

	"github.com/celrenheit/sandglass/sgproto"
)

type Consumer struct {
	client                        *Client
	topic, partition, group, name string
}

func (c *Consumer) Consume(ctx context.Context) (chan *sgproto.Message, error) {
	consumer, err := c.client.client.ConsumeFromGroup(ctx, &sgproto.ConsumeFromGroupRequest{
		Topic:             c.topic,
		Partition:         c.partition,
		ConsumerGroupName: c.group,
		ConsumerName:      c.name,
	})
	if err != nil {
		return nil, err
	}

	msgCh := make(chan *sgproto.Message, 1)

	go func() {
		defer close(msgCh)
		for {
			msg, err := consumer.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				// TODO: handle this error more appropriately
				log.Printf("got err while consuming: %v", err)
				return
			}

			msgCh <- msg
		}
	}()

	return msgCh, nil
}

func (c *Consumer) Acknowledge(ctx context.Context, msg *sgproto.Message) error {
	_, err := c.client.client.Acknowledge(ctx, &sgproto.OffsetChangeRequest{
		Topic:         c.topic,
		Partition:     c.partition,
		ConsumerGroup: c.group,
		ConsumerName:  c.name,
		Offset:        msg.Offset,
	})
	return err
}

func (c *Consumer) Commit(ctx context.Context, msg *sgproto.Message) error {
	_, err := c.client.client.Commit(ctx, &sgproto.OffsetChangeRequest{
		Topic:         c.topic,
		Partition:     c.partition,
		ConsumerGroup: c.group,
		ConsumerName:  c.name,
		Offset:        msg.Offset,
	})
	return err
}
