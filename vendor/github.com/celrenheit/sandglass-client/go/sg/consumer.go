package sg

import (
	"context"
	"io"
	"log"

	"github.com/celrenheit/sandflake"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
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

func (c *Consumer) Acknowledge(ctx context.Context, msgs ...*sgproto.Message) error {
	offsets := make([]sandflake.ID, len(msgs))
	for i, msg := range msgs {
		offsets[i] = msg.Offset
	}
	_, err := c.client.client.Acknowledge(ctx, &sgproto.MarkRequest{
		Topic:         c.topic,
		Partition:     c.partition,
		ConsumerGroup: c.group,
		ConsumerName:  c.name,
		Offsets:       offsets,
	})
	return err
}

func (c *Consumer) NotAcknowledge(ctx context.Context, msgs ...*sgproto.Message) error {
	offsets := make([]sandflake.ID, len(msgs))
	for i, msg := range msgs {
		offsets[i] = msg.Offset
	}
	_, err := c.client.client.NotAcknowledge(ctx, &sgproto.MarkRequest{
		Topic:         c.topic,
		Partition:     c.partition,
		ConsumerGroup: c.group,
		ConsumerName:  c.name,
		Offsets:       offsets,
	})
	return err
}
