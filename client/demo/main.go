package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/celrenheit/sandglass/sgproto"

	"github.com/celrenheit/sandglass/client"
)

func main() {
	addr := flag.String("addrs", "", "GRPC addresses of sandglass cluster")

	flag.Parse()

	c, err := client.New(client.WithAddresses(*addr))
	if err != nil {
		panic(err)
	}

	defer c.Close()

	topic := "payments"

	partitions, err := c.ListPartitions(context.Background(), topic)
	if err != nil {
		panic(err)
	}

	partition := partitions[0]
	// var gen sandflake.Generator
	// start := time.Now()
	// err = c.ProduceMessage(context.Background(), &sgproto.Message{
	// 	Topic:     topic,
	// 	Partition: partition,
	// 	Offset:    gen.Next(),
	// 	Value:     []byte("hello"),
	// })
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("took", time.Since(start))

	fmt.Println("consuming...")
	ctx := context.Background()
	consumer := c.NewConsumer(topic, partition, "group", "consumer1")
	for {
		msgCh, err := consumer.Consume(ctx)
		if err != nil {
			panic(err)
		}

		var msg *sgproto.Message
		for msg = range msgCh {
			fmt.Println(msg.Offset, "->", string(msg.Value))
			err := consumer.Acknowledge(ctx, msg)
			if err != nil {
				panic(err)
			}
		}

		if msg != nil {
			fmt.Println("committing", msg.Offset)
			err := consumer.Commit(ctx, msg)
			if err != nil {
				panic(err)
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}
