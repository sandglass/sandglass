package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/client"
	"github.com/celrenheit/sandglass/sgproto"
)

func main() {
	addr := flag.String("addrs", "", "GRPC addresses of sandglass cluster")
	sleepDur := flag.String("sleep", "50ms", "sleep time")

	flag.Parse()

	dur, err := time.ParseDuration(*sleepDur)
	if err != nil {
		panic(err)
	}

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
	fmt.Println("consuming to partition", partition)

	ctx := context.Background()
	consumer := c.NewConsumer(topic, partition, "group", "consumer1")

	for {
		start := time.Now()
		consumeCh, err := consumer.Consume(ctx)
		if err != nil {
			panic(err)
		}

		n := 0
		var msg *sgproto.Message
		offsets := []sandflake.ID{}
		for msg = range consumeCh {
			offsets = append(offsets, msg.Offset)
			n++
			if len(offsets) == 1000 {
				err = consumer.AcknowledgeMessages(context.Background(), offsets)
				if err != nil {
					panic(err)
				}

				offsets = offsets[:0]
			}
		}

		err = consumer.AcknowledgeMessages(context.Background(), offsets)
		if err != nil {
			panic(err)
		}

		if msg != nil {
			err := consumer.Commit(ctx, msg)
			if err != nil {
				panic(err)
			}
		}

		if n > 0 {
			fmt.Println("took", time.Since(start), "for", n)
			fmt.Println("each took", time.Duration(time.Since(start).Nanoseconds()/int64(n)))
		}

		time.Sleep(dur)
	}
}
