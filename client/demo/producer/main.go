package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/client"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
)

func main() {
	addr := flag.String("addrs", "", "GRPC addresses of sandglass cluster")
	file := flag.String("file", "", "GRPC addresses of sandglass cluster")
	n := flag.Int("n", 1, "GRPC addresses of sandglass cluster")

	flag.Parse()

	data, err := ioutil.ReadFile(*file)
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
	fmt.Println("producing to partition", partition)
	var gen sandflake.Generator
	start := time.Now()
	msgCh, errCh := c.ProduceMessageCh(context.Background())
	for i := 0; i < *n; i++ {
		msg := &sgproto.Message{
			Topic:     topic,
			Partition: partition,
			Offset:    gen.Next(),
			Value:     data,
		}

		select {
		case msgCh <- msg:
		case err := <-errCh:
			panic(err)
		}
	}
	close(msgCh)
	fmt.Println("waiting for done")
	<-errCh

	fmt.Println("took", time.Since(start))
}
