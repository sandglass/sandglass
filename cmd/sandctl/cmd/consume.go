// Copyright © 2017 Salim Alami Idrissi
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/celrenheit/sandglass/cmd/cmdcommon"

	"golang.org/x/sync/errgroup"

	"github.com/spf13/viper"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/spf13/cobra"
)

// consumeCmd represents the list command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatal("only one topic is allowed")
		}
		topic := args[0]

		consumerGroup := viper.GetString("consumer-group")
		consumerName := viper.GetString("consumer-name")
		if consumerName == "" {
			var gen sandflake.Generator
			consumerName = gen.Next().String()
		}

		var group errgroup.Group
		msgCh := make(chan *sgproto.Message)
		if cmd.Flag("partition").Changed {
			group.Go(func() error {
				consume(msgCh, topic, viper.GetString("partition"), consumerGroup, consumerName, viper.GetBool("follow"), viper.GetBool("ack"))
				return nil
			})
		} else {
			t, err := client.GetTopic(context.Background(), &sgproto.GetTopicParams{
				Name: topic,
			})
			if err != nil {
				panic(err)
			}

			for _, part := range t.Partitions {
				part := part
				group.Go(func() error {
					consume(msgCh, topic, part, consumerGroup, consumerName, viper.GetBool("follow"), viper.GetBool("ack"))
					return nil
				})
			}
		}

		go func() {
			group.Wait()
			close(msgCh)
		}()

		for msg := range msgCh {
			fmt.Println(string(msg.Value))
		}

	},
}

func init() {
	RootCmd.AddCommand(consumeCmd)

	consumeCmd.Flags().String("partition", "random", "Partition to produce in")
	consumeCmd.Flags().String("consumer-group", "sandctl", "Consumer group")
	consumeCmd.Flags().String("consumer-name", "", "Consumer name (default: random)")
	consumeCmd.Flags().Duration("poll-interval", 50*time.Millisecond, "Poll interval")
	consumeCmd.Flags().BoolP("follow", "f", false, "Consumer name (default: random)")
	consumeCmd.Flags().Bool("ack", true, "Ack messages (batching 10k messages)")

	cmdcommon.BindViper(consumeCmd.Flags(),
		"partition",
		"consumer-group",
		"consumer-name",
		"poll-interval",
		"follow",
		"ack",
	)
}

func consume(msgCh chan *sgproto.Message, topic, partition, group, name string, follow, ack bool) {
	ctx := context.Background()

FOLLOW:

	stream, err := client.ConsumeFromGroup(ctx, &sgproto.ConsumeFromGroupRequest{
		Topic:             topic,
		Partition:         partition,
		ConsumerGroupName: group,
		ConsumerName:      name,
	})
	if err != nil {
		panic(err)
	}

	ackFn := func(offsets []sandflake.ID) error {
		_, err := client.Acknowledge(context.Background(), &sgproto.MarkRequest{
			Topic:         topic,
			Partition:     partition,
			ConsumerGroup: group,
			ConsumerName:  name,
			Offsets:       offsets,
		})
		return err
	}

	offsets := []sandflake.ID{}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		msgCh <- msg

		if ack {
			offsets = append(offsets, msg.Offset)
		}

		if ack && len(offsets) == 1000 {
			err := ackFn(offsets)
			if err != nil {
				panic(err)
			}

			offsets = offsets[:0]
		}
	}

	if ack && len(offsets) > 0 {
		err := ackFn(offsets)
		if err != nil {
			panic(err)
		}
	}

	if follow {
		time.Sleep(viper.GetDuration("poll-interval"))
		goto FOLLOW
	}
}
