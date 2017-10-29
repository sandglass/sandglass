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
	"log"
	"time"

	"github.com/celrenheit/sandglass/cmd/cmdcommon"

	"golang.org/x/sync/errgroup"

	"github.com/spf13/viper"

	"github.com/celrenheit/sandflake"
	"github.com/celrenheit/sandglass/sgproto"
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
				consume(msgCh, topic, viper.GetString("partition"), consumerGroup, consumerName, viper.GetBool("follow"), viper.GetBool("commit"))
				return nil
			})
		} else {
			partitions, err := cli.ListPartitions(context.Background(), topic)
			if err != nil {
				panic(err)
			}

			for _, part := range partitions {
				part := part
				group.Go(func() error {
					consume(msgCh, topic, part, consumerGroup, consumerName, viper.GetBool("follow"), viper.GetBool("commit"))
					return nil
				})
			}
		}

		go func() {
			group.Wait()
			close(msgCh)
		}()

		fmt.Printf("PARTITION\tOFFSET\tPAYLOAD\n")
		for msg := range msgCh {
			fmt.Printf("%s\t%s\t%s\n", msg.Partition, msg.Offset, string(msg.Value))
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
	consumeCmd.Flags().Bool("commit", true, "Commit after consumption")

	cmdcommon.BindViper(consumeCmd.Flags(),
		"partition",
		"consumer-group",
		"consumer-name",
		"poll-interval",
		"follow",
		"commit",
	)
}

func consume(msgCh chan *sgproto.Message, topic, partition, group, name string, follow, commit bool) {
	ctx := context.Background()
	consumer := cli.NewConsumer(topic, partition, group, name)

FOLLOW:
	consumeCh, err := consumer.Consume(ctx)
	if err != nil {
		panic(err)
	}

	var msg *sgproto.Message
	offsets := []sandflake.ID{}
	for msg = range consumeCh {
		offsets = append(offsets, msg.Offset)
		msgCh <- msg
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

	if msg != nil && commit {
		err := consumer.Commit(ctx, msg)
		if err != nil {
			panic(err)
		}
	}

	if follow {
		time.Sleep(viper.GetDuration("poll-interval"))
		goto FOLLOW
	}
}
