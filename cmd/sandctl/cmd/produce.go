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
	"io/ioutil"
	"log"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"

	"github.com/celrenheit/sandglass/cmd/cmdcommon"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// produceCmd represents the list command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			log.Fatal("you should select one topic")
		}
		var (
			topic     = args[0]
			partition = viper.GetString("partition")
		)

		var data []byte
		switch {
		case len(args) == 2:
			data = []byte(args[1])
		case cmd.Flag("file").Changed:
			var err error
			data, err = ioutil.ReadFile(viper.GetString("file"))
			if err != nil {
				log.Fatal(err)
			}
		default:
			log.Fatal("you should provide a file or a payload")
		}

		ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration("timeout"))
		defer cancel()

		produce := func(msgs []*sgproto.Message) error {
			_, err := client.Produce(ctx, &sgproto.ProduceMessageRequest{
				Topic:     topic,
				Partition: partition,
				Messages:  msgs,
			})
			return err
		}

		batchSize := 10000
		messages := make([]*sgproto.Message, 0, batchSize)
		for i := 0; i < viper.GetInt("number"); i++ {
			messages = append(messages, &sgproto.Message{
				Value: data,
			})

			if len(messages) == batchSize {
				if err := produce(messages); err != nil {
					panic(err)
				}
				messages = messages[:0]
			}
		}

		if len(messages) > 0 {
			if err := produce(messages); err != nil {
				panic(err)
			}
		}

		fmt.Println("OK")
	},
}

func init() {
	RootCmd.AddCommand(produceCmd)

	produceCmd.Flags().String("partition", "", "In which partition should we produce")
	produceCmd.Flags().String("file", "", "File to produce")
	produceCmd.Flags().IntP("number", "n", 1, "File to produce")

	cmdcommon.BindViper(produceCmd.Flags(),
		"partition",
		"file",
		"number",
	)
}
