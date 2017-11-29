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

	"github.com/celrenheit/sandglass/cmd/cmdcommon"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
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

		msgCh, errCh := cli.ProduceMessageCh(ctx, topic, partition)
		for i := 0; i < viper.GetInt("number"); i++ {
			msg := &sgproto.Message{
				Value: data,
			}

			select {
			case msgCh <- msg:
			case err := <-errCh:
				panic(err)
			}
		}
		close(msgCh)
		<-errCh

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
