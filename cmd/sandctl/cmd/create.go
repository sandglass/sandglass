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

	"google.golang.org/grpc"

	"github.com/spf13/viper"

	"github.com/celrenheit/sandglass/cmd/cmdcommon"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/spf13/cobra"
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatal("only one topic is allowed")
		}
		name := args[0]

		ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration("timeout"))
		defer cancel()

		err := cli.CreateTopic(ctx, &sgproto.CreateTopicParams{
			Name:              name,
			ReplicationFactor: int32(viper.GetInt("replication_factor")),
			NumPartitions:     int32(viper.GetInt("num_partitions")),
			Kind:              sgproto.TopicKind(sgproto.TopicKind_value[viper.GetString("storage_driver")]),
			StorageDriver:     sgproto.StorageDriver(sgproto.StorageDriver_value[viper.GetString("num_partitions")]),
		})
		if err != nil {
			fmt.Println(grpc.ErrorDesc(err))
			return
		}

		fmt.Printf("topic '%s' was successfully created", name)
	},
}

func init() {
	topicsCmd.AddCommand(createCmd)

	createCmd.Flags().IntP("replication_factor", "r", 0, "Replication factor")
	createCmd.Flags().IntP("num_partitions", "p", 0, "Number of partitions")
	createCmd.Flags().String("storage_driver", sgproto.StorageDriver_RocksDB.String(), "Number of partitions")
	createCmd.Flags().String("kind", sgproto.TopicKind_TimerKind.String(), "Topic kind")

	cmdcommon.BindViper(createCmd.Flags(),
		"replication_factor",
		"num_partitions",
		"storage_driver",
		"kind",
	)
}
