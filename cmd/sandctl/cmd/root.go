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
	"fmt"
	"log"
	"os"
	"time"

	"github.com/celrenheit/sandglass/cmd/cmdcommon"

	"github.com/celrenheit/sandglass/sgproto"
	"google.golang.org/grpc"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	client  sgproto.BrokerServiceClient
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "sandctl",
	Short: "SandCtl",
	Long:  `SandCtl`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("run root")
	},
	PostRun: func(cmd *cobra.Command, args []string) {
		fmt.Println("PostRun")
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	var (
		conn *grpc.ClientConn
		err  error
	)

	for _, addr := range viper.GetStringSlice("addrs") {
		conn, err = grpc.Dial(addr,
			grpc.WithInsecure(),
		)
		if err == nil {
			break
		}
	}
	if conn == nil {
		log.Fatal("no address worked")
	}

	client = sgproto.NewBrokerServiceClient(conn)

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := conn.Close(); err != nil {
		log.Fatalf("error while closing connection: %v", err)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.sandctl.yaml)")

	RootCmd.PersistentFlags().StringSliceP("addrs", "s", []string{":7170"}, "grpc adresses of nodes in sandglass cluster")
	RootCmd.PersistentFlags().Duration("timeout", 3*time.Second, "timeout")

	cmdcommon.BindViper(RootCmd.PersistentFlags(),
		"config",
		"addrs",
		"timeout",
	)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".sandctl" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".sandctl")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
