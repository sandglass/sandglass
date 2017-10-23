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
	"net"
	"net/http"
	"net/url"
	"os"

	"time"

	"github.com/celrenheit/sandglass/broker"
	"github.com/celrenheit/sandglass/logy"
	"github.com/celrenheit/sandglass/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tj/go-gracefully"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "sandglass",
	Short: "Launch the server",
	Long:  `Launch the server`,
	Run: func(cmd *cobra.Command, args []string) {
		conf := broker.Config{
			Name:          viper.GetString("name"),
			BindAddr:      viper.GetString("bind_addr"),
			AdvertiseAddr: viper.GetString("advertise_addr"),
			DBPath:        viper.GetString("db_path"),
			GossipPort:    viper.GetString("gossip_port"),
			HTTPPort:      viper.GetString("http_port"),
			GRPCPort:      viper.GetString("grpc_port"),
			RaftPort:      viper.GetString("raft_port"),
			InitialPeers:  viper.GetStringSlice("initial_peers"),
			BootstrapRaft: viper.GetBool("bootstrap_raft"),
		}

		b, err := broker.New(&conf)
		if err != nil {
			log.Fatal(errors.Cause(err))
		}

		if err := b.Bootstrap(); err != nil {
			log.Fatal(errors.Cause(err))
		}

		if err := b.Join(conf.InitialPeers...); err != nil {
			log.Fatal(err)
		}

		logger := log.New(os.Stdout, conf.Name, log.LstdFlags)
		server := server.New(b, net.JoinHostPort(conf.BindAddr, conf.GRPCPort), net.JoinHostPort(conf.BindAddr, conf.HTTPPort), logy.NewWithLogger(logger, logy.INFO))
		go func() {
			err := server.Start()
			if err != nil {
				log.Println(errors.Cause(err))
			}
		}()

		// waiting for shutdown
		gracefully.Timeout = 10 * time.Second
		gracefully.Shutdown()
		fmt.Println("graceful shutdown")
		ctx := context.Background()

		fmt.Println("shutting down http server")
		if err := server.Shutdown(ctx); err != nil {
			log.Println(errors.Cause(err))
		}

		fmt.Println("shutting down broker")
		if err := b.Stop(ctx); err != nil {
			log.Println(errors.Cause(err))
		}

		fmt.Println("Done!")
		os.Exit(0)
	},
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.sandglass.yaml)")
	viper.BindPFlag("config", RootCmd.PersistentFlags().Lookup("config"))
	RootCmd.PersistentFlags().String("name", "", "name")
	viper.BindPFlag("name", RootCmd.PersistentFlags().Lookup("name"))
	RootCmd.PersistentFlags().String("http_port", ":2108", "http addr")
	viper.BindPFlag("http_port", RootCmd.PersistentFlags().Lookup("http_port"))
	RootCmd.PersistentFlags().String("grpc_port", ":7170", "grpc addr")
	viper.BindPFlag("grpc_port", RootCmd.PersistentFlags().Lookup("grpc_port"))
	RootCmd.PersistentFlags().String("gossip_port", ":9900", "gossip addr")
	viper.BindPFlag("gossip_port", RootCmd.PersistentFlags().Lookup("gossip_port"))
	RootCmd.PersistentFlags().String("advertise_addr", "", "advertise addr")
	viper.BindPFlag("advertise_addr", RootCmd.PersistentFlags().Lookup("advertise_addr"))
	RootCmd.PersistentFlags().String("bind_addr", "", "bind addr")
	viper.BindPFlag("bind_addr", RootCmd.PersistentFlags().Lookup("bind_addr"))
	RootCmd.PersistentFlags().String("db_path", "/tmp/sandglassdb", "base directory for data storage")
	viper.BindPFlag("db_path", RootCmd.PersistentFlags().Lookup("db_path"))
	RootCmd.PersistentFlags().StringArrayP("initial_peers", "p", nil, "Inital peers")
	viper.BindPFlag("initial_peers", RootCmd.PersistentFlags().Lookup("initial_peers"))
	RootCmd.PersistentFlags().Bool("bootstrap_raft", false, "Bootstrap raft")
	viper.BindPFlag("bootstrap_raft", RootCmd.PersistentFlags().Lookup("bootstrap_raft"))

	RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName(".sandglass") // name of config file (without extension)
		viper.AddConfigPath("$HOME")      // adding home directory as first search path
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if isURL(cfgFile) {
		resp, err := http.Get(cfgFile)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if err := viper.ReadConfig(resp.Body); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := viper.ReadInConfig(); err == nil {
			fmt.Println("Using config file:", viper.ConfigFileUsed())
		} else {
			fmt.Println("err", err)
		}
	}
}

func isURL(u string) bool {
	_, err := url.ParseRequestURI(u)
	return err == nil
}
