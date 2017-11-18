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

	"github.com/celrenheit/sandglass/cmd/cmdcommon"

	"github.com/fatih/color"

	"time"

	"github.com/celrenheit/sandglass/broker"
	"github.com/celrenheit/sandglass/logy"
	"github.com/celrenheit/sandglass/server"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tj/go-gracefully"
)

var (
	warningColor   = color.New(color.Italic, color.FgHiBlack).SprintfFunc()
	infoColor      = color.New(color.FgHiBlack).SprintfFunc()
	sandglassColor = color.New(color.Bold, color.FgHiGreen).SprintfFunc()
)

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
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

		if viper.GetBool("verbose") {
			lvl := logy.DEBUG
			conf.LoggingLevel = &lvl
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

		fmt.Println(warningColor("WARNING: This code is a very early release, it contains bugs and should not be used in production environments."))

		fmt.Println("")
		fmt.Println(infoColor("wait for it..."))
		err = b.WaitForIt()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(sandglassColor(`
⏳  Sandglass started!
Beware of the sandstorm.
`))

		// waiting for shutdown
		gracefully.Timeout = 10 * time.Second
		gracefully.Shutdown()
		b.Debug("graceful shutdown")
		ctx := context.Background()

		b.Debug("shutting down http server")
		if err := server.Shutdown(ctx); err != nil {
			log.Println(errors.Cause(err))
		}

		b.Debug("shutting down broker")
		if err := b.Stop(ctx); err != nil {
			log.Println(errors.Cause(err))
		}

		fmt.Println(sandglassColor("⌛️  sandglass stopped!"))
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
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "config")
	RootCmd.PersistentFlags().String("name", "", "name")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "name")
	RootCmd.PersistentFlags().String("http_port", "2108", "http addr")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "http_port")
	RootCmd.PersistentFlags().String("grpc_port", "7170", "grpc addr")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "grpc_port")
	RootCmd.PersistentFlags().String("raft_port", "9190", "raft addr")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "raft_port")
	RootCmd.PersistentFlags().String("gossip_port", "9900", "gossip addr")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "gossip_port")
	RootCmd.PersistentFlags().String("advertise_addr", "", "advertise addr")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "advertise_addr")
	RootCmd.PersistentFlags().String("bind_addr", "0.0.0.0", "bind addr")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "bind_addr")
	RootCmd.PersistentFlags().String("db_path", "/tmp/sandglassdb", "base directory for data storage")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "db_path")
	RootCmd.PersistentFlags().StringSliceP("initial_peers", "p", nil, "Inital peers")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "initial_peers")
	RootCmd.PersistentFlags().Bool("bootstrap_raft", false, "Bootstrap raft")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "bootstrap_raft")
	RootCmd.PersistentFlags().BoolP("verbose", "v", false, "Bootstrap raft")
	cmdcommon.BindViper(RootCmd.PersistentFlags(), "verbose")
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
			fmt.Println(infoColor("Using config file: %s", viper.ConfigFileUsed()))
		}
	}
}

func isURL(u string) bool {
	_, err := url.ParseRequestURI(u)
	return err == nil
}
