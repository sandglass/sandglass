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
	yaml "gopkg.in/yaml.v2"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "sandglass",
	Short: "Launch the server",
	Long:  `Launch the server`,
	Run: func(cmd *cobra.Command, args []string) {
		conf := broker.Config{
			Name:          name,
			AdvertiseAddr: advertiseAddr,
			DBPath:        dbpath,
			HTTPAddr:      httpAddr,
			GRPCAddr:      grpcAddr,
			InitialPeers:  initialPeers,
		}
		if cfgFile != "" {
			fmt.Println("loading from config file:", cfgFile)
			err := loadConfigFile(cfgFile, &conf)
			if err != nil {
				log.Fatal(err)
			}
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
		server := server.New(b, conf.GRPCAddr, conf.HTTPAddr, logy.NewWithLogger(logger, logy.INFO))
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

var (
	name          string
	httpAddr      string
	grpcAddr      string
	advertiseAddr string
	dbpath        string
	initialPeers  []string
)

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.sandglass.yaml)")
	RootCmd.PersistentFlags().StringVar(&name, "name", "", "name")
	RootCmd.PersistentFlags().StringVar(&httpAddr, "http-addr", ":2108", "http addr")
	RootCmd.PersistentFlags().StringVar(&grpcAddr, "grpc-addr", ":7170", "grpc addr")
	RootCmd.PersistentFlags().StringVar(&advertiseAddr, "advertise-addr", ":9900", "advertise addr")
	RootCmd.PersistentFlags().StringVar(&dbpath, "dbpath", "/tmp/sandglassdb", "base directory for data storage")
	RootCmd.PersistentFlags().StringArrayVarP(&initialPeers, "initial-peers", "p", nil, "Inital peers")

	RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName(".sandglass") // name of config file (without extension)
		viper.AddConfigPath("$HOME")      // adding home directory as first search path
		viper.AutomaticEnv()              // read in environment variables that match
	}

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func loadConfigFile(cfgFile string, dest interface{}) (err error) {
	var data []byte
	_, err = url.ParseRequestURI(cfgFile)
	if err == nil {
		resp, err := http.Get(cfgFile)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		data, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
	} else {
		data, err = ioutil.ReadFile(cfgFile)
		if err != nil {
			return err
		}
	}

	return yaml.Unmarshal(data, dest)
}
