# Sandglass [![Build Status](https://img.shields.io/travis/celrenheit/sandglass.svg?style=flat-square)](https://travis-ci.org/celrenheit/sandglass) [![GoDoc](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://godoc.org/github.com/celrenheit/sandglass) [![License](https://img.shields.io/badge/license-apache-blue.svg?style=flat-square)](LICENSE) [![Go Report Card](https://goreportcard.com/badge/github.com/celrenheit/sandglass?style=flat-square)](https://goreportcard.com/report/github.com/celrenheit/sandglass)

Sandglass is a distributed, horizontally scalable, persistent, delayed message queue. It was developped to support asynchronous tasks. It supports synchronous tasks as well. It supports the competing consumers pattern.

## Features

* Horizontal scalability
* Highly available
* Persistent storage
* Roughly strong ordering with a single consumer in a consumer group
* Round robin consumption between multiple consumers in a consumer group (looses ordering)
* Produce message to be consumed in the future
* Acknowledge each message individualy
* Automatic consumer offset tracking

## Project status

**EXPERIMENTAL**: This is a prototype. This should not be used in production in its current form.

See TODO section below for more information

## Installation

As of now there is no binaries available, you can only install from source using:

```shell
$ go get -u github.com/celrenheit/sandglass/cmd/sandglass
```

## Usage

Open a first terminal window:

```shell
$ sandglass --config https://raw.githubusercontent.com/celrenheit/sandglass/master/demo/node1.yml
```

On a second terminal window:

```shell
$ sandglass --config https://raw.githubusercontent.com/celrenheit/sandglass/master/demo/node2.yml
```

## TODO

* Clean up all the mess
* Fix replication and re assign partitions correctly when a node goes down
* Save all the registered nodes and not rely on gossip to allow topic creation even if there is not enough nodes
* More TODOs in TODO section (#inception)
* Make everything more robust...