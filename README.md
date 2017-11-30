<p align="center">
  <img alt="Sandglass Logo" src="https://raw.githubusercontent.com/celrenheit/gifs/master/sandglass/brand%2Blogo-sm.png"  />
</p>
<p align="center">
Sandglass is a distributed, horizontally scalable, persistent, time ordered message queue. It was developed to support asynchronous tasks and message scheduling which makes it suitable for usage as a task queue.
</p>

<p align="center">
    <a href="https://github.com/celrenheit/sandglass/releases/latest">
        <img alt="Release" src="https://img.shields.io/github/release/celrenheit/sandglass.svg?style=flat-square">
    </a>
    <a href="https://travis-ci.org/celrenheit/sandglass">
        <img alt="Build Status" src="https://img.shields.io/travis/celrenheit/sandglass.svg?style=flat-square">
    </a>
    <a href="LICENSE">
        <img alt="License" src="https://img.shields.io/badge/license-apache-blue.svg?style=flat-square">
    </a>
</p>

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
## Table of contents

- [Features](#features)
- [Installation](#installation)
- [Getting started](#getting-started)
- [Clients](#clients)
  - [Go](#go)
    - [Installation](#installation-1)
    - [Documentation](#documentation)
    - [Usage](#usage)
  - [Java, Python, Node.js, Ruby](#java-python-nodejs-ruby)
- [Architecture](#architecture)
  - [General](#general)
  - [Topics](#topics)
- [Contributing](#contributing)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Features

* Horizontal scalability
* Highly available
* Persistent storage
* Time ordered
* Multiple consumers per group for a partition
* Produce message to be consumed in the future
* Acknowledge/NotAcknowledge each message individualy
* Automatic redelivery and commit offset tracking
* Language agnostic

**EXPERIMENTAL**: This is a prototype of a side project. This should not be used in production in its current form as things may change quickly without notice.

## Installation

On MacOS using [Homebrew](https://brew.sh):
```shell
brew install celrenheit/taps/sandglass
```

For other platforms, you can grab binaries [here](https://github.com/celrenheit/sandglass/releases). 

## Getting started

> NOTE: All data will be stored in /tmp/node1. If you wish to change this, copy `demo/node1.yaml` and modify it accordingly.

First, let's launch sandglass server:

```shell
sandglass --config https://raw.githubusercontent.com/celrenheit/sandglass/master/demo/node1.yaml --offset_replication_factor 1
```

In a second terminal window, create a _emails_ topic:

```shell
sandctl topics create emails --num_partitions 3 --replication_factor 1
```

...produce 10,000 messages:

```shell
sandctl produce emails '{"dest" : "hi@example.com"}' -n 10000
```

...and consume from the _emails_ topic:

```shell
sandctl consume emails
```

(or if you wish to watch you can use `sandctl consume -f emails` to see messages coming live)

> We are using a single node cluster, this is not recommended for production.

Add a second node to the cluster:
```shell
$ sandglass --config https://raw.githubusercontent.com/celrenheit/sandglass/master/demo/node2.yaml
```

and repeat the same steps described above for another topic and increasing the replication factor to 2.

## Clients

### Go

#### Installation

```shell
go get -u github.com/celrenheit/sandglass-client/go/sg
```

#### Documentation

The documentation is available on [godoc](https://godoc.org/github.com/celrenheit/sandglass-client/go/sg).


#### Usage

* Producer

```go
// Let's first create a client by providing adresses of nodes in the sandglass cluster
client, err := sg.NewClient(
    sg.WithAddresses(":7170"),
)
if err != nil {
    panic(err)
}
defer client.Close()

// Now we produce a new message
// Notice the empty string "" in the 3th argument, meaning let sandglass choose a random partition
err := client.Produce(context.Background(), "payments", "", &sgproto.Message{
    Value: []byte("Hello, Sandglass!"),
})
if err != nil {
    panic(err)
}
```

* Consumer

```go
// Let's first create a client by providing adresses of nodes in the sandglass cluster
client, err := sg.NewClient(
    sg.WithAddresses(":7170"),
)
if err != nil {
    panic(err)
}
defer client.Close()


// Listing partitions in order to choose one to consume from
partitions, err := c.ListPartitions(context.Background(), topic)
if err != nil {
    panic(err)
}

// we are choosing only one partition for demo purposes
partition := partitions[0]

// Create a new consumer using consumer group payments-sender and consumer name consumer1
consumer := c.NewConsumer(topic, partition, "payments-sender", "consumer1")

// and consume messages
msgCh, err := consumer.Consume(context.Background())
if err != nil {
    panic(err)
}

for msg := range msgCh {
    // do an amazing amount of work
    log.Printf("received: %s\n", string(msg.Value))

    // when we are done, we Acknowledge the message
    // but we can also NotAcknowledge to trigger a redelivery of the message
    if err := consumer.Acknowledge(context.Background(), msg); err!=nil {
        panic(err)
    }
}
```

### Java, Python, Node.js, Ruby

Interested in having client for one the following languages ? 

Support is planned but there is no specific schedule. So, if you are interested to quickly have a client in your language, **help is welcome!**

Check the raw generated code available on https://github.com/celrenheit/sandglass-grpc and feel free to submit your through a pull request to https://github.com/celrenheit/sandglass-client.


## Architecture

### General

```
                                                                  +-----------------+
                                                                  |                 |
                          +--------------------------+     +------>  Consumer       |
                          |                          |     |      |                 |
                          |    Sandglass Cluster     |     |      +-----------------+
                          |                          |     |
                          |                          +-----+  Round robin consumption
+-----------------+       |   +------------------+   |     |
|                 |       |   |                  |   |     |      +-----------------+
|  Producer       +------->   |                  |   |     |      |                 |
|                 |       |   |    Broker 1      |   |     +------>  Consumer       |
+-----------------+       |   |                  |   |            |                 |
                          |   |                  |   |            +-----------------+
                          |   +------------------+   |
+-----------------+       |                          |
|                 |       |   +------------------+   |
|  Producer       +------->   |                  |   |            +-----------------+
|                 |       |   |                  |   |            |                 |
+-----------------+       |   |    Broker 2      |   +-----+------>  Consumer       |
                          |   |                  |   |     |      |                 |
                          |   |                  |   |     |      +-----------------+
+-----------------+       |   +------------------+   |     |
|                 |       |                          |     |  Failover consumption
|  Producer       +------->   +------------------+   |     |     (NOT DONE YET)
|                 |       |   |                  |   |     |
+-----------------+       |   |                  |   |     |      +-----------------+
                          |   |    Broker 3      |   |     |      |                 |
                          |   |                  |   |     +------+  Consumer       |
                          |   |                  |   |            |                 |
                          |   +------------------+   |            +-----------------+
                          |                          |
                          |                          |
                          +--------------------------+
```


### Topics

There is two kinds of topics:
* Timer:
   * Fixed number of partitions (set up-front, could change)
   * Time ordered using [sandflake IDs](https://github.com/celrenheit/sandflake)
   * Can produce messages in the future

* KV:
   * Fixed number of partitions (set up-front, cannot change)
   * Behaves like a distributed key value store


A topic has a number of partitions.
Data is written into a single partition. Either the destination partition is specified by the producer. Otherwise, we fallback to choosing the destination partition using a consistent hashing algorithm.

Each produced message to a partition writes a message to a Write Ahead Log (WAL) and to the message log.
The WAL is used for the replication logic, it is sorted in the order each message was produced.
The message log is used for message comsumption, it is mainly sorted by time (please refer to [sandflake ids](https://github.com/celrenheit/sandflake) for the exact composition)

The content of the message is stored in the message log and not in the WAL (only the keys are important). This way the message log is used for fast consumption avoiding random reads. 

This will probably change in order to have the WAL as the only source of truth instead of storing the content in the message log. This of course will have an impact because we are transfering random reads to the consumption path. Utlimately, we are going to have to store the content of the message in both logs for better performance at the cost of disk space.


A message is composed of the following fields:

        topic
        partition

        index   <- position in the WAL

        offset  <- position in the message log for timer topics
        key     <- position in the message log for key for kv topics

        value   <- your payload

## Contributing

Want to contribute to Sandglass ? Awesome! Feel free to submit an issue or a pull request.

Here are some ways you can help:

* Report bugs
* Your language is not supported ? Feel free to [build a client](#java-python-nodejs-ruby). Trust me, it should not take you long :)
* Improve code/documentation
* Propose new features
* and more...

## License

This project is licensed under the Apache License 2.0 available [here](LICENSE).

