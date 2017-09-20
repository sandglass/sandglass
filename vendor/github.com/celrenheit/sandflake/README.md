# Sandflake [![Build Status](https://img.shields.io/travis/celrenheit/sandflake.svg?style=flat-square)](https://travis-ci.org/celrenheit/sandflake) [![GoDoc](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://godoc.org/github.com/celrenheit/sandflake) [![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE) [![Go Report Card](https://goreportcard.com/badge/github.com/celrenheit/sandflake?style=flat-square)](https://goreportcard.com/report/github.com/celrenheit/sandflake)

Decentralized, sequential, lexicographically sortable unique id

**This is a work in progress, things might change quickly without notice**

## Features

* 128 bit
* Lexicographically sortable
* Sequential (not guarranted for future ids)
* 1.21e+24 unique ids per millisecond
* 2.81e+14 unique ids per worker per millisecond


## Table of contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Install/Update](#installupdate)
- [Usage](#usage)
- [Composition](#composition)
- [Inspiration](#inspiration)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Install/Update

```bash
go get -u github.com/celrenheit/sandflake
```

## Usage

```go
var g sandflake.Generator
id := g.Next()
fmt.Println(id)
```

## Composition

* 48 bit: timestamp in milliseconds
* 32 bit: worker id (random at initialization)
* 24 bit: sequence number
* 24 bit: randomness

Sandflake ids do not need to wait some milliseconds for the next id if time goes backwards, it can just generate new ones and random bytes at the end should avoid any possible conflict with previous ids. In this case, the order is not guaranteed anymore. However, with Go 1.9's monotonic clock this should not be an issue anymore.

Likewise, for future manually generated ids, the order is not guaranteed.

## Inspiration

* [twitter/snowflake](https://github.com/twitter/snowflake)
* [alizan/ulid](https://github.com/alizain/ulid) and [oklog/ulid](https://github.com/oklog/ulid)

## License

Apache 2.0