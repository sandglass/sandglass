// +build !cgo

package rocksdb

import (
	"github.com/celrenheit/sandglass/storage/badger"
)

var NewStorage = badger.NewStorage
