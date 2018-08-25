// +build !cgo

package rocksdb

import (
	"github.com/sandglass/sandglass/storage/badger"
)

var NewStorage = badger.NewStorage
