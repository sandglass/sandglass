package sgutils

import (
	"fmt"
	"os"

	"github.com/celrenheit/sandglass-grpc/go/sgproto"
	"github.com/gogo/protobuf/proto"
)

func TestETCDAddr() string {
	addr := "localhost:2379"
	if AmIInDockerContainer() {
		addr = "etcd:4001"
	}

	return addr
}

func AmIInDockerContainer() bool {
	_, err := os.Stat("/.dockerenv")
	return !os.IsNotExist(err)
}

func DebugPartitionStorageKey(b []byte) {
	if len(b) <= 3 {
		return
	}

	// TODO: handle wal messages
	if b[0] != 1 || b[1] != 'v' || b[2] != '/' {
		return
	}

	b = b[3:] // 1v/
	var key sgproto.MarkedOffsetStorageKey
	err := proto.Unmarshal(b, &key)
	if err != nil {
		panic(err)
	}

	fmt.Printf("key: %+v\n", key)
}
