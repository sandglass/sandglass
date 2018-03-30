package sgutils

import "os"

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
