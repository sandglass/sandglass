package sgutils

import (
	"os"

	jump "github.com/dgryski/go-jump"
	"github.com/spaolacci/murmur3"
)

func MkdirIfNotExist(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func StringSliceHasString(slice []string, key string) bool {
	for _, item := range slice {
		if item == key {
			return true
		}
	}

	return false
}

func CopyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func Hash(key []byte, numBuckets int) int {
	hash := murmur3.New64()
	hash.Write(key)

	return int(jump.Hash(hash.Sum64(), numBuckets))
}

func HashString(key string, numBuckets int) int {
	hash := murmur3.New64()
	hash.Write([]byte(key))

	return int(jump.Hash(hash.Sum64(), numBuckets))
}
