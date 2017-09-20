package sgutils

import "os"

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
