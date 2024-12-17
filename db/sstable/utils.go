package sstable

import "fmt"

func formatName(level, seqNo int, extra string) string {
	return fmt.Sprintf("%d_%d_%s.sst", level, seqNo, extra)
}

func formatKeyValue[K any, V any](key K, value V) ([]byte, []byte) {
	var bKey []byte
	var bValue []byte

	switch v := any(key).(type) {
	case string:
		bKey = []byte(v)
	case []byte:
		bKey = v
	default:
		panic("Unsupported key type")
	}

	switch v := any(value).(type) {
	case string:
		bValue = []byte(v)
	case []byte:
		bValue = v
	default:
		panic("Unsupported value type")
	}
	return bKey, bValue
}
