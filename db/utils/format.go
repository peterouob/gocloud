package utils

import (
	"fmt"
	"log"
)

func FormatKeyValue2Byte[K any, V any](key K, value V) ([]byte, []byte) {
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

func FormatKeyValue(k interface{}, v interface{}) ([]byte, []byte) {
	var keyBytes []byte
	var valueBytes []byte

	switch k := k.(type) {
	case int:
		keyBytes = []byte(fmt.Sprintf("%d", k))
	case string:
		keyBytes = []byte(k)
	case []byte:
		keyBytes = k
	default:
		panic("Unsupported key type")
	}

	switch v := v.(type) {
	case int:
		valueBytes = []byte(fmt.Sprintf("%d", v))
	case string:
		valueBytes = []byte(v)
	case []byte:
		valueBytes = v
	default:
		panic("Unsupported value type")
	}

	return keyBytes, valueBytes
}

func FormatKeyV(k interface{}) []byte {
	var keyBytes []byte

	switch k := k.(type) {
	case int:
		keyBytes = []byte(fmt.Sprintf("%d", k))
	case string:
		keyBytes = []byte(k)
	case []byte:
		keyBytes = k
	default:
		panic("Unsupported key type")
	}
	log.Println("keyBytes:", keyBytes)
	return keyBytes
}

func FormatName(level, seqNo int, extra string) string {
	return fmt.Sprintf("%d_%d_%s.sst", level, seqNo, extra)
}
