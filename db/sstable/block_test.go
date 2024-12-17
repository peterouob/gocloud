package sstable

import (
	"encoding/binary"
	"github.com/peterouob/gocloud/db/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockAppend(t *testing.T) {
	block := NewBlock(config.NewConfig(dir))

	key1 := []byte("testkey1")
	value1 := []byte("testvalue1")

	block.Append(key1, value1)

	assert.Equal(t, 1, block.n, "Block record count should increment")
	assert.Equal(t, key1, block.prvKey, "Previous key should be updated")

}

func TestPrefixCompression(t *testing.T) {
	block := NewBlock(config.NewConfig(dir))

	key1 := []byte("hello/world")
	key2 := []byte("hello/test")
	value1 := []byte("value1")
	value2 := []byte("value2")

	block.Append(key1, value1)
	block.Append(key2, value2)

	recordBuffer := block.records.Bytes()

	var prefixLen uint64
	prefixLen, _ = binary.Uvarint(recordBuffer[0:])
	assert.Equal(t, uint64(0), prefixLen, "First entry should have no prefix")

	prefixOffset := len(recordBuffer[0:prefixLen]) + len(key1) + len(value1)
	prefixLen, _ = binary.Uvarint(recordBuffer[prefixOffset:])
	assert.Greater(t, prefixLen, uint64(0), "Subsequent entries should use prefix compression")
}

func TestMultipleAppends(t *testing.T) {
	block := NewBlock(config.NewConfig(dir))

	testCases := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("user/1"), []byte("John")},
		{[]byte("user/2"), []byte("Alice")},
		{[]byte("product/1"), []byte("Laptop")},
		{[]byte("product/2"), []byte("Phone")},
	}

	for _, tc := range testCases {
		block.Append(tc.key, tc.value)
	}

	assert.Equal(t, 4, block.n, "Should append multiple entries")
}

func BenchmarkBlockAppend(b *testing.B) {
	block := NewBlock(config.NewConfig(dir))
	key := []byte("benchmarkkey")
	value := []byte("benchmarkvalue")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block.Append(key, value)
	}
}
