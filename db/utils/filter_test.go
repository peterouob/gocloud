package utils

import (
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	bitsPerKey := 10
	bf := NewBloomFilter(bitsPerKey)

	if bf == nil {
		t.Error("NewBloomFilter returned nil")
	}

	if bf.bytesKey != bitsPerKey {
		t.Errorf("Expected bytesKey to be %d, got %d", bitsPerKey, bf.bytesKey)
	}

	if len(bf.hashKeys) != 0 {
		t.Error("New BloomFilter should have empty hashKeys")
	}
}

func TestBloomFilterAdd(t *testing.T) {
	bf := NewBloomFilter(10)

	testKeys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("test"),
	}

	for _, key := range testKeys {
		bf.MurmurAdd(key)
	}

	if len(bf.hashKeys) != len(testKeys) {
		t.Errorf("Expected %d hash keys, got %d", len(testKeys), len(bf.hashKeys))
	}
}

func TestBloomFilterMurmurHash(t *testing.T) {
	bf := NewBloomFilter(10)

	testKeys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("test"),
	}

	for _, key := range testKeys {
		hash := MurmurHash3Algo(key, 0)
		bf.hashKeys = append(bf.hashKeys, hash)
	}

	hashResult := bf.Hash()

	if len(hashResult) == 0 {
		t.Error("Hash() returned empty result")
	}

	// Check that the last byte represents the number of hash functions (k)
	k := hashResult[len(hashResult)-1]
	if k < 1 || k > 30 {
		t.Errorf("Invalid number of hash functions: %d", k)
	}
}

// Benchmark the hash function
func BenchmarkHash(b *testing.B) {
	testData := []byte("benchmark test data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Hash(testData, 0)
	}
}

func TestBloomFilterHashAdd(t *testing.T) {
	bf := NewBloomFilter(10)

	testKeys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("test"),
	}

	for _, key := range testKeys {
		bf.Add(key)
	}

	if len(bf.hashKeys) != len(testKeys) {
		t.Errorf("Expected %d hash keys, got %d", len(testKeys), len(bf.hashKeys))
	}
}

func TestBloomFilterHash(t *testing.T) {
	bf := NewBloomFilter(10)

	testKeys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("test"),
	}

	for _, key := range testKeys {
		hash := Hash(key, 0)
		bf.hashKeys = append(bf.hashKeys, hash)
	}

	hashResult := bf.Hash()

	if len(hashResult) == 0 {
		t.Error("Hash() returned empty result")
	}

	// Check that the last byte represents the number of hash functions (k)
	k := hashResult[len(hashResult)-1]
	if k < 1 || k > 30 {
		t.Errorf("Invalid number of hash functions: %d", k)
	}
}

// Benchmark the hash function
func BenchmarkMurmurHash3Algo(b *testing.B) {
	testData := []byte("benchmark test data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Hash(testData, 0)
	}
}
