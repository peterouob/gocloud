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

func TestBloomFilterReset(t *testing.T) {
	bf := NewBloomFilter(10)

	testKeys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("test"),
	}

	for _, key := range testKeys {
		bf.Add(key)
	}

	bf.Reset()

	if len(bf.hashKeys) != 0 {
		t.Error("Reset() did not clear hashKeys")
	}
}

func TestMurmurHash3Algo(t *testing.T) {
	testCases := []struct {
		input    []byte
		seed     uint32
		expected uint32
	}{
		{[]byte("hello"), 0, 0x248bae4f},
		{[]byte("world"), 42, 0xf3ddc412},
		{[]byte("test"), 100, 0xab7b7e23},
		{[]byte{}, 0, 0},
	}

	for _, tc := range testCases {
		t.Run(string(tc.input), func(t *testing.T) {
			result := MurmurHash3Algo(tc.input, tc.seed)

			if result != tc.expected {
				t.Errorf("MurmurHash3Algo(%v, %d): expected %x, got %x",
					tc.input, tc.seed, tc.expected, result)
			}
		})
	}
}

// Benchmark the hash function
func BenchmarkMurmurHash3Algo(b *testing.B) {
	testData := []byte("benchmark test data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MurmurHash3Algo(testData, 0)
	}
}
