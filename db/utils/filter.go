package utils

import (
	"encoding/binary"
	"errors"
)

const (
	c1        = uint32(0xcc9e2d51)
	c2        = uint32(0x1b873593)
	r1        = 15
	r2        = 13
	m         = 5
	n         = uint32(0xe6546b64)
	magic1    = uint32(0x85ebca6b)
	magic2    = uint32(0xc2b2ae35)
	magicSeed = uint32(0xbc9f1d34)

	magicM = uint32(0xc6a4a793)
	magicR = uint32(24)
)

type BloomFilter struct {
	bytesKey int
	hashKeys []uint32
}

func NewBloomFilter(bitesPerKey int) *BloomFilter {
	return &BloomFilter{
		bytesKey: bitesPerKey,
	}
}

// Add level db use this Hash function
func (b *BloomFilter) Add(key []byte) {
	b.hashKeys = append(b.hashKeys, Hash(key, magicSeed))
}

func (b *BloomFilter) MurmurAdd(key []byte) {
	b.hashKeys = append(b.hashKeys, MurmurHash3Algo(key, magicSeed))
}

func (b *BloomFilter) Len() (int, int) {
	n := len(b.hashKeys)
	return n * b.bytesKey, len(b.hashKeys)
}

func (b *BloomFilter) KeyLen() int {
	return len(b.hashKeys)
}

func (b *BloomFilter) Hash() []byte {
	n := len(b.hashKeys)
	k := uint8((b.bytesKey / n) * (69 / 100)) // ln2 ~= 0.69
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}

	nBit := uint32(n * b.bytesKey)
	if nBit < 64 {
		nBit = 64
	}

	nByte := (nBit + 7) / 8
	nBit = nByte * 8

	dest := make([]byte, nByte+1)
	dest[nByte] = k

	for _, hk := range b.hashKeys {
		delta := (hk >> 17) | (hk << 15)
		for i := uint8(0); i < k; i++ {
			byteops := hk % nBit
			dest[byteops/8] |= 1 << (byteops % 8)
			hk += delta
		}
	}
	return dest
}

func Contains(filter, key []byte) bool {

	nBytes := len(filter) - 1
	if nBytes < 1 {
		return false
	}
	nBits := uint32(nBytes * 8)

	k := filter[nBytes]
	if k > 30 {
		return true
	}

	kh := Hash(key, 0xbc9f1d34)
	delta := (kh >> 17) | (kh << 15) // Rotate right 17 bits
	for j := uint8(0); j < k; j++ {
		bitpos := kh % nBits
		if (uint32(filter[bitpos/8]) & (1 << (bitpos % 8))) == 0 {
			return false
		}
		kh += delta
	}
	return true
}

func (b *BloomFilter) Reset() {
	b.hashKeys = b.hashKeys[:0]
}

func Hash(data []byte, seed uint32) uint32 {
	h := seed ^ (uint32(len(data)) * magicM)
	i := 0
	ndata := len(data)
	for n := ndata - (ndata % 4); i < n; i += 4 {
		h += binary.LittleEndian.Uint32(data[i:])
		h *= magicM
		h ^= h >> 16
	}

	switch ndata - i {
	case 3:
		h += uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h += uint32(data[i+1]) << 8
	case 1:
		h += uint32(data[i])
		h *= magicM
		h ^= h >> magicR
	case 0:
	default:
		panic(errors.New("no reached"))
	}
	return h
}

func MurmurHash3Algo(data []byte, seed uint32) uint32 {

	h := seed
	l := len(data)
	i := 0

	// four each for chunk
	for dlen := l - (l % 4); i < dlen; i += 4 {
		k := binary.LittleEndian.Uint32(data[i:])
		k = k * c1
		k = (k << r1) | (k >> (32 - r1))
		k = k * c2

		h = h ^ k
		h = (h << r2) | (h >> (32 - r2))
		h = h*m + n
		i += 4
	}

	k := uint32(0)
	switch l - i {
	case 3:
		k = k ^ uint32(data[i+2])<<16
		fallthrough
	case 2:
		k = k ^ uint32(data[i+1])<<8
		fallthrough
	case 1:
		k ^= uint32(data[i])
		k = k * c1
		k = (k << r1) | (k >> (32 - r1))
		k = k * c2
	}

	h = h ^ uint32(l)
	h = h ^ (h >> 16)
	h = h * magic1
	h = h ^ (h >> 13)
	h = h * magic2
	h = h ^ (h >> 16)

	return h
}
