package sstable

import "encoding/binary"

type BloomFilter struct {
	bitsPerKey int
	hashKeys   []uint32
}

func NewBloomFilter(bitesPerKey int) *BloomFilter {
	return &BloomFilter{
		bitsPerKey: bitesPerKey,
	}
}

func (b *BloomFilter) Add(key []byte) {
	b.hashKeys = append(b.hashKeys)
}

func (b *BloomFilter) Len() (uint32, uint8) {
	n := len(b.hashKeys)
	k := uint8(b.bitsPerKey * 69 / (100 * n))
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}

	nBytes := uint32(n * b.bitsPerKey)
	if nBytes < 64 {
		nBytes = 64
	}
	return nBytes, k
}

func (b *BloomFilter) Hash() []byte {
	nBytes, k := b.Len()

	nBits := (nBytes + 7) / 8
	nBytes = nBits * 8

	dest := make([]byte, nBits+1)
	dest[nBits] = k

	for _, h := range b.hashKeys {
		delta := (h >> 17) | (h << 15)
		for i := uint8(0); i < k; i++ {
			byteops := h % nBytes
			dest[byteops/8] = dest[byteops/8] | (1 << (byteops % 8))
			h += delta
		}
	}
	return dest
}

func (b *BloomFilter) Reset() {
	b.hashKeys = b.hashKeys[:0]
}

func MurmurHash3Algo(data []byte, seed uint32) uint32 {
	const (
		c1     = 0xcc932d51
		c2     = 0x1b873593
		r1     = 15
		r2     = 13
		m      = 5
		n      = 0xe6546b64
		magic1 = 0x85ebca6b
		magic2 = 0xc2b2ae35
	)

	h := seed
	l := len(data)
	i := 0

	// four each for chunk
	for i+4 < l {
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
