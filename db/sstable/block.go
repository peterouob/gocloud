package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/snappy"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/utils"
	"io"
)

type Block struct {
	conf        *config.Config
	header      [30]byte
	records     *bytes.Buffer
	trailers    *bytes.Buffer
	n           int
	prvKey      []byte
	compression []byte //壓縮
}

func NewBlock(conf *config.Config) *Block {
	return &Block{
		conf:        conf,
		header:      [30]byte{},
		records:     bytes.NewBuffer(make([]byte, 0)),
		trailers:    bytes.NewBuffer(make([]byte, 0)),
		n:           0,
		prvKey:      make([]byte, 0),
		compression: make([]byte, 0),
	}
}

func (b *Block) Append(key, value []byte) {
	// Calculate shared prefix with previous key
	klen := len(key)
	vlen := len(value)
	nprefix := 0

	// Add restart point if needed
	if b.n%b.conf.SstRestartInterval == 0 {
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(b.records.Len()))
		b.trailers.Write(buf4)
		nprefix = 0 // Force full key at restart points
	} else {
		nprefix = SharedPrefixLen(b.prvKey, key)
	}

	// Write header (lengths)
	n := binary.PutUvarint(b.header[0:], uint64(nprefix))
	n += binary.PutUvarint(b.header[n:], uint64(klen-nprefix))
	n += binary.PutUvarint(b.header[n:], uint64(vlen))

	// Write record
	if _, err := b.records.Write(b.header[:n]); err != nil {
		panic(err)
	}
	if _, err := b.records.Write(key[nprefix:]); err != nil {
		panic(err)
	}
	if _, err := b.records.Write(value); err != nil {
		panic(err)
	}

	// Update state
	b.prvKey = append(b.prvKey[:0], key...)
	b.n++
}

func (b *Block) compress() []byte {
	// Add final restart point count
	buf4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf4, uint32(b.trailers.Len()/4))
	b.trailers.Write(buf4)

	// Combine all data
	totalSize := b.records.Len() + b.trailers.Len()
	rawData := make([]byte, totalSize)
	copy(rawData, b.records.Bytes())
	copy(rawData[b.records.Len():], b.trailers.Bytes())

	// Compress the data
	compressed := snappy.Encode(nil, rawData)

	// Add CRC
	result := make([]byte, len(compressed)+4)
	copy(result, compressed)

	// Calculate and append checksum
	crc := utils.CompressedCheckSum(compressed)
	binary.LittleEndian.PutUint32(result[len(compressed):], crc)

	return result
}

func (b *Block) FlushBlockTo(w io.Writer) (uint64, error) {
	compressed := b.compress()
	if compressed == nil {
		return 0, errors.New("compression failed")
	}

	written, err := w.Write(compressed)
	if err != nil {
		return 0, err
	}

	b.clear()
	return uint64(written), nil
}

func (b *Block) clear() {
	b.n = 0
	b.prvKey = b.prvKey[:0]
	b.records.Reset()
	b.trailers.Reset()
}

func (b *Block) Len() int {
	return b.records.Len() + b.trailers.Len() + 4
}

func (b *Block) Size() int {
	return b.records.Len() + b.trailers.Len() + 4
}
