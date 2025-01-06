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

type BlockInterface interface {
	Append([]byte, []byte)
	FlushBlockTo(io.Writer) (uint64, error)
	Len() int
	Size() int
}

type Block struct {
	conf        *config.Config
	header      [30]byte
	records     *bytes.Buffer
	trailers    *bytes.Buffer
	n           int
	prvKey      []byte
	compression []byte //壓縮
}

var _ BlockInterface = (*Block)(nil)

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
	klen := len(key)
	vlen := len(value)
	nprefix := 0

	if b.n%b.conf.SstRestartInterval == 0 {
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(b.records.Len()))
		b.trailers.Write(buf4)
		nprefix = 0 // Force full key at restart points
	} else {
		nprefix = SharedPrefixLen(b.prvKey, key)
	}

	n := binary.PutUvarint(b.header[0:], uint64(nprefix))
	n += binary.PutUvarint(b.header[n:], uint64(klen-nprefix))
	n += binary.PutUvarint(b.header[n:], uint64(vlen))

	if _, err := b.records.Write(b.header[:n]); err != nil {
		panic(err)
	}
	if _, err := b.records.Write(key[nprefix:]); err != nil {
		panic(err)
	}
	if _, err := b.records.Write(value); err != nil {
		panic(err)
	}

	b.prvKey = append(b.prvKey[:0], key...)
	b.n++
}

func (b *Block) compress() []byte {
	buf4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf4, uint32(b.trailers.Len()/4))
	b.trailers.Write(buf4)

	totalSize := b.records.Len() + b.trailers.Len()
	rawData := make([]byte, totalSize)
	copy(rawData, b.records.Bytes())
	copy(rawData[b.records.Len():], b.trailers.Bytes())

	compressed := snappy.Encode(nil, rawData)

	result := make([]byte, len(compressed)+4)
	copy(result, compressed)

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
