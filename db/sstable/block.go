package sstable

import (
	"bytes"
	"encoding/binary"
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

func (b *Block) Append(key, value []byte) {
	klen := len(key)
	vlen := len(value)
	nprefix := 0

	if b.n%b.conf.SstRestartInterval == 0 {
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(b.records.Len()))
		b.trailers.Write(buf4)
	} else {
		nprefix = countPrefix(b.prvKey, key)
	}

	n := binary.PutUvarint(b.header[0:], uint64(nprefix))
	n += binary.PutUvarint(b.header[n:], uint64(klen-nprefix))
	n += binary.PutUvarint(b.header[n:], uint64(vlen))

	b.records.Write(b.header[:n])
	b.records.Write(key[nprefix:])
	b.records.Write(value)

	b.prvKey = append(b.prvKey[:0], key...)
	b.n++
}

func (b *Block) compress() []byte {
	buf4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf4, uint32(b.trailers.Len())/4)
	b.trailers.Write(buf4)

	b.records.Write(b.trailers.Bytes())

	n := snappy.MaxEncodedLen(b.records.Len())
	if n > len(b.compression) {
		b.compression = make([]byte, n+b.conf.SstBlockTrailerSize)
	}

	compressed := snappy.Encode(b.compression, b.records.Bytes())
	crc := utils.CompressedCheckSum(compressed)
	size := len(compressed)
	compressed = compressed[:size+b.conf.SstBlockTrailerSize]
	binary.LittleEndian.PutUint32(compressed[:size], crc)

	return compressed
}

func (b *Block) FlushBlockTo(w io.Writer) (uint64, error) {
	defer b.clear()
	n, err := w.Write(b.compression)
	return uint64(n), err
}

func (b *Block) clear() {
	b.n = 0
	b.prvKey = b.prvKey[:0]
	b.records.Reset()
	b.trailers.Reset()
}

func countPrefix(a, b []byte) int {
	i := 0
	j := max(len(a), len(b))
	for i < j && a[i] == b[i] {
		i++
	}
	return i
}
