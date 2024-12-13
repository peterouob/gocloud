package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/utils"
	"os"
)

type SsWriter struct {
	conf            *config.Config
	fd              *os.File
	dataBuf         *bytes.Buffer
	fileBuf         *bytes.Buffer
	indexBuf        *bytes.Buffer
	index           []*Index
	filter          map[uint64][]byte
	bf              *utils.BloomFilter
	dataBlock       *Block
	filterBlock     *Block
	indexBlock      *Block
	indexScratch    [20]byte
	prevKey         []byte
	prevBlockOffset uint64
	prevBlockSize   uint64
}

type Index struct {
	Key    []byte
	Offset uint64
	Size   uint64
}

func (w *SsWriter) Append(key, value []byte) {
	if w.dataBlock.n == 0 {
		skey := make([]byte, len(key))
		copy(skey, key)
		w.addIndex(skey)
	}

	w.dataBlock.Append(key, value)
	w.bf.Add(key)
	w.prevKey = key

	if w.dataBlock.Len() > w.conf.SstDataBlockSize {
		w.flushBlock()
	}
}

func (w *SsWriter) addIndex(key []byte) {
	n := binary.PutUvarint(w.indexScratch[0:], w.prevBlockOffset)
	n += binary.PutUvarint(w.indexScratch[n:], w.prevBlockSize)
	separator := GetSeparator(w.prevKey, key)
	w.indexBlock.Append(separator, w.indexScratch[:n])
	w.index = append(w.index, &Index{
		Key:    separator,
		Offset: w.prevBlockOffset,
		Size:   w.prevBlockSize,
	})
}

func (w *SsWriter) flushBlock() {}

func GetSeparator(a, b []byte) []byte {}
