package sstable

import (
	"bytes"
	"github.com/peterouob/gocloud/db/config"
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
	bf              *BloomFilter
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

}

func (w *SsWriter) addIndex(key []byte) {}
