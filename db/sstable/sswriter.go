package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/utils"
	"os"
	"path"
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

func NewSStWriter(file string, conf *config.Config) (*SsWriter, error) {
	fd, err := os.OpenFile(path.Join(conf.Dir, file), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("create file error :" + err.Error())
	}
	return &SsWriter{
		conf:         conf,
		fd:           fd,
		dataBuf:      bytes.NewBuffer(make([]byte, 0)),
		fileBuf:      bytes.NewBuffer(make([]byte, 0)),
		indexBuf:     bytes.NewBuffer(make([]byte, 0)),
		index:        make([]*Index, 0),
		filter:       make(map[uint64][]byte),
		bf:           utils.NewBloomFilter(10),
		dataBlock:    NewBlock(conf),
		filterBlock:  NewBlock(conf),
		indexBlock:   NewBlock(conf),
		indexScratch: [20]byte{},
		prevKey:      make([]byte, 0),
	}, nil
}

type Index struct {
	Key        []byte
	PrevOffset uint64
	PrevSize   uint64
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
		Key:        separator,
		PrevOffset: w.prevBlockOffset,
		PrevSize:   w.prevBlockSize,
	})
}

func (w *SsWriter) flushBlock() {
	w.prevBlockOffset = uint64(w.dataBuf.Len())
	n := binary.PutUvarint(w.indexScratch[0:], w.prevBlockOffset)

	filter := w.bf.Hash()
	w.filter[w.prevBlockOffset] = filter
	w.filterBlock.Append(w.indexScratch[:n], filter)
	w.bf.Reset()
	var err error
	w.prevBlockSize, err = w.dataBlock.FlushBlockTo(w.dataBuf)
	if err != nil {
		panic(errors.New("error in flush block to w.dataBuf"))
	}
}

func (w *SsWriter) Finish() (int64, map[uint64][]byte, []*Index) {
	if w.bf.KeyLen() > 0 {
		w.flushBlock()
	}

	if _, err := w.filterBlock.FlushBlockTo(w.fileBuf); err != nil {
		panic(errors.New("error in flush filter block"))
	}
	w.addIndex(w.prevKey)
	if _, err := w.indexBlock.FlushBlockTo(w.indexBuf); err != nil {
		panic(errors.New("error in flush index block"))
	}

	footer := make([]byte, w.conf.SstFooterSize)
	size := w.dataBuf.Len()

	n := binary.PutUvarint(footer[0:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.fileBuf.Len()))
	size += w.fileBuf.Len()
	n += binary.PutUvarint(footer[n:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.indexBuf.Len()))
	size += w.indexBuf.Len()
	size += w.conf.SstFooterSize

	w.fd.Write(w.dataBuf.Bytes())
	w.fd.Write(w.fileBuf.Bytes())
	w.fd.Write(w.indexBuf.Bytes())
	w.fd.Write(footer)

	return int64(size), w.filter, w.index
}

func GetSeparator(a, b []byte) []byte {
	if len(a) == 0 {
		n := len(b) - 1
		c := b[n] - 1
		return append(b[0:n], c)
	}

	n := countPrefix(a, b)
	if n == 0 || n == len(a) {
		return a
	} else {
		c := a[n] + 1
		return append(a[0:n], c)
	}
}

func (w *SsWriter) Size() int {
	return w.dataBuf.Len()
}

func (w *SsWriter) Close() {
	if err := w.fd.Close(); err != nil {
		panic(errors.New("error in close w.fd : " + err.Error()))
	}
	w.dataBuf.Reset()
	w.indexBuf.Reset()
}
