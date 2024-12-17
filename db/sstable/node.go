package sstable

import (
	"bytes"
	"errors"
	"github.com/peterouob/gocloud/db/config"
	"io"
	"sync"
)

type RawNodeData struct {
	Level  int
	SeqNo  int
	Offset int64
	Data   []byte
	Done   bool
	Err    error
}

type Node struct {
	wg         sync.WaitGroup
	sr         *SStReader
	filter     map[uint64][]byte
	startKey   []byte
	endKey     []byte
	index      []*Index
	Level      int
	SeqNo      int
	Extra      string
	FileSize   int64
	compacting bool

	curBlock int
	curBuf   *bytes.Buffer
	prevKey  []byte
}

func NewNode(filter map[uint64][]byte, index []*Index, level, seqNo int, extra string, fileSize int64, conf *config.Config, file string) (*Node, error) {
	r, err := NewSStReader(file, conf)
	if err != nil {
		return nil, errors.New("error in new ssReader : " + err.Error())
	}
	return &Node{
		sr:       r,
		filter:   filter,
		index:    index,
		startKey: index[0].Key,
		endKey:   index[len(index)-1].Key,
		Level:    level,
		SeqNo:    seqNo,
		Extra:    extra,
		FileSize: fileSize,
		curBlock: 1,
	}, nil
}

func (n *Node) nextRecord() ([]byte, []byte) {
	if n.curBuf == nil {
		if n.curBlock > len(n.index)-1 {
			return nil, nil
		}

		data, err := n.sr.readBlock(int64(n.index[n.curBlock].PrevOffset), int64(n.index[n.curBlock].PrevSize))
		if err != nil {
			if err != io.EOF {
				panic(errors.New("error in readBlock EOF : " + err.Error()))
			}
			return nil, nil
		}

		record, _ := DecodeBlock(data)
		n.curBuf = bytes.NewBuffer(record)
		n.prevKey = make([]byte, 0)
		n.curBlock++
	}

	key, value, err := ReadRecord(n.prevKey, n.curBuf)
	if err == nil {
		n.prevKey = key
		return key, value
	}

	if err != io.EOF {
		panic(errors.New("read record error : " + err.Error()))
		return nil, nil
	}
	n.curBuf = nil
	return n.nextRecord()
}

func (n *Node) destroy() {
	n.wg.Wait()
	n.sr.Destroy()
	n.Level = -1
	n.filter = nil
	n.index = nil
	n.curBuf = nil
	n.prevKey = nil
	n.FileSize = 0
}
