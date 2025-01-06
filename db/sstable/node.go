package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/utils"
	"io"
	"log"
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

		data, err := n.sr.ReadBlock(n.index[n.curBlock].PrevOffset, n.index[n.curBlock].PrevSize)
		if err != nil {
			if err != io.EOF {
				panic(errors.New("error in readBlock EOF : " + err.Error()))
			}
			return nil, nil
		}

		record, _, _ := DecodeBlock(data)
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
		panic(errors.New("read records error : " + err.Error()))
		return nil, nil
	}
	n.curBuf = nil
	return n.nextRecord()
}

func (n *Node) Get(key []byte) ([]byte, error) {
	if bytes.Compare(key, n.startKey) < 0 || bytes.Compare(key, n.endKey) > 0 {
		log.Printf("Key %v out of range: startKey %v, endKey %v", key, n.startKey, n.endKey)
		return nil, nil
	}

	//TODO:key cannot get value
	log.Println("Get from record :", string(key))
	for _, index := range n.index[1:] {
		f := n.filter[index.PrevOffset]
		if !utils.Contains(f, key) {
			continue
		}
		if bytes.Compare(key, index.Key) <= 0 {
			data, err := n.sr.readBlock(int64(index.PrevOffset), int64(index.PrevSize))
			if err != nil {
				if err != io.EOF {
					return nil, fmt.Errorf("%d stage %d node, read records error %v", n.Level, n.SeqNo, err)
				}
				return nil, errors.New("error in readBlock EOF : " + err.Error())
			}
			record, restartPoint, err := DecodeBlock(data)
			log.Println("record", string(record))
			if err != nil {
				return nil, fmt.Errorf("%d stage %d node, read records error %v", n.Level, n.SeqNo, err)
			}
			prevOffset := restartPoint[len(restartPoint)-1]
			if len(restartPoint) == 1 {
				recordBuf := bytes.NewBuffer(record)
				rKey, value, err := ReadRecord(nil, recordBuf)
				if err != nil {
					return nil, fmt.Errorf("%d stage %d node, read records error %v", n.Level, n.SeqNo, err)
				}
				cmp := bytes.Compare(key, rKey)
				if cmp != 0 {
					return nil, fmt.Errorf("%d stage %d node, read records error %v", n.Level, n.SeqNo, err)
				} else {
					return value, nil
				}
			} else {
				for i := len(restartPoint) - 2; i >= 0; i-- {
					recordBuf := bytes.NewBuffer(record[restartPoint[i]:prevOffset])
					rKey, value, err := ReadRecord(nil, recordBuf)
					if err != nil {
						if err != io.EOF {
							return nil, fmt.Errorf("%d stage %d node, read records error %v", n.Level, n.SeqNo, err)
						}
						return nil, errors.New("error in readBlock EOF : " + err.Error())
					}
					cmp := bytes.Compare(key, rKey)
					if cmp < 0 {
						prevOffset = restartPoint[i]
						continue
					} else if cmp == 0 {
						return value, nil
					} else {
						prevKey := rKey
						for {
							rKey, value, err = ReadRecord(prevKey, recordBuf)
							if err != nil {
								if err != io.EOF {
									return nil, fmt.Errorf("%d stage %d node, read records error %v", n.Level, n.SeqNo, err)
								}
								return nil, errors.New("error in readBlock EOF : " + err.Error())
							}
							if bytes.Equal(key, rKey) {
								return value, nil
							}
							prevKey = rKey
						}
					}
				}
			}
		}
	}
	return nil, nil
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
