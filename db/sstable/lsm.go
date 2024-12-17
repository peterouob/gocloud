package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/memtable"
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

type LSMTree[K any, V any] struct {
	mu          sync.Mutex
	conf        *config.Config
	tree        [][]*LSMTree[K, V]
	seqNo       []int
	compactChan chan int
	stopChan    chan struct{}
}

func (t *LSMTree[K, V]) FlushRecord(memtable *memtable.MemTable[K, V], extra string) error {
	level := 0
	seqNo := t.NextSeqNo(level)

	file := formatName(level, seqNo, extra)
	w, err := NewSStWriter(file, t.conf)
	if err != nil {
		return errors.New("error in new ssWriter : " + err.Error())
	}
	// TODO:defer w.Close
	tree := memtable.MemTree

	var keys []K
	count := 0
	for {
		node, found := tree.Next(keys)
		if !found {
			break
		}
		bkey, bvalue := formatKeyValue(node.Key, node.Value)
		w.Append(bkey, bvalue)
		count++
	}
	log.Printf("write in %s, count %d", file, count)

	size, filter, index := w.Finish()
	node, err := NewNode(filter, index, level, seqNo, extra, size, t.conf, file)
	if err != nil {
		return errors.New("error in new Node after append ssWriter: " + err.Error())
	}
	t.insertNode(node)
	t.compactChan <- level
	return nil
}

func (t *LSMTree[K, V]) insertNode(node *Node) {}

func (t *LSMTree[K, V]) NextSeqNo(level int) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seqNo[level]++

	return t.seqNo[level]
}

func formatName(level, seqNo int, extra string) string {
	return fmt.Sprintf("%d_%d_%s.sst", level, seqNo, extra)
}

func formatKeyValue[K any, V any](key K, value V) ([]byte, []byte) {
	var bKey []byte
	var bValue []byte

	switch v := any(key).(type) {
	case string:
		bKey = []byte(v)
	case []byte:
		bKey = v
	default:
		panic("Unsupported key type")
	}

	switch v := any(value).(type) {
	case string:
		bValue = []byte(v)
	case []byte:
		bValue = v
	default:
		panic("Unsupported value type")
	}
	return bKey, bValue
}