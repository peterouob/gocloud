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

func (t *LSMTree[K, V]) insertNode(node *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()

	level := node.Level
	length := len(t.tree[level])

	if length == 0 {
		t.tree[level] = []*Node{node}
		return
	}

	if level == 0 {
		idx := length - 1
		for ; idx >= 0; idx-- {
			if node.SeqNo > t.tree[level][idx].SeqNo {
				break
			} else if node.SeqNo == t.tree[level][idx].SeqNo {
				t.tree[level][idx] = node
				return
			}
		}
		t.tree[level] = append(t.tree[level][:idx+1], t.tree[level][idx:]...)
		t.tree[level][idx+1] = node
	} else {
		for i, n := range t.tree[level] {
			cmp := bytes.Compare(n.startKey, node.startKey)
			if cmp < 0 {
				t.tree[level] = append(t.tree[level][:i+1], t.tree[level][i:]...)
				t.tree[level][i] = node
				return
			}
		}
		t.tree[level] = append(t.tree[level], node)
	}
}

func (t *LSMTree[K, V]) PickCompactionNode(level int) []*Node {
	t.mu.Lock()
	defer t.mu.Unlock()

	compactionNode := make([]*Node, 0)
	if len(t.tree[level]) == 0 {
		return compactionNode
	}

	// for level 0
	startKey := t.tree[level][0].startKey
	endKey := t.tree[level][0].endKey

	if level != 0 {
		node := t.tree[level][(len(t.tree[level])-1)/2] // find middle point
		if bytes.Compare(node.startKey, startKey) < 0 {
			startKey = node.startKey
		}
		if bytes.Compare(node.endKey, endKey) > 0 {
			endKey = node.endKey
		}
	}

	for i := level + 1; i >= level; i-- {
		for _, node := range t.tree[i] {
			if node.index == nil {
				continue
			}

			nodeStartKey := node.index[0].Key
			nodeEndKey := node.index[len(node.index)-1].Key

			if bytes.Compare(startKey, nodeEndKey) <= 0 &&
				bytes.Compare(endKey, nodeStartKey) >= 0 &&
				!node.compacting {
				compactionNode = append(compactionNode, node)
				node.compacting = true
				if i == level+1 {
					if bytes.Compare(nodeStartKey, startKey) < 0 {
						startKey = node.startKey
					}
					if bytes.Compare(nodeEndKey, endKey) > 0 {
						endKey = node.endKey
					}
				}
			}
		}
	}

	return compactionNode
}

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

func (t *LSMTree[K, V]) removeNode(nodes []*Node) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, node := range nodes {
		log.Printf("remove %d_%d_%s.sst", node.Level, node.SeqNo, node.Extra)
		for i, tn := range t.tree[node.Level] {
			if tn.SeqNo == node.SeqNo {
				t.tree[node.Level] = append(t.tree[node.Level][:i], t.tree[node.Level][i+1:]...)
				break
			}
		}
	}

	go func() {
		for _, n := range nodes {
			n.destroy()
		}
	}()
}

func (t *LSMTree[K, V]) CheckCompaction() {
	level0 := make(chan struct{}, 100)
	levelN := make(chan int, 100)

	go func() {
		for {
			select {
			case <-level0:
				if len(t.tree[0]) > 4 {
					log.Printf("level0 compaction ..., num: %d", len(t.tree[0]))
					if err := t.compaction(0); err != nil {
						panic(errors.New(err.Error()))
					}
				}
			case <-t.stopChan:
				close(level0)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case lvn := <-levelN:
				var prevSize int64
				maxNodeSize := int64(t.conf.SstSize * int(math.Pow10(lvn+1)))
				for {
					var totalSize int64
					for _, node := range t.tree[lvn] {
						totalSize += node.FileSize
					}
					if totalSize > maxNodeSize && (prevSize == 0 || totalSize < prevSize) {
						if err := t.compaction(lvn); err != nil {
							panic(errors.New(err.Error()))
						} else {
							break
						}
					}
				}
			case <-t.stopChan:
				close(levelN)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-t.stopChan:
				return
			case lv := <-t.compactChan:
				if lv == 0 {
					level0 <- struct{}{}
				} else {
					levelN <- lv
				}
			}
		}
	}()
}
