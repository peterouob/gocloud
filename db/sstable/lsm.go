package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/memtable"
	"log"
	"math"
	"sync"
)

type LSMTree[K any, V any] struct {
	mu          sync.Mutex
	conf        *config.Config
	tree        [][]*Node
	seqNo       []int
	compactChan chan int
	stopChan    chan struct{}
}

func NewLSMTree[K any, V any](conf *config.Config) *LSMTree[K, V] {
	compactionChan := make(chan int, 100)
	levelTree := make([][]*Node, conf.MaxLevel)

	for i := range levelTree {
		levelTree[i] = make([]*Node, 0)
	}

	seqNos := make([]int, conf.MaxLevel)
	lsmt := &LSMTree[K, V]{
		conf:        conf,
		tree:        levelTree,
		seqNo:       seqNos,
		compactChan: compactionChan,
		stopChan:    make(chan struct{}),
	}

	lsmt.CheckCompaction()
	return lsmt
}

func (t *LSMTree[K, V]) FlushRecord(memtable *memtable.MemTable[K, V], extra string) error {
	level := 0
	seqNo := t.NextSeqNo(level)

	file := formatName(level, seqNo, extra)
	w, err := NewSStWriter(file, t.conf)
	if err != nil {
		return errors.New("error in new ssWriter : " + err.Error())
	}
	defer w.Close()
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
		keys = append(keys, node.Key)
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

func (t *LSMTree[K, V]) compaction(level int) error {
	nodes := t.PickCompactionNode(level)
	lenNodes := len(nodes)
	if lenNodes == 0 {
		return nil
	}

	nextLevel := lenNodes + 1
	seqNo := t.NextSeqNo(nextLevel)
	extra := nodes[len(nodes)-1].Extra
	file := formatName(level, seqNo, extra)
	writer, err := NewSStWriter(file, t.conf)
	if err != nil {
		return errors.New("error in new ssWriter : " + err.Error())
	}

	var record *Record
	var files string
	maxNodeSize := t.conf.SstSize * int(math.Pow10(nextLevel))

	for i, node := range nodes {
		files += formatName(node.Level, node.SeqNo, node.Extra)
		record = record.Fill(nodes, i)
	}
	log.Printf("compaction : %v", files)

	writeCount := 0

	for record != nil {
		writeCount++
		i := record.Idx
		writer.Append(record.Key, record.Value)
		record = record.next.Fill(nodes, i)

		if writer.Size() > maxNodeSize {
			size, filter, index := writer.Finish()
			writer.Close()

			node, err := NewNode(filter, index, level, seqNo, extra, size, t.conf, file)
			if err != nil {
				return errors.New("error in create new node : " + err.Error())
			}
			t.insertNode(node)

			seqNo = t.NextSeqNo(nextLevel)
			file = formatName(nextLevel, seqNo, extra)
			writer, err = NewSStWriter(file, t.conf)
			if err != nil {
				return fmt.Errorf("%s error in create writer,cannot compaction lsm log error: %v", file, err)
			}
		}
	}

	size, filter, index := writer.Finish()
	node, err := NewNode(filter, index, level, seqNo, extra, size, t.conf, file)
	if err != nil {
		return errors.New("error in create new node : " + err.Error())
	}
	t.insertNode(node)
	t.removeNode(nodes)

	t.compactChan <- nextLevel
	return nil
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
