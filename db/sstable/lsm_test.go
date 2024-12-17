package sstable

import (
	"bytes"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/memtable"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const dir string = "./sst"

func TestBlockCompress(t *testing.T) {
	b := NewBlock(config.NewConfig(dir))
	b.Append([]byte("heelo"), []byte("woorld"))
	b.Append([]byte("heal@"), []byte("w00rld"))
	b.Append([]byte("he1lo"), []byte("woor1d"))
	b.Append([]byte("h-elo"), []byte("w@@rld"))

	t.Log(string(b.compress()))
}

func TestNewLSMTree(t *testing.T) {
	conf := config.NewConfig(dir)
	_ = NewLSMTree[string, string](conf)
}

func TestInsertNode(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))

	node0 := &Node{
		Level:    0,
		SeqNo:    1,
		startKey: []byte("key1"),
		endKey:   []byte("key1"),
	}
	lsmt.insertNode(node0)
	assert.Len(t, lsmt.tree[0], 1, "Node should be inserted at level 0")

	node1 := &Node{
		Level:    0,
		SeqNo:    2,
		startKey: []byte("key2"),
		endKey:   []byte("key2"),
	}
	lsmt.insertNode(node1)
	assert.Len(t, lsmt.tree[0], 2, "Second node should be inserted at level 0")

	node2 := &Node{
		Level:    1,
		SeqNo:    1,
		startKey: []byte("key3"),
		endKey:   []byte("key3"),
	}
	lsmt.insertNode(node2)
	assert.Len(t, lsmt.tree[1], 1, "Node should be inserted at level 1")
}

func TestNextSeqNo(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))

	seqNo1 := lsmt.NextSeqNo(0)
	assert.Equal(t, 1, seqNo1, "First sequence number should be 1")

	seqNo2 := lsmt.NextSeqNo(0)
	assert.Equal(t, 2, seqNo2, "Second sequence number should increment")
}

func TestPickCompactionNode(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))

	node1 := &Node{
		Level:    1,
		startKey: []byte("key1"),
		endKey:   []byte("key3"),
		index:    []*Index{{Key: []byte("key1")}},
	}
	node2 := &Node{
		Level:    1,
		startKey: []byte("key4"),
		endKey:   []byte("key6"),
		index:    []*Index{{Key: []byte("key4")}},
	}
	node3 := &Node{
		Level:    2,
		startKey: []byte("key2"),
		endKey:   []byte("key5"),
		index:    []*Index{{Key: []byte("key2")}},
	}

	lsmt.tree[1] = append(lsmt.tree[1], node1, node2)
	lsmt.tree[2] = append(lsmt.tree[2], node3)

	compactionNodes := lsmt.PickCompactionNode(1)
	assert.Len(t, compactionNodes, 2, "Should pick nodes for compaction")
	assert.True(t, compactionNodes[0].compacting, "Nodes should be marked as compacting")
}

func TestFlushRecord(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))

	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	memtab := memtable.NewMemTable[string, string](compare, 1024, r, w, 3*time.Hour, im)
	err := memtab.Put("key1", "value1")
	assert.NoError(t, err)
	err = memtab.Put("key2", "value2")
	assert.NoError(t, err)

	err = lsmt.FlushRecord(memtab, "test")
	assert.NoError(t, err, "Flush record should not return an error")
	assert.Len(t, lsmt.tree[0], 1, "A new node should be created in level 0")
}

func TestRemoveNode(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))

	node1 := &Node{
		Level:    1,
		SeqNo:    1,
		startKey: []byte("key1"),
		endKey:   []byte("key3"),
	}
	node2 := &Node{
		Level:    1,
		SeqNo:    2,
		startKey: []byte("key4"),
		endKey:   []byte("key6"),
	}

	lsmt.tree[1] = append(lsmt.tree[1], node1, node2)

	lsmt.removeNode([]*Node{node1})
	assert.Len(t, lsmt.tree[1], 1, "Node should be removed")
	assert.Equal(t, node2, lsmt.tree[1][0], "Remaining node should be the one not removed")
}
