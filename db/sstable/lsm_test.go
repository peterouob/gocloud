package sstable

import (
	"bytes"
	"fmt"
	bptree2 "github.com/peterouob/gocloud/bptree"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/memtable"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
	"github.com/stretchr/testify/assert"
	"os"
	"runtime"
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
	memtab := memtable.NewMemTable[string, string](compare, 1024, r, w, 3*time.Hour, im, "1", config.NewConfig("./"))
	err := memtab.Put("key1", "value1")
	assert.NoError(t, err)
	err = memtab.Put("key2", "value2")
	assert.NoError(t, err)

	err = lsmt.FlushRecord(memtab, "test")
	assert.NoError(t, err, "Flush records should not return an error")
	assert.Len(t, lsmt.tree[0], 1, "A new node should be created in level 0")
}

func TestFlushMutilRecord(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))
	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	memtab := memtable.NewMemTable[string, string](compare, 1024, r, w, 3*time.Hour, im, "1", config.NewConfig("./"))
	for i := 0; i < 100; i++ {
		err := memtab.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		assert.NoError(t, err)
		err = lsmt.FlushRecord(memtab, "test")
		assert.NoError(t, err, "Flush records should not return an error")
	}
}

func TestLargeScaleWritePerformance(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))
	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	memtab := memtable.NewMemTable[string, string](compare, 10240, r, w, 3*time.Hour, im, "1", config.NewConfig(dir))

	const recordCount = 1000

	startLSM := time.Now()
	for i := 0; i < recordCount; i++ {
		err := memtab.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		assert.NoError(t, err)

		if i%100 == 0 && i != 0 {
			err = lsmt.FlushRecord(memtab, "test")
			assert.NoError(t, err, "Flush records should not return an error")
		}
	}
	lsmDuration := time.Since(startLSM)
	t.Logf("LSM Tree Write Duration: %v", lsmDuration)

	startFile := time.Now()
	file, err := os.Create("test_file.txt")
	assert.NoError(t, err, "File creation should not return an error")
	defer file.Close()

	for i := 0; i < recordCount; i++ {
		_, err := file.WriteString(fmt.Sprintf("key%d: value%d\n", i, i))
		assert.NoError(t, err, "File write should not return an error")
	}
	fileDuration := time.Since(startFile)
	t.Logf("Normal File Write Duration: %v", fileDuration)

	t.Logf("Performance Comparison: LSM = %v, File = %v", lsmDuration, fileDuration)
}

func TestFlushComparNormal(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))
	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	memtab := memtable.NewMemTable[string, string](compare, 1024, r, w, 3*time.Hour, im, "1", config.NewConfig(dir))

	startLSM := time.Now()
	for i := 0; i < 100; i++ {
		err := memtab.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		assert.NoError(t, err)

		err = lsmt.FlushRecord(memtab, "test")
		assert.NoError(t, err, "Flush records should not return an error")
	}
	lsmDuration := time.Since(startLSM)
	t.Logf("LSM Tree Flush duration: %v", lsmDuration)

	startFile := time.Now()
	file, err := os.Create("test_file.txt")
	assert.NoError(t, err, "File creation should not return an error")
	defer file.Close()

	for i := 0; i < 100; i++ {
		_, err := file.WriteString(fmt.Sprintf("key%d: value%d\n", i, i))
		assert.NoError(t, err, "File write should not return an error")
	}
	fileDuration := time.Since(startFile)
	t.Logf("Normal file write duration: %v", fileDuration)

	t.Logf("LSM Tree vs Normal File Write: LSM = %v, File = %v", lsmDuration, fileDuration)
}
func TestLargeScaleWritePerformanceWithMemory(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))
	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	memtab := memtable.NewMemTable[string, string](compare, 10240, r, w, 3*time.Hour, im, "1", config.NewConfig(dir))

	const recordCount = 1000

	startMemLSM := getMemoryUsage()
	startLSM := time.Now()
	for i := 0; i < recordCount; i++ {
		err := memtab.Put(fmt.Sprintf("key%d", 1), fmt.Sprintf("value%d", 1))
		assert.NoError(t, err)

		if i%100 == 0 && i != 0 {
			err = lsmt.FlushRecord(memtab, "test")
			assert.NoError(t, err, "Flush records should not return an error")
		}
	}
	lsmDuration := time.Since(startLSM)
	endMemLSM := getMemoryUsage()
	t.Logf("LSM Tree Write Duration: %v", lsmDuration)
	t.Logf("LSM Tree Memory Usage: %d KB", endMemLSM-startMemLSM)

	startMemFile := getMemoryUsage()
	startFile := time.Now()
	file, err := os.Create("test_file.txt")
	assert.NoError(t, err, "File creation should not return an error")
	defer file.Close()

	for i := 0; i < recordCount; i++ {
		_, err := file.WriteString(fmt.Sprintf("key%d: value%d\n", 1, 1))
		assert.NoError(t, err, "File write should not return an error")
	}
	fileDuration := time.Since(startFile)
	endMemFile := getMemoryUsage()
	t.Logf("Normal File Write Duration: %v", fileDuration)
	t.Logf("Normal File Memory Usage: %d KB", endMemFile-startMemFile)

	t.Logf("Performance Comparison: LSM = %v, File = %v", lsmDuration, fileDuration)
	t.Logf("Memory Usage Comparison: LSM = %d KB, File = %d KB", endMemLSM-startMemLSM, endMemFile-startMemFile)
}

func getMemoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024
}
func TestGetKey(t *testing.T) {
	lsmt := NewLSMTree[string, string](config.NewConfig(dir))

	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	memtab := memtable.NewMemTable[string, string](compare, 1024, r, w, 3*time.Hour, im, "1", config.NewConfig("./"))

	for i := 0; i < 1000; i++ {
		err := memtab.Put("key1", "value1")
		assert.NoError(t, err)

		err = lsmt.FlushRecord(memtab, "test")
		assert.NoError(t, err, "Flush records should not return an error")

		err = memtab.Put("key2", "value2")
		assert.NoError(t, err)

		err = lsmt.FlushRecord(memtab, "test2")
		assert.NoError(t, err, "Flush records should not return an error")

		err = memtab.Put("key3", "value3")
		assert.NoError(t, err)

		err = lsmt.FlushRecord(memtab, "test3")
		assert.NoError(t, err, "Flush records should not return an error")

	}

	value := lsmt.Get("key1")
	assert.Equal(t, string(value), "value1")
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

func TestCompareWithBPTree(t *testing.T) {

	lsmt := NewLSMTree[string, string](config.NewConfig(dir))
	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	memtab := memtable.NewMemTable[string, string](compare, 10240, r, w, 3*time.Hour, im, "1", config.NewConfig(dir))

	const recordCount = 3000

	for i := 0; i < recordCount; i++ {
		err := memtab.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		assert.NoError(t, err)
	}
	startMemLSM := getMemoryUsage()
	startLSM := time.Now()
	lsmt.FlushRecord(memtab, "test")
	lsmDuration := time.Since(startLSM)
	endMemLSM := getMemoryUsage()
	t.Logf("LSM Tree Write Duration: %v", lsmDuration)
	t.Logf("LSM Tree Write Memory Usage: %d KB", endMemLSM-startMemLSM)

	readMemLSM := getMemoryUsage()
	startReadLSM := time.Now()
	lsmt.Get(fmt.Sprintf("key%d", 777))
	endReadMemLSM := getMemoryUsage()
	endReadLSM := time.Since(startReadLSM)
	t.Logf("LSM Tree Read Duration: %v", endReadLSM)
	t.Logf("LSM Tree Read Memory Usage: %d KB", endReadMemLSM-readMemLSM)

	startMemBP := getMemoryUsage()
	startBP := time.Now()
	bptree := bptree2.NewBPTree[string](10)
	for i := 0; i < recordCount; i++ {
		bptree.Insert(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	BPDuration := time.Since(startBP)
	endMemBP := getMemoryUsage()
	t.Logf("BP Tree Write Duration: %v", BPDuration)
	t.Logf("BP Tree Write Memory Usage: %d KB", endMemBP-startMemBP)

	readMemBP := getMemoryUsage()
	startReadBP := time.Now()
	bptree.Get(fmt.Sprintf("key%d", 777))
	endReadMemBP := getMemoryUsage()
	endReadBP := time.Since(startReadBP)
	t.Logf("BPTree Tree Read Duration: %v", endReadBP)
	t.Logf("BPTree Tree Read Memory Usage: %d KB", endReadMemBP-readMemBP)

}
