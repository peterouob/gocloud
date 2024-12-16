package memtable

import (
	"bytes"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMemTableWrite(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	err := m.Put(1, 1)
	assert.NoError(t, err)
}

func TestMemTableOverflow(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	err := m.Put(1, 1)
	assert.NoError(t, err)
	err = m.Put(1, 1)
	assert.NoError(t, err)
	err = m.Put(1, 1)
	assert.NoError(t, err)
}

func TestMemTableListen(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	_ = NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	time.Sleep(10 * time.Second)
}

func TestMemTableReadEmpty(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	_, err := m.Get(1)
	assert.Error(t, err)
}

func TestMemTableRead(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	m.Put(1, 1)
	v, err := m.Get(1)
	assert.Equal(t, v, 1)
	assert.NoError(t, err)
}

func TestTimeOutAndRead(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 1024, r, w, 3*time.Second, im)
	err := m.Put(1, 1)
	assert.NoError(t, err)
	time.Sleep(4 * time.Second)
	val, err := m.Get(1)
	assert.Error(t, err)
	assert.Equal(t, val, 0)
	n := m.IMemTable.Len()
	t.Log(n)
}

func TestImmTable(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 2, r, w, 3*time.Minute, im)

	assert.Equal(t, im.Len(), 0)

	err := m.Put(1, 1)
	assert.NoError(t, err)
	err = m.Put(2, 2)
	assert.NoError(t, err)
	err = m.Put(3, 3)
	assert.NoError(t, err)
	v, err := im.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, v, 1)
	assert.Equal(t, im.Len(), 3)
	_, err = m.Get(1)
	assert.Error(t, err)
}

func TestManyWrite(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 4*1024*1024+9, r, w, 1*time.Second, im)
	for i := 0; i < 15000; i++ {
		// TODO:slice overflow
		err := m.Put(i%10, 1)
		assert.NoError(t, err)
	}
	v, err := m.Get(10000)
	assert.NoError(t, err)
	t.Log(v)
	t.Log(m.curSize)
	t.Log(m.IMemTable.Len())
}

func TestTimeout(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 4*1024+9, r, w, 1*time.Second, im)
	err := m.Put(1, 1)
	assert.NoError(t, err)
	err = m.Put(2, 2)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	_, err = m.Get(1)
	assert.Error(t, err)
	val, err := im.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, val, 1)
	err = m.Put(9, 10)
	assert.NoError(t, err)
	_, err = m.Get(9)
	assert.NoError(t, err)
	val, err = im.Get(9)
	assert.Error(t, err)
}
