package memtable

import (
	"bytes"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type dropper struct {
	t *testing.T
}

func (d dropper) Drop(err error) {
	d.t.Log(err)
}

func TestMemTableWrite(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	err := m.Put(1, 1)
	assert.NoError(t, err)
}

func TestMemTableOverflow(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
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
	r := wal.NewReader(buf, dropper{t}, false, true)
	im := NewIMemTable[int, int]()
	_ = NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	time.Sleep(10 * time.Second)
}

func TestMemTableReadEmpty(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute, im)
	_, err := m.Get(1)
	assert.Error(t, err)
}

func TestMemTableRead(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
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
	r := wal.NewReader(buf, dropper{t}, false, true)
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

func TestFlush(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 2, r, w, 3*time.Minute, im)
	m.Flush()
}

func TestImmTable(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
	im := NewIMemTable[int, int]()
	m := NewMemTable[int, int](compare, 2, r, w, 3*time.Minute, im)

	assert.Equal(t, im.Len(), 0)

	err := m.Put(1, 1)
	assert.NoError(t, err)
	err = m.Put(2, 1)
	assert.NoError(t, err)
	err = m.Put(3, 1)
	assert.NoError(t, err)

	assert.Equal(t, im.Len(), 1)

}
