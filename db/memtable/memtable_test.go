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
	m := NewMemTable[int, int](compare, 1024, r, w, 10*time.Minute)
	err := m.Put(1, 1)
	assert.NoError(t, err)
}

func TestMemTableOverflow(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
	m := NewMemTable[int, int](compare, 2, r, w, 10*time.Minute)
	err := m.Put(1, 1)
	assert.NoError(t, err)
	err = m.Put(1, 1)
	assert.NoError(t, err)
	err = m.Put(1, 1)
	assert.NoError(t, err)
}

func TestMemTableListe(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf, dropper{t}, false, true)
	_ = NewMemTable[int, int](compare, 2, r, w, 3*time.Second)
	time.Sleep(10 * time.Second)
}
