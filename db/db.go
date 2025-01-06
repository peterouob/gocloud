package db

import (
	"bytes"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/memtable"
	"github.com/peterouob/gocloud/db/sstable"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
	"time"
)

type DB struct {
	LsmTree  *sstable.LSMTree[string, string]
	FileName string `json:"file_name"`
	Key      string `json:"key"`
	Value    string `json:"value"`
}

func NewTableInt(filename string, timeout time.Duration) *memtable.MemTable[int, int] {
	compare := &utils.OrderComparator[int]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[int, int]()
	conf := config.NewConfig("./")
	m := memtable.NewMemTable[int, int](compare, 1024, r, w, timeout, im, filename, conf)
	return m
}

func NewTableString(filename string, timeout time.Duration) *memtable.MemTable[string, string] {
	compare := &utils.OrderComparator[string]{}
	buf := new(bytes.Buffer)
	w := wal.NewWriter(buf)
	r := wal.NewReader(buf)
	im := memtable.NewIMemTable[string, string]()
	conf := config.NewConfig("./")
	m := memtable.NewMemTable[string, string](compare, 1024, r, w, timeout, im, filename, conf)
	return m
}
