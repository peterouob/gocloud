package memtable

import (
	"encoding/json"
	"errors"
	"github.com/peterouob/gocloud/db/kv"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
	"log"
	"sync"
	"time"
)

type memState uint8

const (
	writeAble memState = iota
	readOnly
	flush
)

type mockDropper struct {
	droppedErrors []error
}

func (m *mockDropper) Drop(err error) {
	m.droppedErrors = append(m.droppedErrors, err)
}

type MemTable[K any, V any] struct {
	MemTree   *Tree[K, V]
	WalReader *wal.Reader
	WalWriter *wal.Writer
	mu        sync.Mutex
	curSize   int
	maxSize   int
	state     memState
	stateChan chan memState
	ticker    *time.Ticker
}

type IMemTable[K any, V any] struct {
	ReadOnlyTable []*MemTable[K, V]
	mu            sync.Mutex
}

func NewMemTable[K any, V any](c utils.Comparator[K], maxSize int, r *wal.Reader, w *wal.Writer, t time.Duration) *MemTable[K, V] {
	m := &MemTable[K, V]{
		MemTree:   NewTree[K, V](c),
		WalReader: r,
		WalWriter: w,
		maxSize:   maxSize,
		curSize:   0,
		state:     writeAble,
		ticker:    time.NewTicker(t),
	}
	go m.listenState()
	return m
}

func (m *MemTable[K, V]) listenState() {
	for {
		select {
		case st := <-m.stateChan:
			if st == writeAble {

			} else if st == readOnly {
				// TODO: flush
			} else {

			}
		case <-m.ticker.C:
			log.Println("time arrival !")
		}
	}
}

func NewIMemTable[K any, V any]() *IMemTable[K, V] {
	return &IMemTable[K, V]{
		ReadOnlyTable: make([]*MemTable[K, V], 0),
	}
}

func (m *MemTable[K, V]) Put(key K, value V) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := kv.NewKV(key, value)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.New("error in marshal data")
	}

	w := m.WalWriter.Next()
	size, err := w.Write(dataBytes)
	if err != nil {
		return errors.New("error in write data")
	}

	m.MemTree.Insert(key, value)
	m.curSize += size

	if m.curSize > m.maxSize {
		m.Flush()
	}
	return nil
}

func (m *MemTable[K, V]) Flush() {
	m.WalWriter.Flush()
	m.curSize = 0
	w := m.WalWriter.Next()
	m.WalWriter.Reset(w)
}
