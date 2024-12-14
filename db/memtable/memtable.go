package memtable

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/peterouob/gocloud/db/memtable/kv"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
)

type memState uint8

const (
	writeAble memState = iota
	readOnly
)

type mockDropper struct {
	droppedErrors []error
}

func (m *mockDropper) Drop(err error) {
	m.droppedErrors = append(m.droppedErrors, err)
}

type MemTable[K any, V any] struct {
	MemTree     *Tree[K, V]
	WalReader   *wal.Reader
	WalWriter   *wal.Writer
	mu          sync.Mutex
	curSize     int
	maxSize     int
	state       memState
	stateChan   *sync.Cond
	flushPeriod time.Duration
	ticker      *time.Ticker
	IMemTable   *IMemTable[K, V]
}

func NewMemTable[K any, V any](c utils.Comparator[K], maxSize int, r *wal.Reader,
	w *wal.Writer, t time.Duration, iMemTable *IMemTable[K, V]) *MemTable[K, V] {
	m := &MemTable[K, V]{
		MemTree:     NewTree[K, V](c),
		WalReader:   r,
		WalWriter:   w,
		maxSize:     maxSize,
		curSize:     0,
		flushPeriod: t,
		ticker:      time.NewTicker(t),
		stateChan:   sync.NewCond(&sync.Mutex{}),
		IMemTable:   iMemTable,
	}
	go m.listenState()
	return m
}

func (m *MemTable[K, V]) listenState() {
	defer m.ticker.Stop()
	for {
		select {
		case <-m.ticker.C:
			if m.state != readOnly {
				log.Println("hello !")
				m.state = readOnly
				m.stateChan.Broadcast()
				m.Reset()
				m.ticker.Reset(m.flushPeriod)
			} else {
				m.mu.Unlock()
			}
		}
	}
}

func (m *MemTable[K, V]) Put(key K, value V) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == readOnly {
		log.Println("this is read only table")
		m.Reset()
		return errors.New("memtable is read-only, flushed")
	}

	data := kv.NewKV(key, value)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.New("error in marshal data")
	}

	w := m.WalWriter.Next()
	size, err := w.Write(dataBytes)

	m.curSize += size
	if m.curSize > m.maxSize {
		log.Println("Max size exceeded, switching to read-only state")
		m.state = readOnly
		m.Reset()
	} else {
		m.WalWriter.Flush()
	}

	if err != nil {
		return errors.New("error in write data :" + err.Error())
	}

	m.MemTree.Insert(key, value)

	return nil
}

func (m *MemTable[K, V]) Get(key K) (V, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var v V

	if m.MemTree == nil || m.MemTree.Size == 0 {
		return v, errors.New("memtable is empty")
	}

	keydata, err := json.Marshal(key)
	if err != nil {
		return v, errors.New("error in marshal key")
	}
	reader, err := m.WalReader.Next()
	if err != nil {
		return v, errors.New("error in call wal reader next :" + err.Error())

	}
	if _, err := reader.Read(keydata); err != nil {
		return v, errors.New("error in wal reader key data :" + err.Error())

	}
	node := m.MemTree.FindKey(key)
	v = node.Value
	return v, nil
}

func (m *MemTable[K, V]) DeepCopy() *MemTable[K, V] {
	newMemTree := m.MemTree.DeepCopy()
	return &MemTable[K, V]{
		MemTree:     newMemTree,
		WalReader:   m.WalReader,
		WalWriter:   m.WalWriter,
		maxSize:     m.maxSize,
		flushPeriod: m.flushPeriod,
		ticker:      m.ticker,
		curSize:     m.curSize,
		state:       m.state,
		stateChan:   m.stateChan,
		IMemTable:   m.IMemTable,
		mu:          sync.Mutex{},
	}
}

func (m *MemTable[K, V]) Reset() {
	newCopy := m.DeepCopy()

	m.IMemTable.mu.Lock()
	if m.IMemTable != nil {
		m.IMemTable.readOnlyTable = append(m.IMemTable.readOnlyTable, newCopy)
	}
	m.MemTree = NewTree[K, V](m.MemTree.comparator)
	m.curSize = 0
	m.state = writeAble
	m.IMemTable.mu.Unlock()

}

func (m *MemTable[K, V]) Restore(file string) (*Tree[K, V], error) {
	fd, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL file: %v", err)
	}
	node := NewTree[K, V](m.MemTree.comparator)
	r := wal.NewReader(fd)
	for {
		k, v, err := r.WalNext()
		if err != nil {
			return nil, err
		}

		if len(k) == 0 {
			break
		}

		node.Insert(any(k), any(v))
	}

	return node, nil
}
