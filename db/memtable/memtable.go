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
				m.mu.Lock()
				m.state = readOnly
				m.stateChan.Broadcast()
				m.mu.Unlock()
				m.Flush()
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
		m.Flush()
		return errors.New("memtable is read-only, flushed")

	}

	data := kv.NewKV(key, value)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.New("error in marshal data")
	}

	w := m.WalWriter.Next()
	size, err := w.Write(dataBytes)
	if err != nil {
		return errors.New("error in write data :" + err.Error())
	}

	m.MemTree.Insert(key, value)
	m.curSize += size
	if m.curSize > m.maxSize {
		log.Println("Max size exceeded, switching to read-only state")
		m.state = readOnly
		m.Flush()
	}
	return nil
}

func (m *MemTable[K, V]) Get(key K) (V, error) {
	var v V

	m.mu.Lock()
	defer m.mu.Unlock()

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

func (m *MemTable[K, V]) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IMemTable != nil {
		m.IMemTable.ReadOnlyTable = append(m.IMemTable.ReadOnlyTable, m)
		log.Println("MemTable appended to ReadOnlyTable")
	}
	log.Println("append finish")
	m.WalWriter.Flush()
	m.curSize = 0
	w := m.WalWriter.Next()
	m.WalWriter.Reset(w)

	r, err := m.WalReader.Next()
	if err != nil {
		panic(errors.New("error in call wal reader next"))
	}
	drop, ok := m.WalReader.State[0].(wal.Dropper)
	if !ok {
		panic(errors.New("error in translate type for wal reader state "))
	}
	strict, ok := m.WalReader.State[1].(bool)
	if !ok {
		panic(errors.New("error in translate type for wal reader state "))
	}
	checksum, ok := m.WalReader.State[2].(bool)
	if !ok {
		panic(errors.New("error in translate type for wal reader state "))
	}

	m.WalReader.Reset(r, drop, strict, checksum)
	m.state = writeAble
	log.Println("Flush completed, state reset to writeAble")

}
