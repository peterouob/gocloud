package memtable

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/memtable/kv"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/peterouob/gocloud/db/utils"
	"github.com/peterouob/gocloud/db/wal"
)

type memState uint8

const (
	writeAble memState = iota
	readOnly
)

type MemTableInterface[K any, V any] interface {
	Put(k K, v V) error
	Get(k K) (V, error)
	DeepCopy() *MemTable[K, V]
	Reset()
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
	f           *os.File
	conf        *config.Config
}

var _ MemTableInterface[any, any] = (*MemTable[any, any])(nil)

func NewMemTable[K any, V any](c utils.Comparator[K], maxSize int, r *wal.Reader,
	w *wal.Writer, t time.Duration, iMemTable *IMemTable[K, V], fileName string, conf *config.Config) *MemTable[K, V] {
	base := strings.TrimSuffix(fileName, path.Ext(fileName))

	txtPath := path.Join(conf.Dir, base+".txt")
	txtFile, err := os.OpenFile(txtPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil
	}
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
		f:           txtFile,
		conf:        conf,
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

func (m *MemTable[K, V]) Put(k K, v V) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == readOnly {
		log.Println("This is read only table")
		m.Reset()
		return errors.New("memtable is read-only, flushed")
	}

	key, value := utils.FormatKeyValue(k, v)
	data := kv.NewKV(key, value)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.New("error in marshal data")
	}

	w := m.WalWriter.Next()
	size, err := w.Write(dataBytes)
	if err != nil {
		return fmt.Errorf("error in write data: %v", err)
	}

	m.curSize += size
	if m.curSize > m.maxSize {
		log.Println("Max size exceeded, switching to read-only state")
		m.state = readOnly
		m.Reset()
	} else {
		m.WalWriter.Flush()
	}

	m.MemTree.Insert(k, v)
	if err := m.write(key, value); err != nil {
		return fmt.Errorf("error in write data: %v", err)
	}

	return nil
}

func (m *MemTable[K, V]) write(key []byte, value []byte) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(key))); err != nil {
		return fmt.Errorf("error writing key length: %v", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(value))); err != nil {
		return fmt.Errorf("error writing value length: %v", err)
	}

	if _, err := buf.Write(key); err != nil {
		return fmt.Errorf("error writing key to buffer: %v", err)
	}

	if _, err := buf.Write(value); err != nil {
		return fmt.Errorf("error writing value to buffer: %v", err)
	}

	return nil
}

func (m *MemTable[K, V]) Get(key K) (V, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var v V

	if m.MemTree == nil || m.MemTree.Size == 0 {
		if m.IMemTable.Len() != 0 {
			v, err := m.IMemTable.Get(key)
			if err != nil {
				return v, errors.New("error in get data from memtable and immtable")
			} else {
				return v, nil
			}
		}
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
