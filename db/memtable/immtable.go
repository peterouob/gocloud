package memtable

import (
	"errors"
	"sync"
)

type IMemTable[K any, V any] struct {
	ReadOnlyTable []*MemTable[K, V]
	mu            sync.Mutex
}

func NewIMemTable[K any, V any]() *IMemTable[K, V] {
	return &IMemTable[K, V]{
		ReadOnlyTable: make([]*MemTable[K, V], 0),
	}
}

func (i *IMemTable[K, V]) Len() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.ReadOnlyTable)
}

func (i *IMemTable[K, V]) GetTable(key K) *MemTable[K, V] {
	i.mu.Lock()
	defer i.mu.Unlock()
	table := i.ReadOnlyTable[0]
	i.ReadOnlyTable = i.ReadOnlyTable[:]
	return table
}

func (i *IMemTable[K, V]) Get(key K) (V, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, table := range i.ReadOnlyTable {
		if value, err := table.Get(key); err == nil {
			return value, nil
		}
	}

	var vnil V
	return vnil, errors.New("key not found in immutable table")
}
