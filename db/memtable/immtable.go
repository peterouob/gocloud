package memtable

import (
	"errors"
	"sync"
)

type IMemTable[K any, V any] struct {
	readOnlyTable []*MemTable[K, V]
	mu            sync.Mutex
}

func NewIMemTable[K any, V any]() *IMemTable[K, V] {
	return &IMemTable[K, V]{
		readOnlyTable: make([]*MemTable[K, V], 0),
	}
}

func (i *IMemTable[K, V]) Len() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.readOnlyTable)
}

func (i *IMemTable[K, V]) GetTable(key K) *MemTable[K, V] {
	i.mu.Lock()
	defer i.mu.Unlock()
	table := i.readOnlyTable[0]
	i.readOnlyTable = i.readOnlyTable[:]
	return table
}

func (i *IMemTable[K, V]) Get(key K) (V, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, table := range i.readOnlyTable {
		node := table.MemTree.FindKey(key)
		return node.Value, nil
	}

	var vnil V
	return vnil, errors.New("key not found in immutable table")
}
