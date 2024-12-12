package kv

type status uint8

type KV[K any, V any] struct {
	Key    any
	Value  any
	Status status
}

func NewKV[K any, V any](key K, value V) *KV[K, V] {
	return &KV[K, V]{
		Key:   key,
		Value: value,
	}
}
