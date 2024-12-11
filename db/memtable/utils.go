package memtable

import "cmp"

type Comparator[T comparable] interface{}

type OrderComparator[T cmp.Ordered] struct{}

func (o *OrderComparator[T]) Comparator(a T, b T) int {
	return cmp.Compare[T](a, b)
}
