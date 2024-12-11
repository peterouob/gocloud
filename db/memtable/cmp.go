package memtable

import "cmp"

type Comparator[T comparable] interface {
	Compare(T, T) int
}

type OrderComparator[T cmp.Ordered] struct{}

func (o *OrderComparator[T]) Compare(a T, b T) int {
	return cmp.Compare[T](a, b)
}
