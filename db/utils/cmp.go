package utils

import "cmp"

type Comparator[T any] interface {
	Compare(T, T) int
}

type OrderComparator[T cmp.Ordered] struct{}

var _ Comparator[int] = (*OrderComparator[int])(nil)

func (o *OrderComparator[T]) Compare(a T, b T) int {
	return cmp.Compare[T](a, b)
}
