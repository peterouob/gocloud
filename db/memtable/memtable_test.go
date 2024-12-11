package memtable

import "testing"

func TestMemTable(t *testing.T) {
	compare := &OrderComparator[int]{}
	tree := NewTree[int, int](compare)
	tree.FindKey(1)
	tree.Insert(1, 1)
	tree.Insert(2, 2)
	tree.Insert(1, 2)
	tree.Insert(13, 23)
	tree.Insert(14, 21)
	tree.Insert(15, 20)
	t.Log(tree.root)
}
