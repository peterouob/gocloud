package Btree

import "testing"

func TestBPTree_INT(t *testing.T) {
	bpt := NewBPTree[int](4, 4)
	bpt.Set(10, 100)
	bpt.Set(23, 1123)
	bpt.Set(33, 1000)
	bpt.Set(11, 1010)
}

func TestBPTreeSet(t *testing.T) {
	bpt := NewBPTree[int](4, 4)
	bpt.Set(10, 100)
	bpt.Set(23, 1123)
	bpt.Set(33, 1000)
	bpt.Set(40, 900)
	bpt.Set(40, 200)
	bpt.Set(40, 1000)
	t.Log(bpt.Get(40))
	bpt.Set(40, 1)
	t.Log(bpt.Get(40))

}

func TestBPTree_String(t *testing.T) {
	bpt := NewBPTree[string](4, 4)
	bpt.Set(10, "hello")
	bpt.Set(23, "iam")
	bpt.Set(33, "peter")

}
