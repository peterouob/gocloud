package Btree

import "testing"

func TestNewBPTree(t *testing.T) {
	_ = NewBPTree[string](4)
	_ = NewBPTree[int64](4)
}

func TestBPTree_INT(t *testing.T) {
	bpt := NewBPTree[int](4)
	bpt.Set(10, 100)
	bpt.Set(23, 1123)
	bpt.Set(33, 1000)

	keys := []int64{10, 23, 33}
	ans := []interface{}{100, 1123, 1000}

	for i, key := range keys {
		if bpt.Get(key) != ans[i] {
			t.Logf("got=%v,need=%d", bpt.Get(key), ans)
		}
	}
}

func TestBPTree_String(t *testing.T) {
	bpt := NewBPTree[string](4)
	bpt.Set(10, "hello")
	bpt.Set(23, "iam")
	bpt.Set(33, "peter")

	keys := []int64{10, 23, 33}
	ans := []interface{}{"hello", "iam", "peter"}

	for i, key := range keys {
		if bpt.Get(key) != ans[i] {
			t.Logf("got=%v,need=%d", bpt.Get(key), ans)
		}
	}
}
