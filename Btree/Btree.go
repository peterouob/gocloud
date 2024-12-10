package Btree

import (
	"sync"
)

type BPTreeInterface[T is] interface {
	BPTreeInsert[T]
}

type BPTreeInsert[T is] interface {
	Set(key int64, value T)
	setValue(parent *BPTreeNode[T], node *BPTreeNode[T], key int64, value T)
}

var _ BPTreeInterface[string] = (*BPTree[string])(nil)

type BPTree[T is] struct {
	mutex sync.Mutex
	root  *BPTreeNode[T]
	width int
	half  int
}

func NewBPTree[T is](width int) *BPTree[T] {
	if width < 3 {
		width = MAX
	}
	var bpt = &BPTree[T]{}
	bpt.root = NewLeafNode[T](width)
	bpt.width = width
	bpt.half = (bpt.width + 1) / 2
	return bpt
}

func (b *BPTree[T]) Get(key int64) interface{} {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	node := b.root
	for i := 0; i < len(node.Nodes); i++ {
		if key <= node.Nodes[i].MaxKey {
			node = node.Nodes[i]
			i = 0
		}
	}

	if len(node.Nodes) > 0 {
		return nil
	}

	for i := 0; i < len(node.Items); i++ {
		if node.Items[i].Key == key {
			return node.Items[i].Value
		}
	}
	return nil
}

func (b *BPTree[T]) Set(key int64, value T) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.setValue(nil, b.root, key, value)
}

func (b *BPTree[T]) setValue(parent *BPTreeNode[T], node *BPTreeNode[T], key int64, value T) {
	for i := 0; i < len(node.Nodes); i++ {
		if key <= node.Nodes[i].MaxKey || i == len(node.Nodes) {
			b.setValue(node, node.Nodes[i], key, value)
			break
		}
	}

	if len(node.Nodes) < 1 {
		node.setNodeValue(key, value)
	}

	newNode := b.splitNode(node)
	if newNode != nil {
		if parent == nil {
			parent = NewIndexNode[T](b.width)
			parent.addChild(node)
			b.root = parent
		}
		parent.addChild(newNode)
	}
}

func (b *BPTree[T]) splitNode(node *BPTreeNode[T]) *BPTreeNode[T] {
	if len(node.Nodes) > b.width {
		half := b.width/2 + 1
		node2 := NewIndexNode[T](half)
		node2.Nodes = append(node2.Nodes, node.Nodes[half:len(node.Nodes)]...)
		node2.MaxKey = node2.Nodes[len(node.Nodes)-1].MaxKey

		node.Nodes = node.Nodes[0:half]
		node.MaxKey = node.Nodes[len(node.Nodes)-1].MaxKey

		return node2
	} else if len(node.Items) > b.width {
		half := b.width/2 + 1
		node2 := NewLeafNode[T](b.width)
		node2.Items = append(node2.Items, node.Items[half:len(node.Items)]...)
		node2.MaxKey = node2.Items[len(node.Items)-1].Key

		node.Next = node2
		node.Items = node.Items[0:half]
		node.MaxKey = node.Items[len(node.Items)-1].Key

		return node2
	}
	return nil
}
