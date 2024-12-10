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
	mutex       sync.Mutex
	root        *BPTreeNode[T]
	width       int
	internalMax int
	isLeaf      bool
}

func NewBPTree[T is](width, internalMax int) *BPTree[T] {
	if width < 3 {
		width = MAX
	} else if internalMax < 3 {
		internalMax = MAX
	}
	var bpt = &BPTree[T]{}
	bpt.root = NewBTreeNode[T](width, true)
	bpt.width = width
	bpt.internalMax = internalMax
	bpt.isLeaf = false
	return bpt
}

func (b *BPTree[T]) Get(key int64) []T {
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
	var result []T
	for i := 0; i < len(node.Items); i++ {

		if node.Items[i].Key == key {
			result = append(result, node.Items[i].Value)
		}
	}
	return result
}

func (b *BPTree[T]) Set(key int64, value T) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.setValue(nil, b.root, key, value)
}

func (b *BPTree[T]) setValue(parent *BPTreeNode[T], node *BPTreeNode[T], key int64, value T) {
	if len(node.Nodes) == 0 {
		node.setNodeValue(key, value)
	}
	for i := 0; i < len(node.Nodes); i++ {
		if key <= node.Nodes[i].MaxKey {
			b.setValue(node, node.Nodes[i], key, value)
			break
		}
	}

	newNode := b.splitNode(node)
	if newNode != nil {
		if parent == nil {
			parent = NewBTreeNode[T](b.width, true)
			parent.addChild(node)
			b.root = parent
		}
		parent.addChild(newNode)
	}
}

func (b *BPTree[T]) splitNode(node *BPTreeNode[T]) *BPTreeNode[T] {
	half := b.width/2 + 1

	if len(node.Nodes) > b.width {
		newNode := NewBTreeNode[T](b.width, false)
		copy(newNode.Nodes, node.Nodes[half:len(node.Nodes)])
		newNode.MaxKey = newNode.Nodes[len(newNode.Nodes)-1].MaxKey

		node.Nodes = node.Nodes[0:half]
		node.MaxKey = node.Nodes[len(node.Nodes)-1].MaxKey
		return newNode
	} else if len(node.Items) > b.internalMax {
		newNode := NewBTreeNode[T](b.width, true)
		newNode.Items = append(newNode.Items, node.Items[half:len(node.Items)]...)
		newNode.MaxKey = newNode.Items[len(newNode.Items)-1].Key

		node.Next = newNode
		node.Items = node.Items[0:half]
		node.MaxKey = node.Items[len(node.Items)-1].Key
		return newNode

	}

	return nil
}
