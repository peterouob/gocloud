package Btree

import (
	"fmt"
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
		fmt.Println(node.Items)

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
	fmt.Println("root :", b.root)
	fmt.Println("nodes :", b.root.Nodes)
	fmt.Println("items :", b.root.Items)
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
	var newNode *BPTreeNode[T]

	if len(node.Nodes) > b.width {
		n := len(node.Nodes)
		b.split(node, newNode, n)
		return newNode
	} else if len(node.Items) > b.internalMax {
		b.isLeaf = false
		n := len(node.Items)
		b.split(node, newNode, n)
		return newNode
	}
	return nil
}

func (b *BPTree[T]) split(node *BPTreeNode[T], newNode *BPTreeNode[T], n int) {
	half := (b.width + 1) >> 1
	if b.isLeaf {
		newNode = NewBTreeNode[T](half, b.isLeaf)
		copy(newNode.Nodes, node.Nodes[half:])
		copy(node.Nodes, node.Nodes[:half])
	} else {
		newNode = NewBTreeNode[T](half, b.isLeaf)
		copy(newNode.Items, node.Items[half:])
		copy(node.Items, node.Items[:half])
	}
	newNode.MaxKey = newNode.Nodes[n-1].MaxKey
	node.MaxKey = node.Nodes[n-1].MaxKey
}
