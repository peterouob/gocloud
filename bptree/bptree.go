package btree

import (
	"fmt"
	"sync"
)

const (
	MaxOrder = 3
)

type Comparable interface {
	~int | ~int64 | ~string | ~float64
}

type BPTreeInterface[T Comparable] interface {
	Insert(key T, value interface{})
	Delete(key T)
	Get(key T) []interface{}
	Range(start, end T) []interface{}
	Update(key T, value interface{})
}

type BPTree[T Comparable] struct {
	mutex sync.RWMutex
	root  *BPTreeNode[T]
	order int
}

type BPTreeNode[T Comparable] struct {
	MaxKey     T
	Nodes      []*BPTreeNode[T]
	Items      []BPTreeItem[T]
	Next       *BPTreeNode[T]
	IsLeaf     bool
	ParentNode *BPTreeNode[T]
}

type BPTreeItem[T Comparable] struct {
	Key   T
	Value interface{}
}

func NewBPTree[T Comparable](order int) *BPTree[T] {
	if order < 3 {
		order = MaxOrder
	}
	root := NewBTreeNode[T](order, true)
	return &BPTree[T]{
		root:  root,
		order: order,
	}
}

func NewBTreeNode[T Comparable](order int, isLeaf bool) *BPTreeNode[T] {
	node := &BPTreeNode[T]{
		IsLeaf: isLeaf,
	}
	if isLeaf {
		node.Items = make([]BPTreeItem[T], 0, order)
	} else {
		node.Nodes = make([]*BPTreeNode[T], 0, order+1)
	}
	return node
}

// Find correct child node for insertion
func (b *BPTree[T]) findChildNode(node *BPTreeNode[T], key T) *BPTreeNode[T] {
	if node.IsLeaf {
		return node
	}

	for i, childNode := range node.Nodes {
		if key <= childNode.MaxKey {
			return b.findChildNode(childNode, key)
		}
		if i == len(node.Nodes)-1 {
			return b.findChildNode(childNode, key)
		}
	}
	return node.Nodes[len(node.Nodes)-1]
}

// Insert implements insertion with splitting
func (b *BPTree[T]) Insert(key T, value interface{}) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.root == nil {
		b.root = NewBTreeNode[T](b.order, true)
	}

	leafNode := b.findChildNode(b.root, key)
	b.insertIntoLeaf(leafNode, key, value)

	// Handle root splitting
	if len(b.root.Nodes) > b.order {
		b.splitRoot()
	}
}

func (b *BPTree[T]) insertIntoLeaf(node *BPTreeNode[T], key T, value interface{}) {
	// Find insertion point
	insertIndex := 0
	for insertIndex < len(node.Items) && node.Items[insertIndex].Key < key {
		insertIndex++
	}

	// Insert or update
	if insertIndex < len(node.Items) && node.Items[insertIndex].Key == key {
		// Append to existing values for the key
		node.Items[insertIndex].Value = value
	} else {
		// Create new item with initial value
		newItem := BPTreeItem[T]{
			Key:   key,
			Value: value,
		}

		// Insert the new item
		node.Items = append(node.Items, BPTreeItem[T]{})
		copy(node.Items[insertIndex+1:], node.Items[insertIndex:])
		node.Items[insertIndex] = newItem
	}

	// Update max key
	if len(node.Items) > 0 {
		node.MaxKey = node.Items[len(node.Items)-1].Key
	}

	// Handle node splitting
	if len(node.Items) > b.order {
		b.splitLeafNode(node)
	}
}

func (b *BPTree[T]) splitLeafNode(node *BPTreeNode[T]) {
	midIndex := len(node.Items) / 2
	newNode := NewBTreeNode[T](b.order, true)

	// Copy second half of items to new node
	newNode.Items = append(newNode.Items, node.Items[midIndex:]...)
	node.Items = node.Items[:midIndex]

	// Update max keys
	newNode.MaxKey = newNode.Items[len(newNode.Items)-1].Key
	node.MaxKey = node.Items[len(node.Items)-1].Key

	// Link leaf nodes
	newNode.Next = node.Next
	node.Next = newNode

	// Propagate to parent
	b.insertIntoParent(node, newNode)
}

func (b *BPTree[T]) insertIntoParent(leftNode, rightNode *BPTreeNode[T]) {
	if leftNode.ParentNode == nil {
		// Create new root if no parent exists
		newRoot := NewBTreeNode[T](b.order, false)
		newRoot.Nodes = append(newRoot.Nodes, leftNode, rightNode)
		newRoot.MaxKey = rightNode.MaxKey
		leftNode.ParentNode = newRoot
		rightNode.ParentNode = newRoot
		b.root = newRoot
		return
	}

	parentNode := leftNode.ParentNode
	insertIndex := 0
	for insertIndex < len(parentNode.Nodes) && parentNode.Nodes[insertIndex].MaxKey < rightNode.MaxKey {
		insertIndex++
	}

	parentNode.Nodes = append(parentNode.Nodes, nil)
	copy(parentNode.Nodes[insertIndex+1:], parentNode.Nodes[insertIndex:])
	parentNode.Nodes[insertIndex] = rightNode
	rightNode.ParentNode = parentNode

	// Update parent's max key
	parentNode.MaxKey = parentNode.Nodes[len(parentNode.Nodes)-1].MaxKey

	// Handle parent node splitting if needed
	if len(parentNode.Nodes) > b.order {
		b.splitParentNode(parentNode)
	}
}

func (b *BPTree[T]) splitParentNode(node *BPTreeNode[T]) {
	midIndex := len(node.Nodes) / 2
	newNode := NewBTreeNode[T](b.order, false)

	// Copy second half of nodes to new node
	newNode.Nodes = append(newNode.Nodes, node.Nodes[midIndex:]...)
	node.Nodes = node.Nodes[:midIndex]

	// Update parent references for split nodes
	for _, childNode := range newNode.Nodes {
		childNode.ParentNode = newNode
	}

	// Update max keys
	newNode.MaxKey = newNode.Nodes[len(newNode.Nodes)-1].MaxKey
	node.MaxKey = node.Nodes[len(node.Nodes)-1].MaxKey

	// Propagate to parent
	b.insertIntoParent(node, newNode)
}

func (b *BPTree[T]) splitRoot() {
	midIndex := len(b.root.Nodes) / 2
	newRoot := NewBTreeNode[T](b.order, false)

	leftNodes := b.root.Nodes[:midIndex]
	rightNodes := b.root.Nodes[midIndex:]

	newRoot.Nodes = append(newRoot.Nodes, leftNodes[len(leftNodes)-1], rightNodes[0])
	newRoot.MaxKey = rightNodes[0].MaxKey

	// Update parent references
	for _, node := range leftNodes {
		node.ParentNode = newRoot
	}
	for _, node := range rightNodes {
		node.ParentNode = newRoot
	}

	b.root = newRoot
}

// Get retrieves values for a given key
func (b *BPTree[T]) Get(key T) []interface{} {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	leafNode := b.findChildNode(b.root, key)
	var results []interface{}

	for _, item := range leafNode.Items {
		if item.Key == key {
			results = append(results, item.Value)
		}
	}

	return results
}

// Delete removes a key from the B+ Tree
func (b *BPTree[T]) Delete(key T) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	leafNode := b.findChildNode(b.root, key)
	b.deleteFromLeaf(leafNode, key)
}

func (b *BPTree[T]) deleteFromLeaf(node *BPTreeNode[T], key T) {
	for i, item := range node.Items {
		if item.Key == key {
			// Remove the item
			node.Items = append(node.Items[:i], node.Items[i+1:]...)

			// Update max key
			if len(node.Items) > 0 {
				node.MaxKey = node.Items[len(node.Items)-1].Key
			}

			// Handle underflow
			if len(node.Items) < b.order/2 {
				b.rebalanceLeaf(node)
			}
			return
		}
	}
}

func (b *BPTree[T]) rebalanceLeaf(node *BPTreeNode[T]) {
	// Implement more sophisticated rebalancing logic
	// This is a simplified version
	if node.Next != nil && len(node.Next.Items) > b.order/2 {
		// Borrow from right sibling
		borrowedItem := node.Next.Items[0]
		node.Items = append(node.Items, borrowedItem)
		node.Next.Items = node.Next.Items[1:]

		// Update max keys
		node.MaxKey = node.Items[len(node.Items)-1].Key
		node.Next.MaxKey = node.Next.Items[len(node.Next.Items)-1].Key
	} else if node.ParentNode != nil {
		// Merge with sibling
		parentIndex := 0
		for i, n := range node.ParentNode.Nodes {
			if n == node {
				parentIndex = i
				break
			}
		}

		if parentIndex > 0 {
			leftSibling := node.ParentNode.Nodes[parentIndex-1]
			leftSibling.Items = append(leftSibling.Items, node.Items...)
			leftSibling.MaxKey = leftSibling.Items[len(leftSibling.Items)-1].Key

			// Remove node from parent
			node.ParentNode.Nodes = append(node.ParentNode.Nodes[:parentIndex], node.ParentNode.Nodes[parentIndex+1:]...)
		}
	}
}

// Range retrieves values within a key range
func (b *BPTree[T]) Range(start, end T) []interface{} {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	var results []interface{}
	leafNode := b.findChildNode(b.root, start)

	for leafNode != nil {
		for _, item := range leafNode.Items {
			if item.Key >= start && item.Key <= end {
				results = append(results, item.Value)
			}
			if item.Key > end {
				return results
			}
		}
		leafNode = leafNode.Next
	}

	return results
}

// Update modifies the value of an existing key
func (b *BPTree[T]) Update(key T, value interface{}) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	leafNode := b.findChildNode(b.root, key)
	for i, item := range leafNode.Items {
		if item.Key == key {
			leafNode.Items[i].Value = value
			return
		}
	}
}

// Debug methods for tree introspection
func (b *BPTree[T]) PrintTree() {
	b.printNode(b.root, 0)
}

func (b *BPTree[T]) printNode(node *BPTreeNode[T], level int) {
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	if node.IsLeaf {
		fmt.Printf("%sLeaf Node (Max: %v): ", indent, node.MaxKey)
		for _, item := range node.Items {
			fmt.Printf("(%v:%v) ", item.Key, item.Value)
		}
		fmt.Println()
	} else {
		fmt.Printf("%sInternal Node (Max: %v)\n", indent, node.MaxKey)
		for _, childNode := range node.Nodes {
			b.printNode(childNode, level+1)
		}
	}
}
