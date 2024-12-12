package memtable

import (
	"github.com/peterouob/gocloud/db/utils"
)

type color int

const (
	black color = iota
	red
)

type RedBlackTree[K any, V any] interface {
	FindKey(K) *Node[K, V]
	Insert(K, V)
	Delete(K)
	TraverseNodes(func(*Node[K, V]), func(*Node[K, V]))

	leftRotation(*Node[K, V])
	rightRotation(*Node[K, V])
	fixAfterInsert(*Node[K, V])
}

var _ RedBlackTree[int, int] = (*Tree[int, int])(nil)

type Tree[K any, V any] struct {
	root       *Node[K, V]
	leaf       *Node[K, V]
	comparator utils.Comparator[K]
	Size       int
}

type Node[K any, V any] struct {
	Key      K
	Value    V
	color    color
	left     *Node[K, V]
	right    *Node[K, V]
	parent   *Node[K, V]
	isDelete bool
}

func NewTree[K any, V any](comparator utils.Comparator[K]) *Tree[K, V] {
	tree := new(Tree[K, V])
	tree.leaf = &Node[K, V]{color: black}
	tree.root = tree.leaf
	tree.comparator = comparator
	return tree
}

func (tree *Tree[K, V]) leftRotation(node *Node[K, V]) {
	right := node.right
	node.right = right.left
	if right.left != tree.leaf {
		right.left.parent = node
	}
	right.parent = node.parent
	if node.parent == tree.leaf {
		tree.root = right
	} else if node == node.parent.left {
		node.parent.left = right
	} else {
		node.parent.right = right
	}
	right.left = node
	node.parent = right
}

func (tree *Tree[K, V]) rightRotation(node *Node[K, V]) {
	left := node.left
	node.left = left.right
	if left.right != tree.leaf {
		left.right.parent = node
	}
	left.parent = node.parent
	if node.parent == tree.leaf {
		tree.root = left
	} else if node == node.parent.right {
		node.parent.right = left
	} else {
		node.parent.left = left
	}
	left.right = node
	node.parent = left
}

func (tree *Tree[K, V]) Insert(key K, value V) {
	if tree.root == tree.leaf {
		node := &Node[K, V]{
			Key:    key,
			Value:  value,
			color:  black,
			parent: tree.leaf,
			left:   tree.leaf,
			right:  tree.leaf,
		}
		tree.root = node
		tree.Size++
		return
	}
	parent := tree.leaf
	cur := tree.root
	for cur != tree.leaf {
		parent = cur
		cmpResult := tree.comparator.Compare(key, cur.Key)
		switch {
		case cmpResult < 0:
			cur = cur.left
		case cmpResult > 0:
			cur = cur.right
		default:
			cur.Value = value
			return
		}
	}

	node := &Node[K, V]{
		Key:    key,
		Value:  value,
		color:  red,
		parent: parent,
		left:   tree.leaf,
		right:  tree.leaf,
	}

	if tree.comparator.Compare(key, parent.Key) < 0 {
		parent.left = node
	} else {
		parent.right = node
	}
	tree.Size++
	tree.fixAfterInsert(node)
}

func (tree *Tree[K, V]) fixAfterInsert(node *Node[K, V]) {
	node.color = red

	for node != tree.root && node.parent.color == red {
		if node.parent == node.parent.parent.left {
			uncle := node.parent.parent.right

			if uncle.color == red {
				node.parent.color = black
				uncle.color = black
				node.parent.parent.color = red
				node = node.parent.parent
			} else {
				if node == node.parent.right {
					node = node.parent
					tree.leftRotation(node)
				}

				node.parent.color = black
				node.parent.parent.color = red
				tree.rightRotation(node.parent.parent)
			}
		} else {
			uncle := node.parent.parent.left

			if uncle.color == red {
				node.parent.color = black
				uncle.color = black
				node.parent.parent.color = red
				node = node.parent.parent

			} else {
				if node == node.parent.left {
					node = node.parent
					tree.rightRotation(node)
				}

				node.parent.color = black
				node.parent.parent.color = red
				tree.leftRotation(node.parent.parent)
			}
		}
	}
	tree.root.color = black
}

func (tree *Tree[K, V]) FindKey(key K) *Node[K, V] {
	cur := tree.root
	for cur != tree.leaf {
		cmpResult := tree.comparator.Compare(key, cur.Key)
		switch {
		case cmpResult > 0:
			cur = cur.right
		case cmpResult < 0:
			cur = cur.left
		default:
			return cur
		}
	}
	return nil
}

func (tree *Tree[K, V]) Delete(key K) {
	if tree.leaf == nil {
		return
	}
	if node := tree.FindKey(key); node != nil {
		node.isDelete = true
	}
}

func (tree *Tree[K, V]) TraverseNodes(fn func(node *Node[K, V]), dfn func(node *Node[K, V])) {
	if tree.root == tree.leaf {
		return
	}
	var traverse func(node *Node[K, V])
	traverse = func(node *Node[K, V]) {
		if node == tree.leaf {
			return
		}
		traverse(node.left)
		if !node.isDelete && fn != nil {
			fn(node)
		} else if node.isDelete && dfn != nil {
			dfn(node)
		}
		traverse(node.right)
	}
	traverse(tree.root)
}

func (tree *Tree[K, V]) TraverseNodesWithoutDelete(fn func(node *Node[K, V])) {
	if tree.root == tree.leaf {
		return
	}
	var traverse func(node *Node[K, V])
	traverse = func(node *Node[K, V]) {
		if node == tree.leaf {
			return
		}
		traverse(node.left)
		if !node.isDelete && fn != nil {
			fn(node)
		}
		traverse(node.right)
	}
	traverse(tree.root)
}

func (tree *Tree[K, V]) DeepCopy() *Tree[K, V] {
	if tree == nil {
		return nil
	}

	if tree.leaf == nil {
		tree.leaf = &Node[K, V]{color: black}
	}
	newTree := NewTree[K, V](tree.comparator)
	newTree.root = deepCopyNode(tree.root, tree.leaf, newTree.leaf)

	newTree.Size = tree.Size

	return newTree
}

func deepCopyNode[K, V any](node, sourceLeaf, targetLeaf *Node[K, V]) *Node[K, V] {
	if node == nil || node == sourceLeaf {
		return targetLeaf
	}
	cloned := &Node[K, V]{
		Key:      node.Key,
		Value:    node.Value,
		color:    node.color,
		isDelete: node.isDelete,
	}
	cloned.left = deepCopyNode(node.left, sourceLeaf, targetLeaf)
	cloned.right = deepCopyNode(node.right, sourceLeaf, targetLeaf)
	return cloned
}
