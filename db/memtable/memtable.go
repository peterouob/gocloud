package memtable

type color int

const (
	black color = iota
	red
)

type Tree[K comparable, V comparable] struct {
	root       *Node[K, V]
	leaf       *Node[K, V]
	comparator Comparator[K]
	Size       int
}

type Node[K comparable, V comparable] struct {
	Key    K
	Value  V
	color  color
	left   *Node[K, V]
	right  *Node[K, V]
	parent *Node[K, V]
}

func NewTree[K comparable, V comparable](comparator Comparator[K]) *Tree[K, V] {
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
	} else if node == node.left {
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
	} else if node == node.parent.left {
		node.parent.left = left
	} else {
		node.parent.right = left
	}
	left.right = node
	node.parent = left
}
