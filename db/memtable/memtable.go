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

func (tree *Tree[K, V]) leftRotation(node *Node[K, V])  {}
func (tree *Tree[K, V]) rightRotation(node *Node[K, V]) {}
