package memtable

type color int

const (
	black color = iota
	red
)

type Tree[K any, V any] struct {
	root       *Node[K, V]
	leaf       *Node[K, V]
	comparator Comparator[K]
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

func NewTree[K any, V any](comparator Comparator[K]) *Tree[K, V] {
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
		newNode := &Node[K, V]{
			Key:    key,
			Value:  value,
			color:  black,
			parent: tree.leaf,
			left:   tree.leaf,
			right:  tree.leaf,
		}
		tree.root = newNode
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

	newNode := &Node[K, V]{
		Key:    key,
		Value:  value,
		color:  red,
		parent: parent,
		left:   tree.leaf,
		right:  tree.leaf,
	}

	if tree.comparator.Compare(key, parent.Key) < 0 {
		parent.left = newNode
	} else {
		parent.right = newNode
	}

	tree.Size++
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
	return tree.leaf
}

func (tree *Tree[K, V]) Delete(key K) {
	if tree.leaf == nil {
		return
	}
	if node := tree.FindKey(key); node != nil {
		node.isDelete = true
	}
}
