package Btree

type BPTreeItem[T is] struct {
	Key   int64
	Value T
}

type BPTreeNodeInterface[T is] interface {
	setNodeValue(key int64, value T)
}

type BPTreeNode[T is] struct {
	MaxKey int64
	Nodes  []*BPTreeNode[T]
	Items  []BPTreeItem[T]
	Next   *BPTreeNode[T]
}

func NewBTreeNode[T is](width int, isLeaf bool) *BPTreeNode[T] {
	node := &BPTreeNode[T]{}
	if isLeaf {
		node.Nodes = make([]*BPTreeNode[T], width+1)
		node.Nodes = node.Nodes[0:0]
	} else {
		node.Items = make([]BPTreeItem[T], width+1)
		node.Items = node.Items[0:0]
	}
	return node
}

func (node *BPTreeNode[T]) setNodeValue(key int64, value T) {
	item := BPTreeItem[T]{key, value}
	num := len(node.Items)
	if num < 1 || key > node.Items[num-1].Key {
		node.Items = append(node.Items, item)
		node.MaxKey = item.Key
		return
	} else if key < node.Items[0].Key {
		node.Items = append([]BPTreeItem[T]{item}, node.Items...)
		return
	}

	for i := 0; i < num; i++ {
		if node.Items[i].Key > key {
			node.Items = append(node.Items, BPTreeItem[T]{})
			copy(node.Items[i+1:], node.Items[i:])
			node.Items[i] = item
			return
		} else if node.Items[i].Key == key {
			node.Items[i] = item
			return
		}
	}
}

func (node *BPTreeNode[T]) addChild(child *BPTreeNode[T]) {
	num := len(node.Nodes)
	if num < 1 || child.MaxKey > node.Nodes[num-1].MaxKey {
		node.Nodes = append(node.Nodes, child)
		node.MaxKey = child.MaxKey
		return
	} else if child.MaxKey < node.Nodes[0].MaxKey {
		node.Nodes = append([]*BPTreeNode[T]{child}, node.Nodes...)
		return
	}

	for i := 0; i < num; i++ {
		if node.Nodes[i].MaxKey > child.MaxKey {
			node.Nodes = append(node.Nodes, nil)
			copy(node.Nodes[i+1:], node.Nodes[i:])
			node.Nodes[i] = child
			return
		}
	}
}
