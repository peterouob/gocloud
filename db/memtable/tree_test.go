package memtable

import (
	"fmt"
	"github.com/peterouob/gocloud/db/utils"
	"testing"
)

func TestTree(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	tree := NewTree[int, int](compare)
	tree.FindKey(1)
	tree.Insert(1, 1)
	tree.Insert(2, 2)
	tree.Insert(1, 2)
	tree.Insert(13, 23)
	tree.Insert(14, 21)
	tree.Insert(15, 20)

	tree.Delete(13)
	var keys []int
	var dkeys []int

	tree.TraverseNodes(func(node *Node[int, int]) {
		keys = append(keys, node.Key)
	}, func(node *Node[int, int]) {
		dkeys = append(dkeys, node.Key)
	})

	if dkeys[0] != 13 {
		t.Fatalf("need=%d,got=%d", 13, dkeys[0])
	}

	for _, v := range keys {
		t.Log(v)
	}
}

func TestRedBlackTreeInsertion(t *testing.T) {
	testCases := []struct {
		name              string
		insertions        []int
		expectedRootBlack bool
		expectedSize      int
	}{
		{
			name:              "Single Insertion",
			insertions:        []int{10},
			expectedRootBlack: true,
			expectedSize:      1,
		},
		{
			name:              "Multiple Insertions",
			insertions:        []int{10, 20, 30, 40, 50},
			expectedRootBlack: true,
			expectedSize:      5,
		},
		{
			name:              "Insertions with Recoloring",
			insertions:        []int{50, 30, 70, 20, 40, 60, 80},
			expectedRootBlack: true,
			expectedSize:      7,
		},
		{
			name:              "Insertions with Rotation",
			insertions:        []int{50, 30, 70, 20, 40, 10},
			expectedRootBlack: true,
			expectedSize:      6,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compare := &utils.OrderComparator[int]{}
			tree := NewTree[int, int](compare)

			for _, val := range tc.insertions {
				tree.Insert(val, val)
			}

			if tree.root.color != black {
				t.Errorf("Root should be black after insertions")
			}

			if tree.Size != tc.expectedSize {
				t.Errorf("Expected size %d, got %d", tc.expectedSize, tree.Size)
			}

			if err := validateRedBlackProperties(tree.root, tree.Leaf); err != nil {
				t.Errorf("Red-Black Tree property violation: %v", err)
			}
		})
	}
}

func validateRedBlackProperties(node *Node[int, int], leaf *Node[int, int]) error {
	_, err := checkBlackHeight(node, leaf)
	if err != nil {
		return err
	}

	return validateNoConsecutiveRed(node, false)
}

func checkBlackHeight(node *Node[int, int], leaf *Node[int, int]) (int, error) {
	if node == leaf {
		return 1, nil
	}

	leftBlackHeight, err := checkBlackHeight(node.left, leaf)
	if err != nil {
		return 0, err
	}

	rightBlackHeight, err := checkBlackHeight(node.right, leaf)
	if err != nil {
		return 0, err
	}

	if leftBlackHeight != rightBlackHeight {
		return 0, fmt.Errorf("black height inconsistency: left %d, right %d", leftBlackHeight, rightBlackHeight)
	}

	currentBlackHeight := leftBlackHeight
	if node.color == black {
		currentBlackHeight++
	}

	return currentBlackHeight, nil
}

func validateNoConsecutiveRed(node *Node[int, int], parentRed bool) error {
	if node == nil {
		return nil
	}

	if parentRed && node.color == red {
		return fmt.Errorf("consecutive red nodes found")
	}

	if node.left != nil {
		if err := validateNoConsecutiveRed(node.left, node.color == red); err != nil {
			return err
		}
	}

	if node.right != nil {
		if err := validateNoConsecutiveRed(node.right, node.color == red); err != nil {
			return err
		}
	}

	return nil
}

func TestTree_DeepCopy(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	tree := NewTree[int, int](compare)

	tree.Insert(10, 1)
	tree.Insert(20, 2)
	tree.Insert(5, 3)
	tree.Insert(15, 4)

	clonedTree := tree.DeepCopy()

	fmt.Println("Original Tree:")
	tree.TraverseNodes(func(node *Node[int, int]) {
		fmt.Printf("Key: %d, Value: %d\n", node.Key, node.Value)
	}, nil)

	fmt.Println("\nCloned Tree:")
	clonedTree.TraverseNodes(func(node *Node[int, int]) {
		fmt.Printf("Key: %d, Value: %d\n", node.Key, node.Value)
	}, nil)

	fmt.Println("\nModify Cloned Tree:")
	clonedTree.Insert(25, 1)
	clonedTree.Delete(10)

	fmt.Println("\nOriginal Tree After Modify Clone:")
	tree.TraverseNodes(func(node *Node[int, int]) {
		fmt.Printf("Key: %d, Value: %d\n", node.Key, node.Value)
	}, nil)

	fmt.Println("\nCloned Tree After Modify:")
	clonedTree.TraverseNodes(func(node *Node[int, int]) {
		fmt.Printf("Key: %d, Value: %d\n", node.Key, node.Value)
	}, nil)
}

func TestTreeNext(t *testing.T) {
	compare := &utils.OrderComparator[int]{}
	tree := NewTree[int, string](compare)

	tree.Insert(5, "five")
	tree.Insert(3, "three")
	tree.Insert(7, "seven")
	tree.Insert(1, "one")
	tree.Insert(4, "four")

	var keys []int
	var values []string

	for {
		nextNode, found := tree.Next(keys)
		if !found {
			break
		}

		keys = append(keys, nextNode.Key)
		values = append(values, nextNode.Value)

		t.Logf("Key: %v, Value: %v\n", nextNode.Key, nextNode.Value)
	}

	for _, key := range keys {
		t.Logf("Key: %v", key)
	}

	for _, value := range values {
		t.Logf("Value: %v", value)
	}
}
