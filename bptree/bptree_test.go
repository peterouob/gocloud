package btree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestBasicInsertion(t *testing.T) {
	tree := NewBPTree[int](4)

	testCases := []struct {
		key   int
		value string
	}{
		{10, "ten"},
		{20, "twenty"},
		{30, "thirty"},
		{15, "fifteen"},
		{5, "five"},
	}

	for _, tc := range testCases {
		tree.Insert(tc.key, tc.value)
	}

	for _, tc := range testCases {
		values := tree.Get(tc.key)
		if len(values) == 0 {
			t.Errorf("Failed to retrieve value for key %d", tc.key)
		}
		if values[0] != tc.value {
			t.Errorf("Incorrect value for key %d. Expected %s, got %s",
				tc.key, tc.value, values[0])
		}
	}
}

func TestDuplicateInsertion(t *testing.T) {
	tree := NewBPTree[int](4)

	// Insert multiple values for same key
	tree.Insert(10, "first")
	tree.Insert(10, "second")
	tree.Insert(10, "third")

	values := tree.Get(10)

	if values[0] != "third" {
		t.Errorf("Incorrect value for key %d. Expected %s, got %s", 10, values, "third")
	}

}

func TestUpdate(t *testing.T) {
	tree := NewBPTree[int](4)

	tree.Insert(10, "original")
	//tree.Update(10, "updated")

	values := tree.Get(10)
	if len(values) != 1 || values[0] != "updated" {
		t.Errorf("Update failed. Expected 'updated', got %v", values)
	}
}

func TestDelete(t *testing.T) {
	tree := NewBPTree[int](4)

	// Insert multiple elements
	testKeys := []int{10, 20, 30, 40, 50}
	for _, key := range testKeys {
		tree.Insert(key, fmt.Sprintf("value_%d", key))
	}

	// Delete an element
	tree.Delete(30)

	// Verify deletion
	values := tree.Get(30)
	if len(values) != 0 {
		t.Errorf("Delete failed. Key 30 should not exist")
	}
}

func TestRangeQuery(t *testing.T) {
	tree := NewBPTree[int](4)

	// Insert test data
	testData := []int{5, 10, 15, 20, 25, 30, 35, 40}
	for _, key := range testData {
		tree.Insert(key, fmt.Sprintf("value_%d", key))
	}

	// Test range query
	rangeValues := tree.Range(15, 35)
	expectedKeys := []int{15, 20, 25, 30, 35}

	if len(rangeValues) != len(expectedKeys) {
		t.Errorf("Range query failed. Expected %d values, got %d",
			len(expectedKeys), len(rangeValues))
	}
}

func TestLargeDataset(t *testing.T) {
	tree := NewBPTree[int](4)

	// Generate random dataset
	datasetSize := 1000
	insertedData := make(map[int]string)

	for i := 0; i < datasetSize; i++ {
		key := rand.Intn(10000)
		value := fmt.Sprintf("value_%d", key)
		tree.Insert(key, value)
		insertedData[key] = value
	}

	// Verify all inserted data
	for key, expectedValue := range insertedData {
		values := tree.Get(key)
		if len(values) == 0 {
			t.Errorf("Failed to retrieve value for key %d", key)
		}
		found := false
		for _, v := range values {
			if v == expectedValue {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Incorrect value for key %d", key)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	tree := NewBPTree[int](4)

	// Concurrent insertion
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(start int) {
			for j := 0; j < 100; j++ {
				key := start + j
				tree.Insert(key, fmt.Sprintf("value_%d", key))
			}
			done <- true
		}(i * 100)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify total number of insertions
	totalInsertions := 100 * 100
	insertedKeys := make([]int, 0)

	for i := 0; i < totalInsertions; i++ {
		values := tree.Get(i)
		if len(values) > 0 {
			insertedKeys = append(insertedKeys, i)
		}
	}

	if len(insertedKeys) != totalInsertions {
		t.Errorf("Concurrent insertion failed. Expected %d keys, found %d",
			totalInsertions, len(insertedKeys))
	}
}

func TestStringKeyBPTree(t *testing.T) {
	tree := NewBPTree[string](4)

	testData := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testData {
		tree.Insert(key, fmt.Sprintf("value_%s", key))
	}

	// Sort test data for range query
	sort.Strings(testData)

	// Test range query with strings
	rangeValues := tree.Range("banana", "date")

	if len(rangeValues) != 3 {
		t.Errorf("String range query failed. Expected 3 values, got %d", len(rangeValues))
	}
}

func BenchmarkInsertion(b *testing.B) {
	tree := NewBPTree[int](4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Insert(i, fmt.Sprintf("value_%d", i))
	}
}

func BenchmarkSearch(b *testing.B) {
	tree := NewBPTree[int](4)

	// Prepare tree with data
	for i := 0; i < 10000; i++ {
		tree.Insert(i, fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(rand.Intn(10000))
	}
}
