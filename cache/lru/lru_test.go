package lru

import "testing"

func TestLRU(t *testing.T) {
	cache := &LRU[string, string]{}

	// Test empty cache lookup
	_, found := cache.Lookup("nonexistent")
	if found {
		t.Error("Should not find key in empty cache")
	}
	if cache.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", cache.Misses)
	}

	// Test insert
	inserted := cache.Insert("key1", "value1", 10)
	if !inserted {
		t.Error("Should report new insertion")
	}
	if cache.Size != 10 {
		t.Errorf("Expected size 10, got %d", cache.Size)
	}

	// Test lookup after insert
	value, found := cache.Lookup("key1")
	if !found {
		t.Error("Should find inserted key")
	}
	if value != "value1" {
		t.Errorf("Expected 'value1', got %q", value)
	}
	if cache.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", cache.Hits)
	}

	// Test update existing key
	inserted = cache.Insert("key1", "value1_updated", 15)
	if inserted {
		t.Error("Should not report insertion for update")
	}
	if cache.Size != 15 {
		t.Errorf("Expected size 15, got %d", cache.Size)
	}

	// Test delete
	cache.Delete("key1")
	if cache.Size != 0 {
		t.Errorf("Expected size 0 after delete, got %d", cache.Size)
	}
	if cache.Evictions != 1 {
		t.Errorf("Expected 1 eviction, got %d", cache.Evictions)
	}

	// Test delete non-existent key
	cache.Delete("nonexistent")
	if cache.Evictions != 1 {
		t.Errorf("Eviction count should not change for non-existent key, got %d", cache.Evictions)
	}
}

func TestList(t *testing.T) {
	l := &list[string]{}

	// Test empty list
	if elem := l.popBack(); elem != nil {
		t.Error("popBack on empty list should return nil")
	}

	// Test single element
	elem1 := &element[string]{item: "item1"}
	l.pushFront(elem1)

	if l.head != elem1 || l.tail != elem1 {
		t.Error("Single element should be both head and tail")
	}

	// Test adding second element
	elem2 := &element[string]{item: "item2"}
	l.pushFront(elem2)

	if l.head != elem2 || l.tail != elem1 {
		t.Error("Head should be elem2, tail should be elem1")
	}

	// Test moveToFront when already at front
	l.moveToFront(elem2)
	if l.head != elem2 {
		t.Error("moveToFront should not change head when element is already at front")
	}

	// Test moveToFront from tail
	l.moveToFront(elem1)
	if l.head != elem1 || l.tail != elem2 {
		t.Error("moveToFront should move tail to head")
	}

	// Test remove from middle (add third element first)
	elem3 := &element[string]{item: "item3"}
	l.pushFront(elem3)

	// Now we have: elem3 (head) -> elem1 -> elem2 (tail)
	l.remove(elem1) // Remove from middle

	if l.head != elem3 || l.tail != elem2 {
		t.Error("Removing middle element should connect head and tail")
	}
	if elem3.next != elem2 || elem2.prev != elem3 {
		t.Error("Links should be updated after middle removal")
	}

	// Test remove head
	l.remove(elem3)
	if l.head != elem2 || l.tail != elem2 {
		t.Error("After removing head, elem2 should be both head and tail")
	}

	// Test remove last element
	l.remove(elem2)
	if l.head != nil || l.tail != nil {
		t.Error("List should be empty after removing last element")
	}
}
