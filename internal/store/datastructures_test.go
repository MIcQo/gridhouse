package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// List Tests

func TestNewList(t *testing.T) {
	list := NewList()
	assert.NotNil(t, list)
	assert.Equal(t, 0, list.LLen())
}

func TestListLPush(t *testing.T) {
	list := NewList()

	// Test single element
	length := list.LPush("a")
	assert.Equal(t, 1, length)
	assert.Equal(t, 1, list.LLen())

	// Test multiple elements
	length = list.LPush("b", "c")
	assert.Equal(t, 3, length)
	assert.Equal(t, 3, list.LLen())

	// Verify order (LPUSH adds to left, so newest first)
	elements := list.LRange(0, -1)
	assert.Equal(t, []string{"b", "c", "a"}, elements)
}

func TestListRPush(t *testing.T) {
	list := NewList()

	// Test single element
	length := list.RPush("a")
	assert.Equal(t, 1, length)
	assert.Equal(t, 1, list.LLen())

	// Test multiple elements
	length = list.RPush("b", "c")
	assert.Equal(t, 3, length)
	assert.Equal(t, 3, list.LLen())

	// Verify order (RPUSH adds to right, so oldest first)
	elements := list.LRange(0, -1)
	assert.Equal(t, []string{"a", "b", "c"}, elements)
}

func TestListLPop(t *testing.T) {
	list := NewList()

	// Test empty list
	element, exists := list.LPop()
	assert.False(t, exists)
	assert.Equal(t, "", element)

	// Test with elements
	list.LPush("a", "b", "c")

	element, exists = list.LPop()
	assert.True(t, exists)
	assert.Equal(t, "a", element)
	assert.Equal(t, 2, list.LLen())

	element, exists = list.LPop()
	assert.True(t, exists)
	assert.Equal(t, "b", element)
	assert.Equal(t, 1, list.LLen())
}

func TestListRPop(t *testing.T) {
	list := NewList()

	// Test empty list
	element, exists := list.RPop()
	assert.False(t, exists)
	assert.Equal(t, "", element)

	// Test with elements
	list.RPush("a", "b", "c")

	element, exists = list.RPop()
	assert.True(t, exists)
	assert.Equal(t, "c", element)
	assert.Equal(t, 2, list.LLen())

	element, exists = list.RPop()
	assert.True(t, exists)
	assert.Equal(t, "b", element)
	assert.Equal(t, 1, list.LLen())
}

func TestListLLen(t *testing.T) {
	list := NewList()
	assert.Equal(t, 0, list.LLen())

	list.LPush("a")
	assert.Equal(t, 1, list.LLen())

	list.RPush("b", "c")
	assert.Equal(t, 3, list.LLen())

	list.LPop()
	assert.Equal(t, 2, list.LLen())
}

func TestListLRange(t *testing.T) {
	list := NewList()
	list.RPush("a", "b", "c", "d", "e")

	// Test full range
	elements := list.LRange(0, -1)
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, elements)

	// Test partial range
	elements = list.LRange(1, 3)
	assert.Equal(t, []string{"b", "c", "d"}, elements)

	// Test negative indices
	elements = list.LRange(-3, -1)
	assert.Equal(t, []string{"c", "d", "e"}, elements)

	// Test out of bounds
	elements = list.LRange(10, 15)
	assert.Equal(t, []string{}, elements)

	// Test invalid range
	elements = list.LRange(3, 1)
	assert.Equal(t, []string{}, elements)
}

func TestListLIndex(t *testing.T) {
	list := NewList()
	list.RPush("a", "b", "c", "d")

	// Test valid indices
	element, exists := list.LIndex(0)
	assert.True(t, exists)
	assert.Equal(t, "a", element)

	element, exists = list.LIndex(2)
	assert.True(t, exists)
	assert.Equal(t, "c", element)

	// Test negative indices
	element, exists = list.LIndex(-1)
	assert.True(t, exists)
	assert.Equal(t, "d", element)

	element, exists = list.LIndex(-2)
	assert.True(t, exists)
	assert.Equal(t, "c", element)

	// Test out of bounds
	element, exists = list.LIndex(10)
	assert.False(t, exists)
	assert.Equal(t, "", element)

	element, exists = list.LIndex(-10)
	assert.False(t, exists)
	assert.Equal(t, "", element)
}

func TestListLSet(t *testing.T) {
	list := NewList()
	list.RPush("a", "b", "c")

	// Test valid set
	success := list.LSet(1, "x")
	assert.True(t, success)

	element, exists := list.LIndex(1)
	assert.True(t, exists)
	assert.Equal(t, "x", element)

	// Test negative index
	success = list.LSet(-1, "z")
	assert.True(t, success)

	element, exists = list.LIndex(-1)
	assert.True(t, exists)
	assert.Equal(t, "z", element)

	// Test out of bounds
	success = list.LSet(10, "invalid")
	assert.False(t, success)
}

func TestListLRem(t *testing.T) {
	list := NewList()
	list.RPush("a", "b", "a", "c", "a", "d")

	// Test removing specific count
	removed := list.LRem(2, "a")
	assert.Equal(t, 2, removed)
	assert.Equal(t, 4, list.LLen())

	elements := list.LRange(0, -1)
	assert.Equal(t, []string{"b", "c", "a", "d"}, elements)

	// Test removing all remaining
	removed = list.LRem(0, "a")
	assert.Equal(t, 1, removed)
	assert.Equal(t, 3, list.LLen())

	// Test removing from right
	list.RPush("x", "y", "x")
	removed = list.LRem(-1, "x")
	assert.Equal(t, 1, removed)
}

func TestListLTrim(t *testing.T) {
	list := NewList()
	list.RPush("a", "b", "c", "d", "e")

	// Test normal trim
	list.LTrim(1, 3)
	assert.Equal(t, 3, list.LLen())
	elements := list.LRange(0, -1)
	assert.Equal(t, []string{"b", "c", "d"}, elements)

	// Test negative indices
	list.RPush("f", "g", "h")
	list.LTrim(-3, -1)
	assert.Equal(t, 3, list.LLen())
	elements = list.LRange(0, -1)
	assert.Equal(t, []string{"f", "g", "h"}, elements)

	// Test out of bounds
	list.LTrim(10, 15)
	assert.Equal(t, 0, list.LLen())
}

// Set Tests

func TestNewSet(t *testing.T) {
	set := NewSet()
	assert.NotNil(t, set)
	assert.Equal(t, 0, set.SCard())
}

func TestSetSAdd(t *testing.T) {
	set := NewSet()

	// Test single element
	added := set.SAdd("a")
	assert.Equal(t, 1, added)
	assert.Equal(t, 1, set.SCard())

	// Test multiple elements
	added = set.SAdd("b", "c", "d")
	assert.Equal(t, 3, added)
	assert.Equal(t, 4, set.SCard())

	// Test duplicate elements
	added = set.SAdd("a", "b", "e")
	assert.Equal(t, 1, added) // only 'e' was new
	assert.Equal(t, 5, set.SCard())
}

func TestSetSRem(t *testing.T) {
	set := NewSet()
	set.SAdd("a", "b", "c", "d")

	// Test removing existing elements
	removed := set.SRem("a", "c")
	assert.Equal(t, 2, removed)
	assert.Equal(t, 2, set.SCard())

	// Test removing non-existent elements
	removed = set.SRem("x", "y")
	assert.Equal(t, 0, removed)
	assert.Equal(t, 2, set.SCard())

	// Test removing mixed existing/non-existing
	removed = set.SRem("b", "x", "d")
	assert.Equal(t, 2, removed)
	assert.Equal(t, 0, set.SCard())
}

func TestSetSIsMember(t *testing.T) {
	set := NewSet()
	set.SAdd("a", "b", "c")

	// Test existing members
	assert.True(t, set.SIsMember("a"))
	assert.True(t, set.SIsMember("b"))
	assert.True(t, set.SIsMember("c"))

	// Test non-existing members
	assert.False(t, set.SIsMember("x"))
	assert.False(t, set.SIsMember(""))
}

func TestSetSMembers(t *testing.T) {
	set := NewSet()

	// Test empty set
	members := set.SMembers()
	assert.Equal(t, []string{}, members)

	// Test with elements
	set.SAdd("a", "b", "c")
	members = set.SMembers()
	assert.Len(t, members, 3)
	assert.Contains(t, members, "a")
	assert.Contains(t, members, "b")
	assert.Contains(t, members, "c")
}

func TestSetSCard(t *testing.T) {
	set := NewSet()
	assert.Equal(t, 0, set.SCard())

	set.SAdd("a")
	assert.Equal(t, 1, set.SCard())

	set.SAdd("b", "c")
	assert.Equal(t, 3, set.SCard())

	set.SRem("a")
	assert.Equal(t, 2, set.SCard())
}

func TestSetSPop(t *testing.T) {
	set := NewSet()

	// Test empty set
	element, exists := set.SPop()
	assert.False(t, exists)
	assert.Equal(t, "", element)

	// Test with elements
	set.SAdd("a", "b", "c")

	element, exists = set.SPop()
	assert.True(t, exists)
	assert.Contains(t, []string{"a", "b", "c"}, element)
	assert.Equal(t, 2, set.SCard())

	_, exists = set.SPop()
	assert.True(t, exists)
	assert.Equal(t, 1, set.SCard())
}

// Hash Tests

func TestNewHash(t *testing.T) {
	hash := NewHash()
	assert.NotNil(t, hash)
	assert.Equal(t, 0, hash.HLen())
}

func TestHashHSet(t *testing.T) {
	hash := NewHash()

	// Test single field
	isNew := hash.HSet("field1", "value1")
	assert.True(t, isNew)
	assert.Equal(t, 1, hash.HLen())

	// Test updating existing field
	isNew = hash.HSet("field1", "newvalue")
	assert.False(t, isNew)
	assert.Equal(t, 1, hash.HLen())

	// Test multiple fields
	hash.HSet("field2", "value2")
	hash.HSet("field3", "value3")
	assert.Equal(t, 3, hash.HLen())
}

func TestHashHGet(t *testing.T) {
	hash := NewHash()
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")

	// Test existing fields
	value, exists := hash.HGet("field1")
	assert.True(t, exists)
	assert.Equal(t, "value1", value)

	value, exists = hash.HGet("field2")
	assert.True(t, exists)
	assert.Equal(t, "value2", value)

	// Test non-existing field
	value, exists = hash.HGet("field3")
	assert.False(t, exists)
	assert.Equal(t, "", value)
}

func TestHashHDel(t *testing.T) {
	hash := NewHash()
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")
	hash.HSet("field3", "value3")

	// Test removing existing fields
	removed := hash.HDel("field1", "field3")
	assert.Equal(t, 2, removed)
	assert.Equal(t, 1, hash.HLen())

	// Test removing non-existing fields
	removed = hash.HDel("field4", "field5")
	assert.Equal(t, 0, removed)
	assert.Equal(t, 1, hash.HLen())

	// Test removing mixed existing/non-existing
	removed = hash.HDel("field2", "field4")
	assert.Equal(t, 1, removed)
	assert.Equal(t, 0, hash.HLen())
}

func TestHashHExists(t *testing.T) {
	hash := NewHash()
	hash.HSet("field1", "value1")

	// Test existing field
	assert.True(t, hash.HExists("field1"))

	// Test non-existing field
	assert.False(t, hash.HExists("field2"))
}

func TestHashHGetAll(t *testing.T) {
	hash := NewHash()

	// Test empty hash
	fields := hash.HGetAll()
	assert.Equal(t, map[string]string{}, fields)

	// Test with fields
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")
	fields = hash.HGetAll()
	assert.Equal(t, map[string]string{
		"field1": "value1",
		"field2": "value2",
	}, fields)
}

func TestHashHKeys(t *testing.T) {
	hash := NewHash()

	// Test empty hash
	keys := hash.HKeys()
	assert.Equal(t, []string{}, keys)

	// Test with fields
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")
	keys = hash.HKeys()
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "field1")
	assert.Contains(t, keys, "field2")
}

func TestHashHVals(t *testing.T) {
	hash := NewHash()

	// Test empty hash
	values := hash.HVals()
	assert.Equal(t, []string{}, values)

	// Test with fields
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")
	values = hash.HVals()
	assert.Len(t, values, 2)
	assert.Contains(t, values, "value1")
	assert.Contains(t, values, "value2")
}

func TestHashHLen(t *testing.T) {
	hash := NewHash()
	assert.Equal(t, 0, hash.HLen())

	hash.HSet("field1", "value1")
	assert.Equal(t, 1, hash.HLen())

	hash.HSet("field2", "value2")
	hash.HSet("field3", "value3")
	assert.Equal(t, 3, hash.HLen())

	hash.HDel("field1")
	assert.Equal(t, 2, hash.HLen())
}

func TestHashHIncrBy(t *testing.T) {
	hash := NewHash()

	// Test incrementing non-existing field
	value, err := hash.HIncrBy("counter", 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), value)

	// Test incrementing existing field
	value, err = hash.HIncrBy("counter", 3)
	require.NoError(t, err)
	assert.Equal(t, int64(8), value)

	// Test with negative increment
	value, err = hash.HIncrBy("counter", -2)
	require.NoError(t, err)
	assert.Equal(t, int64(6), value)

	// Test with non-numeric field
	hash.HSet("text", "not-a-number")
	_, err = hash.HIncrBy("text", 1)
	assert.Error(t, err)
}

func TestHashHIncrByFloat(t *testing.T) {
	hash := NewHash()

	// Test incrementing non-existing field
	value, err := hash.HIncrByFloat("float", 1.5)
	require.NoError(t, err)
	assert.Equal(t, 1.5, value)

	// Test incrementing existing field
	value, err = hash.HIncrByFloat("float", 2.3)
	require.NoError(t, err)
	assert.Equal(t, 3.8, value)

	// Test with negative increment
	value, err = hash.HIncrByFloat("float", -0.5)
	require.NoError(t, err)
	assert.Equal(t, 3.3, value)

	// Test with non-numeric field
	hash.HSet("text", "not-a-number")
	_, err = hash.HIncrByFloat("text", 1.0)
	assert.Error(t, err)
}

// Integration Tests

func TestDataStructuresIntegration(t *testing.T) {
	// Test that different data structures can coexist
	db := NewUltraOptimizedDB()

	// Create different data structures
	list := db.GetOrCreateList("mylist")
	set := db.GetOrCreateSet("myset")
	hash := db.GetOrCreateHash("myhash")

	// Verify they are different types
	assert.Equal(t, TypeList, db.GetDataType("mylist"))
	assert.Equal(t, TypeSet, db.GetDataType("myset"))
	assert.Equal(t, TypeHash, db.GetDataType("myhash"))

	// Test operations
	list.LPush("a", "b", "c")
	set.SAdd("x", "y", "z")
	hash.HSet("field1", "value1")

	assert.Equal(t, 3, list.LLen())
	assert.Equal(t, 3, set.SCard())
	assert.Equal(t, 1, hash.HLen())
}

func TestDataStructuresConcurrency(t *testing.T) {
	// Test concurrent access to data structures
	list := NewList()
	set := NewSet()
	hash := NewHash()

	// Use channels to coordinate goroutines
	done := make(chan bool, 3)

	// Concurrent list operations
	go func() {
		for i := 0; i < 100; i++ {
			list.LPush("item")
			list.RPop()
		}
		done <- true
	}()

	// Concurrent set operations
	go func() {
		for i := 0; i < 100; i++ {
			set.SAdd("item")
			set.SRem("item")
		}
		done <- true
	}()

	// Concurrent hash operations
	go func() {
		for i := 0; i < 100; i++ {
			hash.HSet("field", "value")
			hash.HDel("field")
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	<-done
	<-done
	<-done

	// Verify final state
	assert.Equal(t, 0, list.LLen())
	assert.Equal(t, 0, set.SCard())
	assert.Equal(t, 0, hash.HLen())
}
