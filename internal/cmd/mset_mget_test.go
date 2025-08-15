package cmd

import (
	"fmt"
	"testing"
	"time"

	"gridhouse/internal/resp"
	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock Store for testing MSET/MGET commands
type mockMSetMGetStore struct {
	*store.UltraOptimizedDB
}

func newMockMSetMGetStore() *mockMSetMGetStore {
	return &mockMSetMGetStore{
		UltraOptimizedDB: store.NewUltraOptimizedDB(),
	}
}

func TestMSetHandler(t *testing.T) {
	store := newMockMSetMGetStore()
	handler := MSetHandler(store)

	// Test MSET with single key-value pair
	args := []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "value1"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Verify the key was set
	value, exists := store.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, "value1", value)

	// Test MSET with multiple key-value pairs
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key2"},
		{Type: resp.BulkString, Str: "value2"},
		{Type: resp.BulkString, Str: "key3"},
		{Type: resp.BulkString, Str: "value3"},
		{Type: resp.BulkString, Str: "key4"},
		{Type: resp.BulkString, Str: "value4"},
	}

	_, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Verify all keys were set
	value, exists = store.Get("key2")
	assert.True(t, exists)
	assert.Equal(t, "value2", value)

	value, exists = store.Get("key3")
	assert.True(t, exists)
	assert.Equal(t, "value3", value)

	value, exists = store.Get("key4")
	assert.True(t, exists)
	assert.Equal(t, "value4", value)

	// Test MSET with empty values
	args = []resp.Value{
		{Type: resp.BulkString, Str: "empty_key"},
		{Type: resp.BulkString, Str: ""},
	}

	_, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	value, exists = store.Get("empty_key")
	assert.True(t, exists)
	assert.Equal(t, "", value)
}

func TestMSetHandlerErrors(t *testing.T) {
	store := newMockMSetMGetStore()
	handler := MSetHandler(store)

	// Test MSET with no arguments
	args := []resp.Value{}

	_, err := handler(args)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")

	// Test MSET with odd number of arguments (missing value)
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "value1"},
		{Type: resp.BulkString, Str: "key2"},
	}

	_, err = handler(args)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")

	// Test MSET with single argument (missing value)
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
	}

	_, err = handler(args)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestMGetHandler(t *testing.T) {
	store := newMockMSetMGetStore()
	handler := MGetHandler(store)

	// Set up some test data
	store.Set("key1", "value1", time.Time{})
	store.Set("key2", "value2", time.Time{})
	store.Set("key3", "value3", time.Time{})

	// Test MGET with single key
	args := []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, resp.BulkString, result.Array[0].Type)
	assert.Equal(t, "value1", result.Array[0].Str)

	// Test MGET with multiple keys
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "key2"},
		{Type: resp.BulkString, Str: "key3"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)
	assert.Equal(t, "value1", result.Array[0].Str)
	assert.Equal(t, "value2", result.Array[1].Str)
	assert.Equal(t, "value3", result.Array[2].Str)

	// Test MGET with mixed existing and non-existing keys
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "nonexistent"},
		{Type: resp.BulkString, Str: "key2"},
		{Type: resp.BulkString, Str: "another_nonexistent"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 4)
	assert.Equal(t, "value1", result.Array[0].Str)
	assert.True(t, result.Array[1].IsNull) // Non-existent key
	assert.Equal(t, "value2", result.Array[2].Str)
	assert.True(t, result.Array[3].IsNull) // Non-existent key

	// Test MGET with all non-existing keys
	args = []resp.Value{
		{Type: resp.BulkString, Str: "nonexistent1"},
		{Type: resp.BulkString, Str: "nonexistent2"},
		{Type: resp.BulkString, Str: "nonexistent3"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)
	assert.True(t, result.Array[0].IsNull)
	assert.True(t, result.Array[1].IsNull)
	assert.True(t, result.Array[2].IsNull)
}

func TestMGetHandlerErrors(t *testing.T) {
	store := newMockMSetMGetStore()
	handler := MGetHandler(store)

	// Test MGET with no arguments
	args := []resp.Value{}

	_, err := handler(args)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestMSetMGetIntegration(t *testing.T) {
	store := newMockMSetMGetStore()
	msetHandler := MSetHandler(store)
	mgetHandler := MGetHandler(store)

	// Test MSET followed by MGET
	msetArgs := []resp.Value{
		{Type: resp.BulkString, Str: "user:1"},
		{Type: resp.BulkString, Str: "alice"},
		{Type: resp.BulkString, Str: "user:2"},
		{Type: resp.BulkString, Str: "bob"},
		{Type: resp.BulkString, Str: "user:3"},
		{Type: resp.BulkString, Str: "charlie"},
	}

	result, err := msetHandler(msetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Now MGET the same keys
	mgetArgs := []resp.Value{
		{Type: resp.BulkString, Str: "user:1"},
		{Type: resp.BulkString, Str: "user:2"},
		{Type: resp.BulkString, Str: "user:3"},
	}

	result, err = mgetHandler(mgetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)
	assert.Equal(t, "alice", result.Array[0].Str)
	assert.Equal(t, "bob", result.Array[1].Str)
	assert.Equal(t, "charlie", result.Array[2].Str)
}

func TestMSetMGetWithDataStructures(t *testing.T) {
	store := newMockMSetMGetStore()
	msetHandler := MSetHandler(store)
	mgetHandler := MGetHandler(store)

	// Set up data structures
	list := store.GetOrCreateList("mylist")
	list.LPush("a", "b", "c")

	set := store.GetOrCreateSet("myset")
	set.SAdd("x", "y", "z")

	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")

	// Test MSET with string keys
	msetArgs := []resp.Value{
		{Type: resp.BulkString, Str: "string1"},
		{Type: resp.BulkString, Str: "value1"},
		{Type: resp.BulkString, Str: "string2"},
		{Type: resp.BulkString, Str: "value2"},
	}

	result, err := msetHandler(msetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Test MGET with mixed data types
	mgetArgs := []resp.Value{
		{Type: resp.BulkString, Str: "string1"},
		{Type: resp.BulkString, Str: "mylist"}, // List
		{Type: resp.BulkString, Str: "string2"},
		{Type: resp.BulkString, Str: "myset"},  // Set
		{Type: resp.BulkString, Str: "myhash"}, // Hash
		{Type: resp.BulkString, Str: "nonexistent"},
	}

	result, err = mgetHandler(mgetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 6)
	assert.Equal(t, "value1", result.Array[0].Str)
	assert.True(t, result.Array[1].IsNull) // List type returns null for MGET
	assert.Equal(t, "value2", result.Array[2].Str)
	assert.True(t, result.Array[3].IsNull) // Set type returns null for MGET
	assert.True(t, result.Array[4].IsNull) // Hash type returns null for MGET
	assert.True(t, result.Array[5].IsNull) // Non-existent key
}

func TestMSetMGetPerformance(t *testing.T) {
	store := newMockMSetMGetStore()
	msetHandler := MSetHandler(store)
	mgetHandler := MGetHandler(store)

	// Test with many keys
	var msetArgs []resp.Value
	for i := 0; i < 100; i++ {
		msetArgs = append(msetArgs, resp.Value{Type: resp.BulkString, Str: fmt.Sprintf("key%d", i)})
		msetArgs = append(msetArgs, resp.Value{Type: resp.BulkString, Str: fmt.Sprintf("value%d", i)})
	}

	result, err := msetHandler(msetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Test MGET with many keys
	var mgetArgs []resp.Value
	for i := 0; i < 100; i++ {
		mgetArgs = append(mgetArgs, resp.Value{Type: resp.BulkString, Str: fmt.Sprintf("key%d", i)})
	}

	result, err = mgetHandler(mgetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 100)

	// Verify all values
	for i := 0; i < 100; i++ {
		assert.Equal(t, fmt.Sprintf("value%d", i), result.Array[i].Str)
	}
}

func TestMSetMGetWithSpecialCharacters(t *testing.T) {
	store := newMockMSetMGetStore()
	msetHandler := MSetHandler(store)
	mgetHandler := MGetHandler(store)

	// Test with special characters in keys and values
	msetArgs := []resp.Value{
		{Type: resp.BulkString, Str: "key:with:colons"},
		{Type: resp.BulkString, Str: "value:with:colons"},
		{Type: resp.BulkString, Str: "key-with-dashes"},
		{Type: resp.BulkString, Str: "value-with-dashes"},
		{Type: resp.BulkString, Str: "key_with_underscores"},
		{Type: resp.BulkString, Str: "value_with_underscores"},
		{Type: resp.BulkString, Str: "key.with.dots"},
		{Type: resp.BulkString, Str: "value.with.dots"},
		{Type: resp.BulkString, Str: "key[with]brackets"},
		{Type: resp.BulkString, Str: "value[with]brackets"},
		{Type: resp.BulkString, Str: "key{with}braces"},
		{Type: resp.BulkString, Str: "value{with}braces"},
		{Type: resp.BulkString, Str: "key(with)parens"},
		{Type: resp.BulkString, Str: "value(with)parens"},
		{Type: resp.BulkString, Str: "key*with*stars"},
		{Type: resp.BulkString, Str: "value*with*stars"},
		{Type: resp.BulkString, Str: "key?with?question"},
		{Type: resp.BulkString, Str: "value?with?question"},
	}

	result, err := msetHandler(msetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Test MGET with the same keys
	var mgetArgs []resp.Value
	for i := 0; i < len(msetArgs); i += 2 {
		mgetArgs = append(mgetArgs, msetArgs[i])
	}

	result, err = mgetHandler(mgetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 9)

	// Verify all values
	assert.Equal(t, "value:with:colons", result.Array[0].Str)
	assert.Equal(t, "value-with-dashes", result.Array[1].Str)
	assert.Equal(t, "value_with_underscores", result.Array[2].Str)
	assert.Equal(t, "value.with.dots", result.Array[3].Str)
	assert.Equal(t, "value[with]brackets", result.Array[4].Str)
	assert.Equal(t, "value{with}braces", result.Array[5].Str)
	assert.Equal(t, "value(with)parens", result.Array[6].Str)
	assert.Equal(t, "value*with*stars", result.Array[7].Str)
	assert.Equal(t, "value?with?question", result.Array[8].Str)
}

func TestMSetMGetWithEmptyValues(t *testing.T) {
	store := newMockMSetMGetStore()
	msetHandler := MSetHandler(store)
	mgetHandler := MGetHandler(store)

	// Test MSET with empty values
	msetArgs := []resp.Value{
		{Type: resp.BulkString, Str: "empty1"},
		{Type: resp.BulkString, Str: ""},
		{Type: resp.BulkString, Str: "empty2"},
		{Type: resp.BulkString, Str: ""},
		{Type: resp.BulkString, Str: "non_empty"},
		{Type: resp.BulkString, Str: "value"},
	}

	result, err := msetHandler(msetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Test MGET with the same keys
	mgetArgs := []resp.Value{
		{Type: resp.BulkString, Str: "empty1"},
		{Type: resp.BulkString, Str: "empty2"},
		{Type: resp.BulkString, Str: "non_empty"},
	}

	result, err = mgetHandler(mgetArgs)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)
	assert.Equal(t, "", result.Array[0].Str)      // Empty value
	assert.Equal(t, "", result.Array[1].Str)      // Empty value
	assert.Equal(t, "value", result.Array[2].Str) // Non-empty value
}
