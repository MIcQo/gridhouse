package cmd

import (
	"gridhouse/internal/resp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// List Command Tests

func TestLPushHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LPushHandler(store)

	// Test valid LPUSH
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(3), result.Int)

	// Test insufficient arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestRPushHandler(t *testing.T) {
	store := newMockDataStore()
	handler := RPushHandler(store)

	// Test valid RPUSH
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(3), result.Int)

	// Test insufficient arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestLPopHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LPopHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.LPush("a", "b", "c")

	// Test valid LPOP
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Equal(t, "a", result.Str)

	// Test LPOP on empty list
	args = []resp.Value{
		{Type: resp.BulkString, Str: "emptylist"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.True(t, result.IsNull)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestRPopHandler(t *testing.T) {
	store := newMockDataStore()
	handler := RPopHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.RPush("a", "b", "c")

	// Test valid RPOP
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Equal(t, "c", result.Str)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestLLenHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LLenHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.LPush("a", "b", "c")

	// Test valid LLEN
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(3), result.Int)

	// Test LLEN on empty list
	args = []resp.Value{
		{Type: resp.BulkString, Str: "emptylist"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(0), result.Int)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestLRangeHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LRangeHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.RPush("a", "b", "c", "d", "e")

	// Test valid LRANGE
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "1"},
		{Type: resp.BulkString, Str: "3"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)
	assert.Equal(t, "b", result.Array[0].Str)
	assert.Equal(t, "c", result.Array[1].Str)
	assert.Equal(t, "d", result.Array[2].Str)

	// Test LRANGE with negative indices
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "-3"},
		{Type: resp.BulkString, Str: "-1"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "1"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")

	// Test invalid integer
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "invalid"},
		{Type: resp.BulkString, Str: "3"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not an integer")
}

func TestLIndexHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LIndexHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.RPush("a", "b", "c", "d")

	// Test valid LINDEX
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "2"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Equal(t, "c", result.Str)

	// Test LINDEX with negative index
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "-1"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Equal(t, "d", result.Str)

	// Test LINDEX out of bounds
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "10"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.True(t, result.IsNull)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestLSetHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LSetHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.RPush("a", "b", "c")

	// Test valid LSET
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "1"},
		{Type: resp.BulkString, Str: "x"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Verify the value was set
	element, exists := list.LIndex(1)
	assert.True(t, exists)
	assert.Equal(t, "x", element)

	// Test LSET out of bounds
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "10"},
		{Type: resp.BulkString, Str: "x"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index out of range")

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "1"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestLRemHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LRemHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.RPush("a", "b", "a", "c", "a", "d")

	// Test valid LREM
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "2"},
		{Type: resp.BulkString, Str: "a"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(2), result.Int)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "2"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestLTrimHandler(t *testing.T) {
	store := newMockDataStore()
	handler := LTrimHandler(store)

	// Setup: add elements to list
	list := store.GetOrCreateList("mylist")
	list.RPush("a", "b", "c", "d", "e")

	// Test valid LTRIM
	args := []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "1"},
		{Type: resp.BulkString, Str: "3"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "OK", result.Str)

	// Verify the list was trimmed
	assert.Equal(t, 3, list.LLen())
	elements := list.LRange(0, -1)
	assert.Equal(t, []string{"b", "c", "d"}, elements)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "mylist"},
		{Type: resp.BulkString, Str: "1"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}
