package cmd

import (
	"gridhouse/internal/resp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Set Command Tests

func TestSAddHandler(t *testing.T) {
	store := newMockDataStore()
	handler := SAddHandler(store)

	// Test valid SADD
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(3), result.Int)

	// Test SADD with duplicates
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "d"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(1), result.Int) // only 'd' was new

	// Test insufficient arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestSRemHandler(t *testing.T) {
	store := newMockDataStore()
	handler := SRemHandler(store)

	// Setup: add elements to set
	set := store.GetOrCreateSet("myset")
	set.SAdd("a", "b", "c", "d")

	// Test valid SREM
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "c"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(2), result.Int)

	// Test insufficient arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestSIsMemberHandler(t *testing.T) {
	store := newMockDataStore()
	handler := SIsMemberHandler(store)

	// Setup: add elements to set
	set := store.GetOrCreateSet("myset")
	set.SAdd("a", "b", "c")

	// Test existing member
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "a"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(1), result.Int)

	// Test non-existing member
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "x"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(0), result.Int)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestSMembersHandler(t *testing.T) {
	store := newMockDataStore()
	handler := SMembersHandler(store)

	// Setup: add elements to set
	set := store.GetOrCreateSet("myset")
	set.SAdd("a", "b", "c")

	// Test valid SMEMBERS
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)

	// Verify all members are present
	members := make([]string, len(result.Array))
	for i, member := range result.Array {
		members[i] = member.Str
	}
	assert.Contains(t, members, "a")
	assert.Contains(t, members, "b")
	assert.Contains(t, members, "c")

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestSCardHandler(t *testing.T) {
	store := newMockDataStore()
	handler := SCardHandler(store)

	// Setup: add elements to set
	set := store.GetOrCreateSet("myset")
	set.SAdd("a", "b", "c")

	// Test valid SCARD
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(3), result.Int)

	// Test SCARD on empty set
	args = []resp.Value{
		{Type: resp.BulkString, Str: "emptyset"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(0), result.Int)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestSPopHandler(t *testing.T) {
	store := newMockDataStore()
	handler := SPopHandler(store)

	// Setup: add elements to set
	set := store.GetOrCreateSet("myset")
	set.SAdd("a", "b", "c")

	// Test valid SPOP
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Contains(t, []string{"a", "b", "c"}, result.Str)

	// Test SPOP on empty set
	args = []resp.Value{
		{Type: resp.BulkString, Str: "emptyset"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.True(t, result.IsNull)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}
