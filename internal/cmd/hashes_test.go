package cmd

import (
	"gridhouse/internal/resp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHSetHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HSetHandler(store)

	// Test valid HSET (single field)
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "value1"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(1), result.Int) // new field

	// Test HSET with multiple fields
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field2"},
		{Type: resp.BulkString, Str: "value2"},
		{Type: resp.BulkString, Str: "field3"},
		{Type: resp.BulkString, Str: "value3"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(2), result.Int) // 2 new fields

	// Test updating existing field
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "newvalue"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(0), result.Int) // field already existed

	// Test insufficient arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field1"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHGetHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HGetHandler(store)

	// Setup: add fields to hash
	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")

	// Test valid HGET
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field1"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Equal(t, "value1", result.Str)

	// Test HGET non-existing field
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field3"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.True(t, result.IsNull)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHDelHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HDelHandler(store)

	// Setup: add fields to hash
	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")
	hash.HSet("field3", "value3")

	// Test valid HDEL
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "field3"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(2), result.Int)

	// Test insufficient arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHExistsHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HExistsHandler(store)

	// Setup: add fields to hash
	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")

	// Test existing field
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field1"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(1), result.Int)

	// Test non-existing field
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "field2"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(0), result.Int)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHGetAllHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HGetAllHandler(store)

	// Setup: add fields to hash
	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")

	// Test valid HGETALL
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 4) // field1, value1, field2, value2

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHKeysHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HKeysHandler(store)

	// Setup: add fields to hash
	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")

	// Test valid HKEYS
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	// Verify keys are present
	keys := make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "field1")
	assert.Contains(t, keys, "field2")

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHValsHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HValsHandler(store)

	// Setup: add fields to hash
	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")

	// Test valid HVALS
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	// Verify values are present
	values := make([]string, len(result.Array))
	for i, value := range result.Array {
		values[i] = value.Str
	}
	assert.Contains(t, values, "value1")
	assert.Contains(t, values, "value2")

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHLenHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HLenHandler(store)

	// Setup: add fields to hash
	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")
	hash.HSet("field2", "value2")

	// Test valid HLEN
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(2), result.Int)

	// Test HLEN on empty hash
	args = []resp.Value{
		{Type: resp.BulkString, Str: "emptyhash"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(0), result.Int)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "extra"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")
}

func TestHIncrByHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HIncrByHandler(store)

	// Test valid HINCRBY (new field)
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "counter"},
		{Type: resp.BulkString, Str: "5"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(5), result.Int)

	// Test HINCRBY existing field
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "counter"},
		{Type: resp.BulkString, Str: "3"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, result.Type)
	assert.Equal(t, int64(8), result.Int)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "counter"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")

	// Test invalid integer
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "counter"},
		{Type: resp.BulkString, Str: "invalid"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not an integer")
}

func TestHIncrByFloatHandler(t *testing.T) {
	store := newMockDataStore()
	handler := HIncrByFloatHandler(store)

	// Test valid HINCRBYFLOAT (new field)
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "float"},
		{Type: resp.BulkString, Str: "1.5"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Equal(t, "1.500000", result.Str)

	// Test HINCRBYFLOAT existing field
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "float"},
		{Type: resp.BulkString, Str: "2.3"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Equal(t, "3.800000", result.Str)

	// Test wrong number of arguments
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "float"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")

	// Test invalid float
	args = []resp.Value{
		{Type: resp.BulkString, Str: "myhash"},
		{Type: resp.BulkString, Str: "float"},
		{Type: resp.BulkString, Str: "invalid"},
	}

	_, err = handler(args)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid float")
}
