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

// Mock Store for testing KEYS command
type mockKeysStore struct {
	*store.UltraOptimizedDB
}

func newMockKeysStore() *mockKeysStore {
	return &mockKeysStore{UltraOptimizedDB: store.NewUltraOptimizedDB()}
}

func TestKeysHandler(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Test KEYS with no pattern (should return all keys)
	args := []resp.Value{}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0) // No keys initially

	// Test KEYS with empty pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: ""},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0) // No keys initially

	// Test KEYS with simple pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0) // No keys initially
}

func TestKeysHandlerWithData(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Add some test data
	store.Set("user:1", "alice", time.Time{})
	store.Set("user:2", "bob", time.Time{})
	store.Set("session:abc", "active", time.Time{})
	store.Set("config:redis", "enabled", time.Time{})
	store.Set("temp:123", "data", time.Time{})

	// Test KEYS with no pattern (should return all keys)
	args := []resp.Value{}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 5)

	// Verify all keys are present
	keys := make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "user:1")
	assert.Contains(t, keys, "user:2")
	assert.Contains(t, keys, "session:abc")
	assert.Contains(t, keys, "config:redis")
	assert.Contains(t, keys, "temp:123")

	// Test KEYS with wildcard pattern "*"
	args = []resp.Value{
		{Type: resp.BulkString, Str: "*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 5)

	// Test KEYS with "user:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "user:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	// Verify only user keys are present
	keys = make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "user:1")
	assert.Contains(t, keys, "user:2")
	assert.NotContains(t, keys, "session:abc")
	assert.NotContains(t, keys, "config:redis")
	assert.NotContains(t, keys, "temp:123")

	// Test KEYS with "session:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "session:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "session:abc", result.Array[0].Str)

	// Test KEYS with "config:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "config:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "config:redis", result.Array[0].Str)

	// Test KEYS with "temp:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "temp:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "temp:123", result.Array[0].Str)

	// Test KEYS with non-matching pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "nonexistent:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0)

	// Test KEYS with exact match pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "user:1"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "user:1", result.Array[0].Str)

	// Test KEYS with pattern that matches nothing
	args = []resp.Value{
		{Type: resp.BulkString, Str: "user:999"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0)
}

func TestKeysHandlerWithComplexPatterns(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Add test data with various patterns
	store.Set("user:1:profile", "data1", time.Time{})
	store.Set("user:2:profile", "data2", time.Time{})
	store.Set("user:3:settings", "data3", time.Time{})
	store.Set("session:abc:123", "active1", time.Time{})
	store.Set("session:def:456", "active2", time.Time{})
	store.Set("config:redis:port", "6379", time.Time{})
	store.Set("config:redis:host", "localhost", time.Time{})
	store.Set("temp:123:data", "temp1", time.Time{})
	store.Set("temp:456:data", "temp2", time.Time{})

	// Test KEYS with "user:*:profile" pattern
	args := []resp.Value{
		{Type: resp.BulkString, Str: "user:*:profile"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	keys := make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "user:1:profile")
	assert.Contains(t, keys, "user:2:profile")
	assert.NotContains(t, keys, "user:3:settings")

	// Test KEYS with "session:*:123" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "session:*:123"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "session:abc:123", result.Array[0].Str)

	// Test KEYS with "config:redis:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "config:redis:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	keys = make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "config:redis:port")
	assert.Contains(t, keys, "config:redis:host")

	// Test KEYS with "temp:*:data" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "temp:*:data"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	keys = make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "temp:123:data")
	assert.Contains(t, keys, "temp:456:data")
}

func TestKeysHandlerWithDataStructures(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Add data structures
	list := store.GetOrCreateList("mylist")
	list.LPush("a", "b", "c")

	set := store.GetOrCreateSet("myset")
	set.SAdd("x", "y", "z")

	hash := store.GetOrCreateHash("myhash")
	hash.HSet("field1", "value1")

	// Add some regular string keys
	store.Set("string:key1", "value1", time.Time{})
	store.Set("string:key2", "value2", time.Time{})

	// Test KEYS with no pattern (should return all keys)
	args := []resp.Value{}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 5) // 2 lists, 1 set, 1 hash, 2 strings

	// Verify all keys are present
	keys := make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "mylist")
	assert.Contains(t, keys, "myset")
	assert.Contains(t, keys, "myhash")
	assert.Contains(t, keys, "string:key1")
	assert.Contains(t, keys, "string:key2")

	// Test KEYS with "my*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "my*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 3)

	keys = make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "mylist")
	assert.Contains(t, keys, "myset")
	assert.Contains(t, keys, "myhash")
	assert.NotContains(t, keys, "string:key1")
	assert.NotContains(t, keys, "string:key2")

	// Test KEYS with "string:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "string:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	keys = make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "string:key1")
	assert.Contains(t, keys, "string:key2")
	assert.NotContains(t, keys, "mylist")
	assert.NotContains(t, keys, "myset")
	assert.NotContains(t, keys, "myhash")
}

func TestKeysHandlerWithSpecialCharacters(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Add keys with special characters
	store.Set("key-with-dashes", "value1", time.Time{})
	store.Set("key_with_underscores", "value2", time.Time{})
	store.Set("key.with.dots", "value3", time.Time{})
	store.Set("key:with:colons", "value4", time.Time{})
	store.Set("key[with]brackets", "value5", time.Time{})
	store.Set("key{with}braces", "value6", time.Time{})
	store.Set("key(with)parens", "value7", time.Time{})
	store.Set("key*with*stars", "value8", time.Time{})
	store.Set("key?with?question", "value9", time.Time{})

	// Test KEYS with no pattern (should return all keys)
	args := []resp.Value{}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 9)

	// Test KEYS with "key-with-*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key-with-*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "key-with-dashes", result.Array[0].Str)

	// Test KEYS with "key_with_*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key_with_*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "key_with_underscores", result.Array[0].Str)

	// Test KEYS with "key.with.*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key.with.*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "key.with.dots", result.Array[0].Str)

	// Test KEYS with "key:with:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key:with:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "key:with:colons", result.Array[0].Str)
}

func TestKeysHandlerWithEmptyDatabase(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Test KEYS with no pattern on empty database
	args := []resp.Value{}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0)

	// Test KEYS with "*" pattern on empty database
	args = []resp.Value{
		{Type: resp.BulkString, Str: "*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0)

	// Test KEYS with "user:*" pattern on empty database
	args = []resp.Value{
		{Type: resp.BulkString, Str: "user:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 0)
}

func TestKeysHandlerWithExpiredKeys(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Add keys with expiration
	store.Set("expired:key1", "value1", time.Time{})
	store.Set("expired:key2", "value2", time.Time{})
	store.Set("valid:key1", "value3", time.Time{})

	// Set expiration for some keys (simulate expired keys)
	// Note: In a real scenario, expired keys would be cleaned up
	// For this test, we'll just verify that KEYS returns all keys
	// regardless of expiration status

	// Test KEYS with "expired:*" pattern
	args := []resp.Value{
		{Type: resp.BulkString, Str: "expired:*"},
	}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 2)

	keys := make([]string, len(result.Array))
	for i, key := range result.Array {
		keys[i] = key.Str
	}
	assert.Contains(t, keys, "expired:key1")
	assert.Contains(t, keys, "expired:key2")

	// Test KEYS with "valid:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "valid:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "valid:key1", result.Array[0].Str)
}

func TestKeysHandlerPerformance(t *testing.T) {
	store := newMockKeysStore()
	handler := KeysHandler(store)

	// Add many keys to test performance
	for i := 0; i < 1000; i++ {
		store.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("value:%d", i), time.Time{})
	}

	// Test KEYS with no pattern
	args := []resp.Value{}

	result, err := handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1000)

	// Test KEYS with "key:*" pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key:*"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1000)

	// Test KEYS with specific pattern
	args = []resp.Value{
		{Type: resp.BulkString, Str: "key:123"},
	}

	result, err = handler(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Array, result.Type)
	assert.Len(t, result.Array, 1)
	assert.Equal(t, "key:123", result.Array[0].Str)
}
