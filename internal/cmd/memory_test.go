package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore implements Store interface for testing MEMORY command
type mockMemoryStore struct {
	data map[string]interface{}
}

func newMockMemoryStore() *mockMemoryStore {
	return &mockMemoryStore{
		data: make(map[string]interface{}),
	}
}

func (m *mockMemoryStore) Set(key, value string, expiration time.Time) {
	m.data[key] = value
}

func (m *mockMemoryStore) Get(key string) (string, bool) {
	val, ok := m.data[key]
	if !ok {
		return "", false
	}
	if str, ok := val.(string); ok {
		return str, true
	}
	return "", false
}

func (m *mockMemoryStore) Del(key string) bool {
	_, exists := m.data[key]
	delete(m.data, key)
	return exists
}

func (m *mockMemoryStore) Exists(key string) bool {
	_, exists := m.data[key]
	return exists
}

func (m *mockMemoryStore) TTL(key string) int64                           { return -1 }
func (m *mockMemoryStore) PTTL(key string) int64                          { return -1 }
func (m *mockMemoryStore) Expire(key string, duration time.Duration) bool { return false }
func (m *mockMemoryStore) Keys() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *mockMemoryStore) GetOrCreateList(key string) *store.List {
	if val, ok := m.data[key]; ok {
		if list, ok := val.(*store.List); ok {
			return list
		}
	}
	list := store.NewList()
	m.data[key] = list
	return list
}

func (m *mockMemoryStore) GetOrCreateSet(key string) *store.Set {
	if val, ok := m.data[key]; ok {
		if set, ok := val.(*store.Set); ok {
			return set
		}
	}
	set := store.NewSet()
	m.data[key] = set
	return set
}

func (m *mockMemoryStore) GetOrCreateHash(key string) *store.Hash {
	if val, ok := m.data[key]; ok {
		if hash, ok := val.(*store.Hash); ok {
			return hash
		}
	}
	hash := store.NewHash()
	m.data[key] = hash
	return hash
}

func TestMemoryCommandUsage(t *testing.T) {
	store := newMockMemoryStore()
	handler := MemoryHandler(store)

	t.Run("MEMORY USAGE with string key", func(t *testing.T) {
		// Set up test data
		store.Set("test_key", "hello world", time.Time{})

		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "test_key"},
		}

		result, err := handler(args)
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Greater(t, result.Int, int64(0))
	})

	t.Run("MEMORY USAGE with non-existent key", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "non_existent"},
		}

		result, err := handler(args)
		require.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.True(t, result.IsNull)
	})

	t.Run("MEMORY USAGE with list", func(t *testing.T) {
		list := store.GetOrCreateList("test_list")
		list.LPush("item1")
		list.LPush("item2")
		list.LPush("item3")

		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "test_list"},
		}

		result, err := handler(args)
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Greater(t, result.Int, int64(0))
	})

	t.Run("MEMORY USAGE with hash", func(t *testing.T) {
		hash := store.GetOrCreateHash("test_hash")
		hash.HSet("field1", "value1")
		hash.HSet("field2", "value2")

		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "test_hash"},
		}

		result, err := handler(args)
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Greater(t, result.Int, int64(0))
	})

	t.Run("MEMORY USAGE with set", func(t *testing.T) {
		set := store.GetOrCreateSet("test_set")
		set.SAdd("member1")
		set.SAdd("member2")
		set.SAdd("member3")

		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "test_set"},
		}

		result, err := handler(args)
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Greater(t, result.Int, int64(0))
	})
}

func TestMemoryCommandStats(t *testing.T) {
	store := newMockMemoryStore()
	handler := MemoryHandler(store)

	t.Run("MEMORY STATS", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "STATS"},
		}

		result, err := handler(args)
		require.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Greater(t, len(result.Array), 0)

		// Verify the result contains key-value pairs
		assert.Equal(t, 0, len(result.Array)%2) // Should be even number (key-value pairs)
	})
}

func TestMemoryCommandErrors(t *testing.T) {
	store := newMockMemoryStore()
	handler := MemoryHandler(store)

	t.Run("MEMORY with no arguments", func(t *testing.T) {
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("MEMORY with unknown subcommand", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "UNKNOWN"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown subcommand")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("MEMORY USAGE with no key", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("MEMORY USAGE with SAMPLES option", func(t *testing.T) {
		store.Set("key1", "value", time.Time{})
		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "SAMPLES"},
			{Type: resp.BulkString, Str: "5"},
		}
		result, err := handler(args)
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Greater(t, result.Int, int64(0))
	})

	t.Run("MEMORY USAGE with invalid SAMPLES syntax", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "UNKNOWN"},
			{Type: resp.BulkString, Str: "5"},
		}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "syntax error")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("MEMORY USAGE with invalid SAMPLES count", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "SAMPLES"},
			{Type: resp.BulkString, Str: "abc"},
		}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("MEMORY USAGE with wrong arity (3 args)", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "USAGE"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "extra"},
		}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "syntax error")
		assert.Equal(t, resp.Value{}, result)
	})
}

func TestMemoryCommandCaseInsensitive(t *testing.T) {
	store := newMockMemoryStore()
	handler := MemoryHandler(store)

	store.Set("test_key", "value", time.Time{})

	testCases := []struct {
		subcommand string
		key        string
	}{
		{"usage", "test_key"},
		{"USAGE", "test_key"},
		{"Usage", "test_key"},
		{"stats", ""},
		{"STATS", ""},
		{"Stats", ""},
	}

	for _, tc := range testCases {
		t.Run("case insensitive "+tc.subcommand, func(t *testing.T) {
			var args []resp.Value
			if tc.key != "" {
				args = []resp.Value{
					{Type: resp.BulkString, Str: tc.subcommand},
					{Type: resp.BulkString, Str: tc.key},
				}
			} else {
				args = []resp.Value{
					{Type: resp.BulkString, Str: tc.subcommand},
				}
			}

			result, err := handler(args)
			require.NoError(t, err)

			if tc.key != "" {
				assert.Equal(t, resp.Integer, result.Type)
			} else {
				assert.Equal(t, resp.Array, result.Type)
			}
		})
	}
}
