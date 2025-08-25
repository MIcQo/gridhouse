package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
)

// incrMockStore implements the Store interface for testing
type incrMockStore struct {
	data map[string]string
	ttl  map[string]time.Time
}

func newIncrMockStore() *incrMockStore {
	return &incrMockStore{
		data: make(map[string]string),
		ttl:  make(map[string]time.Time),
	}
}

func (m *incrMockStore) Get(key string) (string, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *incrMockStore) Set(key, value string, expiration time.Time) {
	m.data[key] = value
	m.ttl[key] = expiration
}

func (m *incrMockStore) Del(key string) bool {
	if _, exists := m.data[key]; exists {
		delete(m.data, key)
		delete(m.ttl, key)
		return true
	}
	return false
}

func (m *incrMockStore) Exists(key string) bool {
	_, exists := m.data[key]
	return exists
}

func (m *incrMockStore) TTL(key string) int64 {
	return -1 // No expiration for simplicity
}

func (m *incrMockStore) PTTL(key string) int64 {
	return -1 // No expiration for simplicity
}

func (m *incrMockStore) Expire(key string, duration time.Duration) bool {
	return true // Always succeed for simplicity
}

func (m *incrMockStore) Keys() []string {
	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		keys = append(keys, key)
	}
	return keys
}

func (m *incrMockStore) GetOrCreateList(key string) *store.List {
	return nil // Not needed for incr/decr tests
}

func (m *incrMockStore) GetOrCreateSet(key string) *store.Set {
	return nil // Not needed for incr/decr tests
}

func (m *incrMockStore) GetOrCreateHash(key string) *store.Hash {
	return nil // Not needed for incr/decr tests
}

func (m *incrMockStore) GetOrCreateStream(key string) *store.Stream {
	return nil // Not needed for incr/decr tests
}

func TestOptimizedIncrHandler(t *testing.T) {
	t.Run("INCR with no arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCR' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCR with too many arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "key2"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCR' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCR with new key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "newkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify the value was stored
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "1", value)
	})

	t.Run("INCR with existing integer key", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("existingkey", "5", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "existingkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(6), result.Int)

		// Verify the value was updated
		value, exists := store.Get("existingkey")
		assert.True(t, exists)
		assert.Equal(t, "6", value)
	})

	t.Run("INCR with zero value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("zerokey", "0", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "zerokey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify the value was updated
		value, exists := store.Get("zerokey")
		assert.True(t, exists)
		assert.Equal(t, "1", value)
	})

	t.Run("INCR with negative value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("negkey", "-5", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "negkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-4), result.Int)

		// Verify the value was updated
		value, exists := store.Get("negkey")
		assert.True(t, exists)
		assert.Equal(t, "-4", value)
	})

	t.Run("INCR with large positive value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("largekey", "9223372036854775806", time.Time{}) // Max int64 - 1
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "largekey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(9223372036854775807), result.Int) // Max int64

		// Verify the value was updated
		value, exists := store.Get("largekey")
		assert.True(t, exists)
		assert.Equal(t, "9223372036854775807", value)
	})

	t.Run("INCR with non-integer value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("stringkey", "notanumber", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "stringkey"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("stringkey")
		assert.True(t, exists)
		assert.Equal(t, "notanumber", value)
	})

	t.Run("INCR with decimal value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("decimalkey", "3.14", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "decimalkey"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("decimalkey")
		assert.True(t, exists)
		assert.Equal(t, "3.14", value)
	})

	t.Run("INCR with empty string value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("emptykey", "", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "emptykey"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("emptykey")
		assert.True(t, exists)
		assert.Equal(t, "", value)
	})

	t.Run("INCR with special characters in key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "special-key_123"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify the value was stored
		value, exists := store.Get("special-key_123")
		assert.True(t, exists)
		assert.Equal(t, "1", value)
	})

	t.Run("INCR multiple times on same key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "multikey"},
		}

		// First increment
		result1, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result1.Int)

		// Second increment
		result2, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), result2.Int)

		// Third increment
		result3, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), result3.Int)

		// Verify final value
		value, exists := store.Get("multikey")
		assert.True(t, exists)
		assert.Equal(t, "3", value)
	})
}

func TestOptimizedDecrHandler(t *testing.T) {
	t.Run("DECR with no arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'DECR' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("DECR with too many arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "key2"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'DECR' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("DECR with new key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "newkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-1), result.Int)

		// Verify the value was stored
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "-1", value)
	})

	t.Run("DECR with existing integer key", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("existingkey", "5", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "existingkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(4), result.Int)

		// Verify the value was updated
		value, exists := store.Get("existingkey")
		assert.True(t, exists)
		assert.Equal(t, "4", value)
	})

	t.Run("DECR with zero value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("zerokey", "0", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "zerokey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-1), result.Int)

		// Verify the value was updated
		value, exists := store.Get("zerokey")
		assert.True(t, exists)
		assert.Equal(t, "-1", value)
	})

	t.Run("DECR with negative value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("negkey", "-5", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "negkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-6), result.Int)

		// Verify the value was updated
		value, exists := store.Get("negkey")
		assert.True(t, exists)
		assert.Equal(t, "-6", value)
	})

	t.Run("DECR with large negative value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("largekey", "-9223372036854775806", time.Time{}) // Min int64 + 1
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "largekey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-9223372036854775807), result.Int) // Min int64

		// Verify the value was updated
		value, exists := store.Get("largekey")
		assert.True(t, exists)
		assert.Equal(t, "-9223372036854775807", value)
	})

	t.Run("DECR with non-integer value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("stringkey", "notanumber", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "stringkey"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("stringkey")
		assert.True(t, exists)
		assert.Equal(t, "notanumber", value)
	})

	t.Run("DECR with decimal value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("decimalkey", "3.14", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "decimalkey"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("decimalkey")
		assert.True(t, exists)
		assert.Equal(t, "3.14", value)
	})

	t.Run("DECR with empty string value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("emptykey", "", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "emptykey"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("emptykey")
		assert.True(t, exists)
		assert.Equal(t, "", value)
	})

	t.Run("DECR with special characters in key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "special-key_123"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-1), result.Int)

		// Verify the value was stored
		value, exists := store.Get("special-key_123")
		assert.True(t, exists)
		assert.Equal(t, "-1", value)
	})

	t.Run("DECR multiple times on same key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "multikey"},
		}

		// First decrement
		result1, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(-1), result1.Int)

		// Second decrement
		result2, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(-2), result2.Int)

		// Third decrement
		result3, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(-3), result3.Int)

		// Verify final value
		value, exists := store.Get("multikey")
		assert.True(t, exists)
		assert.Equal(t, "-3", value)
	})

	t.Run("DECR with positive value to zero", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("poskey", "1", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "poskey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int)

		// Verify the value was updated
		value, exists := store.Get("poskey")
		assert.True(t, exists)
		assert.Equal(t, "0", value)
	})
}
