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
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "4.14", result.Str)

		// Verify the value was updated
		value, exists := store.Get("decimalkey")
		assert.True(t, exists)
		assert.Equal(t, "4.14", value)
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
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "2.14", result.Str)

		// Verify the value was updated
		value, exists := store.Get("decimalkey")
		assert.True(t, exists)
		assert.Equal(t, "2.14", value)
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

func TestIncrByHandler(t *testing.T) {
	t.Run("INCRBY with no arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByHandler(store)
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCRBY' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCRBY with one argument", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCRBY' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCRBY with too many arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "5"},
			{Type: resp.BulkString, Str: "extra"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCRBY' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCRBY with new key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "newkey"},
			{Type: resp.BulkString, Str: "5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)

		// Verify the value was stored
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "5", value)
	})

	t.Run("INCRBY with existing integer key", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("existingkey", "10", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "existingkey"},
			{Type: resp.BulkString, Str: "5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(15), result.Int)

		// Verify the value was updated
		value, exists := store.Get("existingkey")
		assert.True(t, exists)
		assert.Equal(t, "15", value)
	})

	t.Run("INCRBY with zero increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("zerokey", "10", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "zerokey"},
			{Type: resp.BulkString, Str: "0"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(10), result.Int)

		// Verify the value was unchanged
		value, exists := store.Get("zerokey")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("INCRBY with negative increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("negkey", "10", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "negkey"},
			{Type: resp.BulkString, Str: "-3"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(7), result.Int)

		// Verify the value was updated
		value, exists := store.Get("negkey")
		assert.True(t, exists)
		assert.Equal(t, "7", value)
	})

	t.Run("INCRBY with large increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("largekey", "1000000", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "largekey"},
			{Type: resp.BulkString, Str: "500000"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1500000), result.Int)

		// Verify the value was updated
		value, exists := store.Get("largekey")
		assert.True(t, exists)
		assert.Equal(t, "1500000", value)
	})

	t.Run("INCRBY with non-integer increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "notanumber"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("INCRBY with non-integer existing value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("stringkey", "notanumber", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "stringkey"},
			{Type: resp.BulkString, Str: "5"},
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

	t.Run("INCRBY with decimal increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "3.14"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("INCRBY with empty string increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: ""},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("INCRBY with special characters in key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "special-key_123"},
			{Type: resp.BulkString, Str: "5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)

		// Verify the value was stored
		value, exists := store.Get("special-key_123")
		assert.True(t, exists)
		assert.Equal(t, "5", value)
	})

	t.Run("INCRBY multiple times on same key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "multikey"},
			{Type: resp.BulkString, Str: "5"},
		}

		// First increment
		result1, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), result1.Int)

		// Second increment
		result2, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), result2.Int)

		// Third increment
		result3, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(15), result3.Int)

		// Verify final value
		value, exists := store.Get("multikey")
		assert.True(t, exists)
		assert.Equal(t, "15", value)
	})

	t.Run("INCRBY with overflow", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("overflowkey", "9223372036854775800", time.Time{}) // Close to max int64
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "overflowkey"},
			{Type: resp.BulkString, Str: "10"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "increment or decrement would overflow")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("overflowkey")
		assert.True(t, exists)
		assert.Equal(t, "9223372036854775800", value)
	})
}

func TestDecrByHandler(t *testing.T) {
	t.Run("DECRBY with no arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := DecrByHandler(store)
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'DECRBY' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("DECRBY with one argument", func(t *testing.T) {
		store := newIncrMockStore()
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'DECRBY' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("DECRBY with too many arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "5"},
			{Type: resp.BulkString, Str: "extra"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'DECRBY' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("DECRBY with new key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "newkey"},
			{Type: resp.BulkString, Str: "5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-5), result.Int)

		// Verify the value was stored
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "-5", value)
	})

	t.Run("DECRBY with existing integer key", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("existingkey", "10", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "existingkey"},
			{Type: resp.BulkString, Str: "5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)

		// Verify the value was updated
		value, exists := store.Get("existingkey")
		assert.True(t, exists)
		assert.Equal(t, "5", value)
	})

	t.Run("DECRBY with zero decrement", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("zerokey", "10", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "zerokey"},
			{Type: resp.BulkString, Str: "0"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(10), result.Int)

		// Verify the value was unchanged
		value, exists := store.Get("zerokey")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("DECRBY with negative decrement (increment)", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("negkey", "10", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "negkey"},
			{Type: resp.BulkString, Str: "-3"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(13), result.Int)

		// Verify the value was updated
		value, exists := store.Get("negkey")
		assert.True(t, exists)
		assert.Equal(t, "13", value)
	})

	t.Run("DECRBY with large decrement", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("largekey", "1000000", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "largekey"},
			{Type: resp.BulkString, Str: "500000"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(500000), result.Int)

		// Verify the value was updated
		value, exists := store.Get("largekey")
		assert.True(t, exists)
		assert.Equal(t, "500000", value)
	})

	t.Run("DECRBY with non-integer decrement", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "notanumber"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("DECRBY with non-integer existing value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("stringkey", "notanumber", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "stringkey"},
			{Type: resp.BulkString, Str: "5"},
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

	t.Run("DECRBY with decimal decrement", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "3.14"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("DECRBY with empty string decrement", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: ""},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("DECRBY with special characters in key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "special-key_123"},
			{Type: resp.BulkString, Str: "5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-5), result.Int)

		// Verify the value was stored
		value, exists := store.Get("special-key_123")
		assert.True(t, exists)
		assert.Equal(t, "-5", value)
	})

	t.Run("DECRBY multiple times on same key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "multikey"},
			{Type: resp.BulkString, Str: "5"},
		}

		// First decrement
		result1, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(-5), result1.Int)

		// Second decrement
		result2, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(-10), result2.Int)

		// Third decrement
		result3, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(-15), result3.Int)

		// Verify final value
		value, exists := store.Get("multikey")
		assert.True(t, exists)
		assert.Equal(t, "-15", value)
	})

	t.Run("DECRBY with underflow", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("underflowkey", "-9223372036854775800", time.Time{}) // Close to min int64
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "underflowkey"},
			{Type: resp.BulkString, Str: "10"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ERR increment or decrement would overflow")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("underflowkey")
		assert.True(t, exists)
		assert.Equal(t, "-9223372036854775800", value)
	})

	t.Run("DECRBY with positive value to zero", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("poskey", "5", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "poskey"},
			{Type: resp.BulkString, Str: "5"},
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

	t.Run("DECRBY with positive value to negative", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("poskey", "5", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "poskey"},
			{Type: resp.BulkString, Str: "10"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(-5), result.Int)

		// Verify the value was updated
		value, exists := store.Get("poskey")
		assert.True(t, exists)
		assert.Equal(t, "-5", value)
	})
}

func TestIncrWithFloatInput(t *testing.T) {
	t.Run("INCR with float existing value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "10.5", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "11.5", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "11.5", value)
	})

	t.Run("INCR with float existing value resulting in integer", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "9.0", time.Time{})
		handler := OptimizedIncrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "10", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})
}

func TestDecrWithFloatInput(t *testing.T) {
	t.Run("DECR with float existing value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "10.5", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "9.5", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "9.5", value)
	})

	t.Run("DECR with float existing value resulting in integer", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "10.0", time.Time{})
		handler := OptimizedDecrHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "9", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "9", value)
	})
}

func TestIncrByWithFloatInput(t *testing.T) {
	t.Run("INCRBY with integer increment and float existing value (10.5 + 3 = 13.5)", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "10.5", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
			{Type: resp.BulkString, Str: "3"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "13.5", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "13.5", value)
	})

	t.Run("INCRBY with integer increment and float existing value (2.2 + 20 = 22.2)", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "2.2", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
			{Type: resp.BulkString, Str: "20"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "22.2", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "22.2", value)
	})

	t.Run("INCRBY with integer increment on float existing value resulting in integer", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10.5", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "-0"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "10.5", result.Str)

		// Verify the value was updated
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10.5", value)
	})

	t.Run("INCRBY with float increment should fail", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := IncrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "3.5"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})
}

func TestDecrByWithFloatInput(t *testing.T) {
	t.Run("DECRBY with integer decrement and float existing value (10.5 - 3 = 7.5)", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "10.5", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
			{Type: resp.BulkString, Str: "3"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "7.5", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "7.5", value)
	})

	t.Run("DECRBY with integer decrement and float existing value (22.2 - 20 = 2.2)", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "22.2", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
			{Type: resp.BulkString, Str: "20"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "2.2", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "2.2", value)
	})

	t.Run("DECRBY with integer decrement on float existing value resulting in integer", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10.5", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "0"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "10.5", result.Str)

		// Verify the value was updated
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10.5", value)
	})

	t.Run("DECRBY with float decrement should fail", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10", time.Time{})
		handler := DecrByHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "3.5"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not an integer or out of range")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})
}

func TestIncrByFloatHandler(t *testing.T) {
	t.Run("INCRBYFLOAT with no arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByFloatHandler(store)
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCRBYFLOAT' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCRBYFLOAT with one argument", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCRBYFLOAT' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCRBYFLOAT with too many arguments", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "5.5"},
			{Type: resp.BulkString, Str: "extra"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'INCRBYFLOAT' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("INCRBYFLOAT with new key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "newkey"},
			{Type: resp.BulkString, Str: "5.5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "5.5", result.Str)

		// Verify the value was stored
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "5.5", value)
	})

	t.Run("INCRBYFLOAT with existing integer key", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("existingkey", "10", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "existingkey"},
			{Type: resp.BulkString, Str: "5.5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "15.5", result.Str)

		// Verify the value was updated
		value, exists := store.Get("existingkey")
		assert.True(t, exists)
		assert.Equal(t, "15.5", value)
	})

	t.Run("INCRBYFLOAT with existing float key", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("floatkey", "10.5", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "floatkey"},
			{Type: resp.BulkString, Str: "3.25"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "13.75", result.Str)

		// Verify the value was updated
		value, exists := store.Get("floatkey")
		assert.True(t, exists)
		assert.Equal(t, "13.75", value)
	})

	t.Run("INCRBYFLOAT with zero increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("zerokey", "10.5", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "zerokey"},
			{Type: resp.BulkString, Str: "0.0"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "10.5", result.Str)

		// Verify the value was unchanged
		value, exists := store.Get("zerokey")
		assert.True(t, exists)
		assert.Equal(t, "10.5", value)
	})

	t.Run("INCRBYFLOAT with negative increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("negkey", "10.5", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "negkey"},
			{Type: resp.BulkString, Str: "-3.25"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "7.25", result.Str)

		// Verify the value was updated
		value, exists := store.Get("negkey")
		assert.True(t, exists)
		assert.Equal(t, "7.25", value)
	})

	t.Run("INCRBYFLOAT with large increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("largekey", "1000000.5", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "largekey"},
			{Type: resp.BulkString, Str: "500000.25"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "1500000.75", result.Str)

		// Verify the value was updated
		value, exists := store.Get("largekey")
		assert.True(t, exists)
		assert.Equal(t, "1500000.75", value)
	})

	t.Run("INCRBYFLOAT with non-numeric increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10.5", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "notanumber"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not a valid float")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10.5", value)
	})

	t.Run("INCRBYFLOAT with non-numeric existing value", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("stringkey", "notanumber", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "stringkey"},
			{Type: resp.BulkString, Str: "5.5"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not a valid float")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("stringkey")
		assert.True(t, exists)
		assert.Equal(t, "notanumber", value)
	})

	t.Run("INCRBYFLOAT with empty string increment", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("key", "10.5", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: ""},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not a valid float")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "10.5", value)
	})

	t.Run("INCRBYFLOAT with special characters in key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "special-key_123"},
			{Type: resp.BulkString, Str: "5.5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "5.5", result.Str)

		// Verify the value was stored
		value, exists := store.Get("special-key_123")
		assert.True(t, exists)
		assert.Equal(t, "5.5", value)
	})

	t.Run("INCRBYFLOAT multiple times on same key", func(t *testing.T) {
		store := newIncrMockStore()
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "multikey"},
			{Type: resp.BulkString, Str: "5.5"},
		}

		// First increment
		result1, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, "5.5", result1.Str)

		// Second increment
		result2, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, "11", result2.Str)

		// Third increment
		result3, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, "16.5", result3.Str)

		// Verify final value
		value, exists := store.Get("multikey")
		assert.True(t, exists)
		assert.Equal(t, "16.5", value)
	})

	t.Run("INCRBYFLOAT with scientific notation", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("scikey", "1.0", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "scikey"},
			{Type: resp.BulkString, Str: "1e-3"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "1.001", result.Str)

		// Verify the value was updated
		value, exists := store.Get("scikey")
		assert.True(t, exists)
		assert.Equal(t, "1.001", value)
	})

	t.Run("INCRBYFLOAT with very small decimal", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("smallkey", "0.1", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "smallkey"},
			{Type: resp.BulkString, Str: "0.0001"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "0.1001", result.Str)

		// Verify the value was updated
		value, exists := store.Get("smallkey")
		assert.True(t, exists)
		assert.Equal(t, "0.1001", value)
	})

	t.Run("INCRBYFLOAT with integer result", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("intkey", "10.5", time.Time{})
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "intkey"},
			{Type: resp.BulkString, Str: "-0.5"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.BulkString, result.Type)
		assert.Equal(t, "10", result.Str)

		// Verify the value was updated
		value, exists := store.Get("intkey")
		assert.True(t, exists)
		assert.Equal(t, "10", value)
	})

	t.Run("INCRBYFLOAT with overflow", func(t *testing.T) {
		store := newIncrMockStore()
		store.Set("overflowkey", "1.7976931348623157e+308", time.Time{}) // Max float64
		handler := IncrByFloatHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "overflowkey"},
			{Type: resp.BulkString, Str: "1e308"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "value is not a valid float")
		assert.Equal(t, resp.Value{}, result)

		// Verify the original value was not changed
		value, exists := store.Get("overflowkey")
		assert.True(t, exists)
		assert.Equal(t, "1.7976931348623157e+308", value)
	})
}
