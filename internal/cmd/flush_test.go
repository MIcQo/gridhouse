package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
)

// SimpleMockStore implements Store interface for testing
type SimpleMockStore struct {
	data map[string]string
}

func (m *SimpleMockStore) Set(key, value string, expiration time.Time) {
	m.data[key] = value
}

func (m *SimpleMockStore) Get(key string) (string, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *SimpleMockStore) Del(key string) bool {
	if _, exists := m.data[key]; exists {
		delete(m.data, key)
		return true
	}
	return false
}

func (m *SimpleMockStore) Exists(key string) bool {
	_, exists := m.data[key]
	return exists
}

func (m *SimpleMockStore) TTL(key string) int64 {
	return -1
}

func (m *SimpleMockStore) PTTL(key string) int64 {
	return -1
}

func (m *SimpleMockStore) Expire(key string, duration time.Duration) bool {
	return true
}

func (m *SimpleMockStore) Keys() []string {
	var keys []string
	for key := range m.data {
		keys = append(keys, key)
	}
	return keys
}

func (m *SimpleMockStore) GetOrCreateList(key string) *store.List {
	return store.NewList()
}

func (m *SimpleMockStore) GetOrCreateSet(key string) *store.Set {
	return store.NewSet()
}

func (m *SimpleMockStore) GetOrCreateHash(key string) *store.Hash {
	return store.NewHash()
}

func (m *SimpleMockStore) GetDataType(key string) store.DataType {
	return store.TypeString
}

func TestFlushDBHandler(t *testing.T) {
	t.Run("flush_all_keys", func(t *testing.T) {
		// Create a mock store
		mockStore := &SimpleMockStore{
			data: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		}

		// Call FLUSHDB handler
		handler := FlushDBHandler(mockStore)
		result, err := handler([]resp.Value{})

		// Verify no error
		assert.NoError(t, err)
		assert.Equal(t, "OK", result.Str)

		// Verify all keys were removed
		assert.Empty(t, mockStore.data)
	})

	t.Run("flush_empty_database", func(t *testing.T) {
		// Create an empty mock store
		mockStore := &SimpleMockStore{
			data: map[string]string{},
		}

		// Call FLUSHDB handler
		handler := FlushDBHandler(mockStore)
		result, err := handler([]resp.Value{})

		// Verify no error
		assert.NoError(t, err)
		assert.Equal(t, "OK", result.Str)

		// Verify store is still empty
		assert.Empty(t, mockStore.data)
	})

	t.Run("flush_with_arguments_ignored", func(t *testing.T) {
		// Create a mock store
		mockStore := &SimpleMockStore{
			data: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}

		// Call FLUSHDB handler with arguments (should be ignored)
		handler := FlushDBHandler(mockStore)
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "arg1"},
			{Type: resp.BulkString, Str: "arg2"},
		})

		// Verify no error
		assert.NoError(t, err)
		assert.Equal(t, "OK", result.Str)

		// Verify all keys were removed
		assert.Empty(t, mockStore.data)
	})
}

// TestFlushDBCommandRegistration removed - RegisterDefaultCommands not available in optimized version
