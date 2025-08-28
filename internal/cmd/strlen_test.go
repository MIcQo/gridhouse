package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
)

// strlenMockStore implements Store interface for testing STRLEN command
type strlenMockStore struct {
	data map[string]string
	ttl  map[string]time.Time
}

func newStrlenMockStore() *strlenMockStore {
	return &strlenMockStore{
		data: make(map[string]string),
		ttl:  make(map[string]time.Time),
	}
}

func (s *strlenMockStore) Get(key string) (string, bool) {
	value, exists := s.data[key]
	return value, exists
}

func (s *strlenMockStore) Set(key, value string, expiration time.Time) {
	s.data[key] = value
	s.ttl[key] = expiration
}

func (s *strlenMockStore) Del(key string) bool {
	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		delete(s.ttl, key)
		return true
	}
	return false
}

func (s *strlenMockStore) Exists(key string) bool {
	_, exists := s.data[key]
	return exists
}

func (s *strlenMockStore) Keys() []string {
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	return keys
}

func (s *strlenMockStore) TTL(key string) int64 {
	if _, exists := s.data[key]; !exists {
		return -2
	}
	if s.ttl[key].IsZero() {
		return -1
	}
	return int64(time.Until(s.ttl[key]).Seconds())
}

func (s *strlenMockStore) PTTL(key string) int64 {
	if _, exists := s.data[key]; !exists {
		return -2
	}
	if s.ttl[key].IsZero() {
		return -1
	}
	return int64(time.Until(s.ttl[key]).Milliseconds())
}

func (s *strlenMockStore) Expire(key string, duration time.Duration) bool {
	if _, exists := s.data[key]; !exists {
		return false
	}
	s.ttl[key] = time.Now().Add(duration)
	return true
}

func (s *strlenMockStore) GetOrCreateList(key string) *store.List {
	return store.NewList()
}

func (s *strlenMockStore) GetOrCreateSet(key string) *store.Set {
	return store.NewSet()
}

func (s *strlenMockStore) GetOrCreateHash(key string) *store.Hash {
	return store.NewHash()
}

func (s *strlenMockStore) GetOrCreateStream(key string) *store.Stream {
	return store.NewStream()
}

func TestStrlenHandler(t *testing.T) {
	t.Run("STRLEN with no arguments", func(t *testing.T) {
		store := newStrlenMockStore()
		handler := StrlenHandler(store)
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'STRLEN' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("STRLEN with too many arguments", func(t *testing.T) {
		store := newStrlenMockStore()
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "key2"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'STRLEN' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("STRLEN with non-existent key", func(t *testing.T) {
		store := newStrlenMockStore()
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "nonexistent"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int)
	})

	t.Run("STRLEN with empty string", func(t *testing.T) {
		store := newStrlenMockStore()
		store.Set("emptykey", "", time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "emptykey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int)
	})

	t.Run("STRLEN with simple string", func(t *testing.T) {
		store := newStrlenMockStore()
		store.Set("simplekey", "hello", time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "simplekey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)
	})

	t.Run("STRLEN with long string", func(t *testing.T) {
		store := newStrlenMockStore()
		longString := "This is a very long string with many characters to test the STRLEN command"
		store.Set("longkey", longString, time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "longkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(len(longString)), result.Int)
	})

	t.Run("STRLEN with special characters", func(t *testing.T) {
		store := newStrlenMockStore()
		specialString := "Hello\nWorld\tTest\r\n"
		store.Set("specialkey", specialString, time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "specialkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(len(specialString)), result.Int)
	})

	t.Run("STRLEN with unicode characters", func(t *testing.T) {
		store := newStrlenMockStore()
		unicodeString := "Hello 世界 🌍"
		store.Set("unicodekey", unicodeString, time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "unicodekey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(len(unicodeString)), result.Int)
	})

	t.Run("STRLEN with numeric string", func(t *testing.T) {
		store := newStrlenMockStore()
		store.Set("numkey", "12345", time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "numkey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)
	})

	t.Run("STRLEN with key containing special characters", func(t *testing.T) {
		store := newStrlenMockStore()
		store.Set("key-with-dashes_123", "test", time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key-with-dashes_123"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(4), result.Int)
	})

	t.Run("STRLEN with zero byte string", func(t *testing.T) {
		store := newStrlenMockStore()
		store.Set("zerokey", "\x00", time.Time{})
		handler := StrlenHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "zerokey"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)
	})
}
