package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
)

// appendMockStore implements Store interface for testing APPEND command
type appendMockStore struct {
	data map[string]string
	ttl  map[string]time.Time
}

func newAppendMockStore() *appendMockStore {
	return &appendMockStore{
		data: make(map[string]string),
		ttl:  make(map[string]time.Time),
	}
}

func (s *appendMockStore) Get(key string) (string, bool) {
	value, exists := s.data[key]
	return value, exists
}

func (s *appendMockStore) Set(key, value string, expiration time.Time) {
	s.data[key] = value
	s.ttl[key] = expiration
}

func (s *appendMockStore) Del(key string) bool {
	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		delete(s.ttl, key)
		return true
	}
	return false
}

func (s *appendMockStore) Exists(key string) bool {
	_, exists := s.data[key]
	return exists
}

func (s *appendMockStore) Keys() []string {
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	return keys
}

func (s *appendMockStore) TTL(key string) int64 {
	if _, exists := s.data[key]; !exists {
		return -2
	}
	if s.ttl[key].IsZero() {
		return -1
	}
	return int64(time.Until(s.ttl[key]).Seconds())
}

func (s *appendMockStore) PTTL(key string) int64 {
	if _, exists := s.data[key]; !exists {
		return -2
	}
	if s.ttl[key].IsZero() {
		return -1
	}
	return int64(time.Until(s.ttl[key]).Milliseconds())
}

func (s *appendMockStore) Expire(key string, duration time.Duration) bool {
	if _, exists := s.data[key]; !exists {
		return false
	}
	s.ttl[key] = time.Now().Add(duration)
	return true
}

func (s *appendMockStore) GetOrCreateList(key string) *store.List {
	return store.NewList()
}

func (s *appendMockStore) GetOrCreateSet(key string) *store.Set {
	return store.NewSet()
}

func (s *appendMockStore) GetOrCreateHash(key string) *store.Hash {
	return store.NewHash()
}

func (s *appendMockStore) GetOrCreateStream(key string) *store.Stream {
	return store.NewStream()
}

func TestAppendHandler(t *testing.T) {
	t.Run("APPEND with no arguments", func(t *testing.T) {
		store := newAppendMockStore()
		handler := AppendHandler(store)
		args := []resp.Value{}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'APPEND' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("APPEND with one argument", func(t *testing.T) {
		store := newAppendMockStore()
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'APPEND' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("APPEND with too many arguments", func(t *testing.T) {
		store := newAppendMockStore()
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "value1"},
			{Type: resp.BulkString, Str: "value2"},
		}

		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'APPEND' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("APPEND to non-existent key", func(t *testing.T) {
		store := newAppendMockStore()
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "newkey"},
			{Type: resp.BulkString, Str: "hello"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)

		// Verify the value was stored
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "hello", value)
	})

	t.Run("APPEND to existing key", func(t *testing.T) {
		store := newAppendMockStore()
		store.Set("existingkey", "hello", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "existingkey"},
			{Type: resp.BulkString, Str: " world"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(11), result.Int)

		// Verify the value was updated
		value, exists := store.Get("existingkey")
		assert.True(t, exists)
		assert.Equal(t, "hello world", value)
	})

	t.Run("APPEND empty string to existing key", func(t *testing.T) {
		store := newAppendMockStore()
		store.Set("key", "hello", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: ""},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)

		// Verify the value was not changed
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "hello", value)
	})

	t.Run("APPEND to empty string", func(t *testing.T) {
		store := newAppendMockStore()
		store.Set("emptykey", "", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "emptykey"},
			{Type: resp.BulkString, Str: "hello"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)

		// Verify the value was updated
		value, exists := store.Get("emptykey")
		assert.True(t, exists)
		assert.Equal(t, "hello", value)
	})

	t.Run("APPEND multiple times", func(t *testing.T) {
		store := newAppendMockStore()
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "multikey"},
			{Type: resp.BulkString, Str: "hello"},
		}

		// First append
		result1, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), result1.Int)

		// Second append
		args[1].Str = " world"
		result2, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(11), result2.Int)

		// Third append
		args[1].Str = "!"
		result3, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), result3.Int)

		// Verify final value
		value, exists := store.Get("multikey")
		assert.True(t, exists)
		assert.Equal(t, "hello world!", value)
	})

	t.Run("APPEND with special characters", func(t *testing.T) {
		store := newAppendMockStore()
		store.Set("specialkey", "hello", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "specialkey"},
			{Type: resp.BulkString, Str: "\n\t\r"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(8), result.Int)

		// Verify the value was updated
		value, exists := store.Get("specialkey")
		assert.True(t, exists)
		assert.Equal(t, "hello\n\t\r", value)
	})

	t.Run("APPEND with unicode characters", func(t *testing.T) {
		store := newAppendMockStore()
		store.Set("unicodekey", "hello", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "unicodekey"},
			{Type: resp.BulkString, Str: " 世界 🌍"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(17), result.Int)

		// Verify the value was updated
		value, exists := store.Get("unicodekey")
		assert.True(t, exists)
		assert.Equal(t, "hello 世界 🌍", value)
	})

	t.Run("APPEND with numeric string", func(t *testing.T) {
		store := newAppendMockStore()
		store.Set("numkey", "123", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "numkey"},
			{Type: resp.BulkString, Str: "456"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(6), result.Int)

		// Verify the value was updated
		value, exists := store.Get("numkey")
		assert.True(t, exists)
		assert.Equal(t, "123456", value)
	})

	t.Run("APPEND with key containing special characters", func(t *testing.T) {
		store := newAppendMockStore()
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "key-with-dashes_123"},
			{Type: resp.BulkString, Str: "value"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(5), result.Int)

		// Verify the value was stored
		value, exists := store.Get("key-with-dashes_123")
		assert.True(t, exists)
		assert.Equal(t, "value", value)
	})

	t.Run("APPEND with zero byte string", func(t *testing.T) {
		store := newAppendMockStore()
		store.Set("zerokey", "hello", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "zerokey"},
			{Type: resp.BulkString, Str: "\x00"},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(6), result.Int)

		// Verify the value was updated
		value, exists := store.Get("zerokey")
		assert.True(t, exists)
		assert.Equal(t, "hello\x00", value)
	})

	t.Run("APPEND with very long string", func(t *testing.T) {
		store := newAppendMockStore()
		longString := "This is a very long string that will be appended to test the APPEND command with a large amount of data"
		store.Set("longkey", "prefix: ", time.Time{})
		handler := AppendHandler(store)
		args := []resp.Value{
			{Type: resp.BulkString, Str: "longkey"},
			{Type: resp.BulkString, Str: longString},
		}

		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(len("prefix: ")+len(longString)), result.Int)

		// Verify the value was updated
		value, exists := store.Get("longkey")
		assert.True(t, exists)
		assert.Equal(t, "prefix: "+longString, value)
	})
}
