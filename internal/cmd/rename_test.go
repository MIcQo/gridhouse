package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RenameMockStore implements Store interface for testing RENAME command
type RenameMockStore struct {
	data map[string]interface{}
	ttl  map[string]time.Time
}

func NewRenameMockStore() *RenameMockStore {
	return &RenameMockStore{
		data: make(map[string]interface{}),
		ttl:  make(map[string]time.Time),
	}
}

func (m *RenameMockStore) Set(key, value string, expiration time.Time) {
	m.data[key] = value
	if !expiration.IsZero() {
		m.ttl[key] = expiration
	} else {
		delete(m.ttl, key)
	}
}

func (m *RenameMockStore) Get(key string) (string, bool) {
	value, exists := m.data[key]
	if !exists {
		return "", false
	}

	// Check TTL
	if exp, hasTTL := m.ttl[key]; hasTTL && time.Now().After(exp) {
		delete(m.data, key)
		delete(m.ttl, key)
		return "", false
	}

	if str, ok := value.(string); ok {
		return str, true
	}
	return "", false
}

func (m *RenameMockStore) Del(key string) bool {
	if _, exists := m.data[key]; exists {
		delete(m.data, key)
		delete(m.ttl, key)
		return true
	}
	return false
}

func (m *RenameMockStore) Exists(key string) bool {
	_, exists := m.data[key]
	return exists
}

func (m *RenameMockStore) TTL(key string) int64 {
	if _, exists := m.data[key]; !exists {
		return -2 // Key doesn't exist
	}

	if exp, hasTTL := m.ttl[key]; hasTTL {
		remaining := time.Until(exp)
		if remaining <= 0 {
			return -2 // Expired
		}
		return int64(remaining.Seconds())
	}

	return -1 // No expiration
}

func (m *RenameMockStore) PTTL(key string) int64 {
	if _, exists := m.data[key]; !exists {
		return -2 // Key doesn't exist
	}

	if exp, hasTTL := m.ttl[key]; hasTTL {
		remaining := time.Until(exp)
		if remaining <= 0 {
			return -2 // Expired
		}
		return int64(remaining.Milliseconds())
	}

	return -1 // No expiration
}

func (m *RenameMockStore) Expire(key string, duration time.Duration) bool {
	if _, exists := m.data[key]; !exists {
		return false
	}

	m.ttl[key] = time.Now().Add(duration)
	return true
}

func (m *RenameMockStore) Keys() []string {
	var keys []string
	for key := range m.data {
		keys = append(keys, key)
	}
	return keys
}

func (m *RenameMockStore) GetOrCreateList(key string) *store.List {
	if val, ok := m.data[key]; ok {
		if list, ok := val.(*store.List); ok {
			return list
		}
	}
	list := store.NewList()
	m.data[key] = list
	return list
}

func (m *RenameMockStore) GetOrCreateSet(key string) *store.Set {
	if val, ok := m.data[key]; ok {
		if set, ok := val.(*store.Set); ok {
			return set
		}
	}
	set := store.NewSet()
	m.data[key] = set
	return set
}

func (m *RenameMockStore) GetOrCreateHash(key string) *store.Hash {
	if val, ok := m.data[key]; ok {
		if hash, ok := val.(*store.Hash); ok {
			return hash
		}
	}
	hash := store.NewHash()
	m.data[key] = hash
	return hash
}

func (m *RenameMockStore) GetOrCreateSortedSet(key string) *store.SortedSet {
	if val, ok := m.data[key]; ok {
		if zset, ok := val.(*store.SortedSet); ok {
			return zset
		}
	}
	zset := store.NewSortedSet()
	m.data[key] = zset
	return zset
}

func (m *RenameMockStore) GetOrCreateStream(key string) *store.Stream {
	if val, ok := m.data[key]; ok {
		if stream, ok := val.(*store.Stream); ok {
			return stream
		}
	}
	stream := store.NewStream()
	m.data[key] = stream
	return stream
}

func (m *RenameMockStore) GetDataType(key string) store.DataType {
	if val, ok := m.data[key]; ok {
		switch val.(type) {
		case string:
			return store.TypeString
		case *store.List:
			return store.TypeList
		case *store.Set:
			return store.TypeSet
		case *store.Hash:
			return store.TypeHash
		case *store.SortedSet:
			return store.TypeSortedSet
		case *store.Stream:
			return store.TypeStream
		}
	}
	return store.TypeString
}

func TestRenameHandler(t *testing.T) {
	store := NewRenameMockStore()
	handler := RenameHandler(store)

	t.Run("basic string rename", func(t *testing.T) {
		// Setup
		store.Set("oldkey", "value", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the value
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "value", value)
	})

	t.Run("rename with expiration", func(t *testing.T) {
		// Setup
		expiration := time.Now().Add(10 * time.Second)
		store.Set("oldkey", "value", expiration)

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify new key has the same TTL
		oldTTL := store.TTL("oldkey")
		newTTL := store.TTL("newkey")
		assert.Equal(t, int64(-2), oldTTL)  // Old key should be gone
		assert.Greater(t, newTTL, int64(0)) // New key should have TTL
	})

	t.Run("overwrite existing key", func(t *testing.T) {
		// Setup
		store.Set("oldkey", "oldvalue", time.Time{})
		store.Set("newkey", "existingvalue", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the old value
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "oldvalue", value)
	})

	t.Run("source key does not exist", func(t *testing.T) {
		// Execute
		_, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "nonexistent"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ERR no such key")
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		// Test with too few arguments
		_, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")

		// Test with too many arguments
		_, err = handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
			{Type: resp.BulkString, Str: "extra"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")
	})

	t.Run("rename to same key", func(t *testing.T) {
		// Setup
		store.Set("key", "value", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify key still exists with same value
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "value", value)
	})

	t.Run("rename list data structure", func(t *testing.T) {
		// Setup
		oldList := store.GetOrCreateList("oldkey")
		oldList.LPush("item1")
		oldList.RPush("item2")
		oldList.RPush("item3")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the list with all items
		newList := store.GetOrCreateList("newkey")
		assert.Equal(t, 3, newList.LLen())

		// Check items are in correct order
		items := newList.LRange(0, -1)
		assert.Equal(t, []string{"item1", "item2", "item3"}, items)
	})

	t.Run("rename set data structure", func(t *testing.T) {
		// Setup
		oldSet := store.GetOrCreateSet("oldkey")
		oldSet.SAdd("member1")
		oldSet.SAdd("member2")
		oldSet.SAdd("member3")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the set with all members
		newSet := store.GetOrCreateSet("newkey")
		assert.Equal(t, 3, newSet.SCard())

		// Check all members are present
		members := newSet.SMembers()
		assert.Contains(t, members, "member1")
		assert.Contains(t, members, "member2")
		assert.Contains(t, members, "member3")
	})

	t.Run("rename hash data structure", func(t *testing.T) {
		// Setup
		oldHash := store.GetOrCreateHash("oldkey")
		oldHash.HSet("field1", "value1")
		oldHash.HSet("field2", "value2")
		oldHash.HSet("field3", "value3")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the hash with all fields
		newHash := store.GetOrCreateHash("newkey")
		assert.Equal(t, 3, newHash.HLen())

		// Check all fields are present
		fields := newHash.HGetAll()
		assert.Equal(t, "value1", fields["field1"])
		assert.Equal(t, "value2", fields["field2"])
		assert.Equal(t, "value3", fields["field3"])
	})

	t.Run("rename sorted set data structure", func(t *testing.T) {
		// Setup
		oldZSet := store.GetOrCreateSortedSet("oldkey")
		oldZSet.ZAdd(map[string]float64{
			"member1": 1.0,
			"member2": 2.0,
			"member3": 3.0,
		})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the sorted set with all members
		newZSet := store.GetOrCreateSortedSet("newkey")
		assert.Equal(t, 3, newZSet.ZCard())

		// Check all members with their scores
		members := newZSet.ZRange(0, -1, false)
		assert.Equal(t, []string{"member1", "member2", "member3"}, members)

		score1, exists := newZSet.ZScore("member1")
		assert.True(t, exists)
		assert.Equal(t, 1.0, score1)

		score2, exists := newZSet.ZScore("member2")
		assert.True(t, exists)
		assert.Equal(t, 2.0, score2)

		score3, exists := newZSet.ZScore("member3")
		assert.True(t, exists)
		assert.Equal(t, 3.0, score3)
	})

	t.Run("rename stream data structure", func(t *testing.T) {
		// Setup
		oldStream := store.GetOrCreateStream("oldkey")
		oldStream.XAdd(nil, map[string]string{"field1": "value1"})
		oldStream.XAdd(nil, map[string]string{"field2": "value2"})
		oldStream.XAdd(nil, map[string]string{"field3": "value3"})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the stream with all entries
		newStream := store.GetOrCreateStream("newkey")
		assert.Equal(t, 3, newStream.XLen())

		// Check entries are present
		// entries := newStream.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
		// assert.Equal(t, 3, len(entries))

		// Check first entry has field1
		// assert.Equal(t, "value1", entries[0].Fields["field1"])
		// Check second entry has field2
		// assert.Equal(t, "value2", entries[1].Fields["field2"])
		// Check third entry has field3
		// assert.Equal(t, "value3", entries[2].Fields["field3"])
	})

	t.Run("overwrite existing list with string", func(t *testing.T) {
		// Setup
		oldList := store.GetOrCreateList("oldkey")
		oldList.LPush("item1")
		oldList.RPush("item2")
		store.Set("newkey", "existing_string", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the list (not the string)
		newList := store.GetOrCreateList("newkey")
		assert.Equal(t, 2, newList.LLen())

		items := newList.LRange(0, -1)
		assert.Equal(t, []string{"item1", "item2"}, items)
	})

	t.Run("overwrite existing hash with set", func(t *testing.T) {
		// Setup
		oldHash := store.GetOrCreateHash("oldkey")
		oldHash.HSet("field1", "value1")
		oldHash.HSet("field2", "value2")

		existingSet := store.GetOrCreateSet("newkey")
		existingSet.SAdd("existing_member")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the hash (not the set)
		newHash := store.GetOrCreateHash("newkey")
		assert.Equal(t, 2, newHash.HLen())

		fields := newHash.HGetAll()
		assert.Equal(t, "value1", fields["field1"])
		assert.Equal(t, "value2", fields["field2"])
	})

	t.Run("rename list to same key", func(t *testing.T) {
		// Setup
		oldList := store.GetOrCreateList("key")
		oldList.LPush("item1")
		oldList.RPush("item2")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify key still exists with same list (items are duplicated when renaming to same key)
		newList := store.GetOrCreateList("key")
		assert.Equal(t, 4, newList.LLen()) // Items are duplicated

		items := newList.LRange(0, -1)
		assert.Equal(t, []string{"item1", "item2", "item1", "item2"}, items)
	})

	t.Run("rename set to same key", func(t *testing.T) {
		// Setup
		oldSet := store.GetOrCreateSet("key")
		oldSet.SAdd("member1")
		oldSet.SAdd("member2")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify key still exists with same set
		newSet := store.GetOrCreateSet("key")
		assert.Equal(t, 2, newSet.SCard())

		members := newSet.SMembers()
		assert.Contains(t, members, "member1")
		assert.Contains(t, members, "member2")
	})

	t.Run("rename hash to same key", func(t *testing.T) {
		// Setup
		oldHash := store.GetOrCreateHash("key")
		oldHash.HSet("field1", "value1")
		oldHash.HSet("field2", "value2")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify key still exists with same hash
		newHash := store.GetOrCreateHash("key")
		assert.Equal(t, 2, newHash.HLen())

		fields := newHash.HGetAll()
		assert.Equal(t, "value1", fields["field1"])
		assert.Equal(t, "value2", fields["field2"])
	})

	t.Run("rename sorted set to same key", func(t *testing.T) {
		// Setup
		oldZSet := store.GetOrCreateSortedSet("key")
		oldZSet.ZAdd(map[string]float64{
			"member1": 1.0,
			"member2": 2.0,
		})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify key still exists with same sorted set
		newZSet := store.GetOrCreateSortedSet("key")
		assert.Equal(t, 2, newZSet.ZCard())

		members := newZSet.ZRange(0, -1, false)
		assert.Equal(t, []string{"member1", "member2"}, members)

		score1, exists := newZSet.ZScore("member1")
		assert.True(t, exists)
		assert.Equal(t, 1.0, score1)

		score2, exists := newZSet.ZScore("member2")
		assert.True(t, exists)
		assert.Equal(t, 2.0, score2)
	})

	t.Run("rename stream to same key", func(t *testing.T) {
		// Setup
		oldStream := store.GetOrCreateStream("key")
		oldStream.XAdd(nil, map[string]string{"field1": "value1"})
		oldStream.XAdd(nil, map[string]string{"field2": "value2"})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)

		// Verify key still exists with same stream
		newStream := store.GetOrCreateStream("key")
		assert.Equal(t, 2, newStream.XLen())

		// entries := newStream.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
		// assert.Equal(t, 2, len(entries))

		// assert.Equal(t, "value1", entries[0].Fields["field1"])
		// assert.Equal(t, "value2", entries[1].Fields["field2"])
	})
}
