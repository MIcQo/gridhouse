package cmd

import (
	"gridhouse/internal/resp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenameNxHandler(t *testing.T) {
	t.Run("basic string rename", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
		// Setup
		store.Set("oldkey", "value", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify old key is gone
		assert.False(t, store.Exists("oldkey"))

		// Verify new key has the value
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "value", value)
	})

	t.Run("rename with expiration", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify new key has the same TTL
		oldTTL := store.TTL("oldkey")
		newTTL := store.TTL("newkey")
		assert.Equal(t, int64(-2), oldTTL)  // Old key should be gone
		assert.Greater(t, newTTL, int64(0)) // New key should have TTL
	})

	t.Run("destination key already exists", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify old key still exists
		assert.True(t, store.Exists("oldkey"))

		// Verify new key still has original value
		value, exists := store.Get("newkey")
		assert.True(t, exists)
		assert.Equal(t, "existingvalue", value)
	})

	t.Run("source key does not exist", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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

	t.Run("rename list data structure", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

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
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

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
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

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
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

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
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
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
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

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

	t.Run("destination key exists for list", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
		// Setup
		oldList := store.GetOrCreateList("oldkey")
		oldList.LPush("item1")
		oldList.RPush("item2")

		existingList := store.GetOrCreateList("newkey")
		existingList.LPush("existing_item")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify old key still exists
		assert.True(t, store.Exists("oldkey"))

		// Verify new key still has original content
		newList := store.GetOrCreateList("newkey")
		assert.Equal(t, 1, newList.LLen())
		items := newList.LRange(0, -1)
		assert.Equal(t, []string{"existing_item"}, items)
	})

	t.Run("destination key exists for set", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
		// Setup
		oldSet := store.GetOrCreateSet("oldkey")
		oldSet.SAdd("member1")
		oldSet.SAdd("member2")

		existingSet := store.GetOrCreateSet("newkey")
		existingSet.SAdd("existing_member")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify old key still exists
		assert.True(t, store.Exists("oldkey"))

		// Verify new key still has original content
		newSet := store.GetOrCreateSet("newkey")
		assert.Equal(t, 1, newSet.SCard())
		members := newSet.SMembers()
		assert.Contains(t, members, "existing_member")
	})

	t.Run("destination key exists for hash", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
		// Setup
		oldHash := store.GetOrCreateHash("oldkey")
		oldHash.HSet("field1", "value1")
		oldHash.HSet("field2", "value2")

		existingHash := store.GetOrCreateHash("newkey")
		existingHash.HSet("existing_field", "existing_value")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify old key still exists
		assert.True(t, store.Exists("oldkey"))

		// Verify new key still has original content
		newHash := store.GetOrCreateHash("newkey")
		assert.Equal(t, 1, newHash.HLen())
		fields := newHash.HGetAll()
		assert.Equal(t, "existing_value", fields["existing_field"])
	})

	t.Run("destination key exists for sorted set", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
		// Setup
		oldZSet := store.GetOrCreateSortedSet("oldkey")
		oldZSet.ZAdd(map[string]float64{
			"member1": 1.0,
			"member2": 2.0,
		})

		existingZSet := store.GetOrCreateSortedSet("newkey")
		existingZSet.ZAdd(map[string]float64{
			"existing_member": 5.0,
		})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify old key still exists
		assert.True(t, store.Exists("oldkey"))

		// Verify new key still has original content
		newZSet := store.GetOrCreateSortedSet("newkey")
		assert.Equal(t, 1, newZSet.ZCard())
		members := newZSet.ZRange(0, -1, false)
		assert.Equal(t, []string{"existing_member"}, members)
	})

	t.Run("destination key exists for stream", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
		// Setup
		oldStream := store.GetOrCreateStream("oldkey")
		oldStream.XAdd(nil, map[string]string{"field1": "value1"})
		oldStream.XAdd(nil, map[string]string{"field2": "value2"})

		existingStream := store.GetOrCreateStream("newkey")
		existingStream.XAdd(nil, map[string]string{"existing_field": "existing_value"})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "oldkey"},
			{Type: resp.BulkString, Str: "newkey"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify old key still exists
		assert.True(t, store.Exists("oldkey"))

		// Verify new key still has original content
		newStream := store.GetOrCreateStream("newkey")
		assert.Equal(t, 1, newStream.XLen())

		// entries := newStream.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
		// assert.Equal(t, 1, len(entries))
		// assert.Equal(t, "existing_value", entries[0].Fields["existing_field"])
	})

	t.Run("rename to same key", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := RenameNxHandler(store)
		// Setup
		store.Set("key", "value", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify key still exists with same value
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "value", value)
	})
}
