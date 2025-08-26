package cmd

import (
	"gridhouse/internal/resp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyHandler(t *testing.T) {
	t.Run("basic string copy", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		store.Set("source", "value", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the value
		value, exists := store.Get("dest")
		assert.True(t, exists)
		assert.Equal(t, "value", value)
	})

	t.Run("copy with expiration", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		expiration := time.Now().Add(10 * time.Second)
		store.Set("source", "value", expiration)

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify both keys have TTL
		sourceTTL := store.TTL("source")
		destTTL := store.TTL("dest")
		assert.Greater(t, sourceTTL, int64(0))
		assert.Greater(t, destTTL, int64(0))
	})

	t.Run("destination key already exists", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		store.Set("source", "sourcevalue", time.Time{})
		store.Set("dest", "existingvalue", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(0), result.Int) // Should return 0 when destination exists

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key still has original value
		value, exists := store.Get("dest")
		assert.True(t, exists)
		assert.Equal(t, "existingvalue", value)
	})

	t.Run("copy with REPLACE option", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		store.Set("source", "sourcevalue", time.Time{})
		store.Set("dest", "existingvalue", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
			{Type: resp.BulkString, Str: "REPLACE"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int) // Should return 1 when REPLACE is used

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the new value
		value, exists := store.Get("dest")
		assert.True(t, exists)
		assert.Equal(t, "sourcevalue", value)
	})

	t.Run("source key does not exist", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Execute
		_, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "nonexistent"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ERR no such key")
	})

	t.Run("wrong number of arguments", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Test with too few arguments
		_, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")

		// Test with too many arguments
		_, err = handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
			{Type: resp.BulkString, Str: "REPLACE"},
			{Type: resp.BulkString, Str: "extra"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")

		// Test with invalid third argument
		_, err = handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
			{Type: resp.BulkString, Str: "INVALID"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")
	})

	t.Run("copy list data structure", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		sourceList := store.GetOrCreateList("source")
		sourceList.LPush("item1")
		sourceList.RPush("item2")
		sourceList.RPush("item3")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the list with all items
		destList := store.GetOrCreateList("dest")
		assert.Equal(t, 3, destList.LLen())

		// Check items are in correct order
		items := destList.LRange(0, -1)
		assert.Equal(t, []string{"item1", "item2", "item3"}, items)
	})

	t.Run("copy set data structure", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		sourceSet := store.GetOrCreateSet("source")
		sourceSet.SAdd("member1")
		sourceSet.SAdd("member2")
		sourceSet.SAdd("member3")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the set with all members
		destSet := store.GetOrCreateSet("dest")
		assert.Equal(t, 3, destSet.SCard())

		// Check all members are present
		members := destSet.SMembers()
		assert.Contains(t, members, "member1")
		assert.Contains(t, members, "member2")
		assert.Contains(t, members, "member3")
	})

	t.Run("copy hash data structure", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		sourceHash := store.GetOrCreateHash("source")
		sourceHash.HSet("field1", "value1")
		sourceHash.HSet("field2", "value2")
		sourceHash.HSet("field3", "value3")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the hash with all fields
		destHash := store.GetOrCreateHash("dest")
		assert.Equal(t, 3, destHash.HLen())

		// Check all fields are present
		fields := destHash.HGetAll()
		assert.Equal(t, "value1", fields["field1"])
		assert.Equal(t, "value2", fields["field2"])
		assert.Equal(t, "value3", fields["field3"])
	})

	t.Run("copy sorted set data structure", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		sourceZSet := store.GetOrCreateSortedSet("source")
		sourceZSet.ZAdd(map[string]float64{
			"member1": 1.0,
			"member2": 2.0,
			"member3": 3.0,
		})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the sorted set with all members
		destZSet := store.GetOrCreateSortedSet("dest")
		assert.Equal(t, 3, destZSet.ZCard())

		// Check all members with their scores
		members := destZSet.ZRange(0, -1, false)
		assert.Equal(t, []string{"member1", "member2", "member3"}, members)

		score1, exists := destZSet.ZScore("member1")
		assert.True(t, exists)
		assert.Equal(t, 1.0, score1)

		score2, exists := destZSet.ZScore("member2")
		assert.True(t, exists)
		assert.Equal(t, 2.0, score2)

		score3, exists := destZSet.ZScore("member3")
		assert.True(t, exists)
		assert.Equal(t, 3.0, score3)
	})

	t.Run("copy stream data structure", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		sourceStream := store.GetOrCreateStream("source")
		sourceStream.XAdd(nil, map[string]string{"field1": "value1"})
		sourceStream.XAdd(nil, map[string]string{"field2": "value2"})
		sourceStream.XAdd(nil, map[string]string{"field3": "value3"})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the stream with all entries
		destStream := store.GetOrCreateStream("dest")
		assert.Equal(t, 3, destStream.XLen())

		// Check entries are present
		// entries := destStream.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
		// assert.Equal(t, 3, len(entries))

		// Check first entry has field1
		// assert.Equal(t, "value1", entries[0].Fields["field1"])
		// Check second entry has field2
		// assert.Equal(t, "value2", entries[1].Fields["field2"])
		// Check third entry has field3
		// assert.Equal(t, "value3", entries[2].Fields["field3"])
	})

	t.Run("copy to same key", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
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

	t.Run("copy to same key with REPLACE", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		store.Set("key", "value", time.Time{})

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "key"},
			{Type: resp.BulkString, Str: "REPLACE"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int) // Should return 1 when REPLACE is used

		// Verify key still exists with same value (copying to itself)
		value, exists := store.Get("key")
		assert.True(t, exists)
		assert.Equal(t, "value", value)
	})

	t.Run("copy list with REPLACE", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		sourceList := store.GetOrCreateList("source")
		sourceList.LPush("item1")
		sourceList.RPush("item2")

		destList := store.GetOrCreateList("dest")
		destList.LPush("existing_item")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
			{Type: resp.BulkString, Str: "REPLACE"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the new content (replaced)
		newDestList := store.GetOrCreateList("dest")
		assert.Equal(t, 2, newDestList.LLen())
		items := newDestList.LRange(0, -1)
		assert.Equal(t, []string{"item1", "item2"}, items)
	})

	t.Run("copy hash with REPLACE", func(t *testing.T) {
		store := NewRenameMockStore()
		handler := CopyHandler(store)
		// Setup
		sourceHash := store.GetOrCreateHash("source")
		sourceHash.HSet("field1", "value1")
		sourceHash.HSet("field2", "value2")

		destHash := store.GetOrCreateHash("dest")
		destHash.HSet("existing_field", "existing_value")

		// Execute
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "source"},
			{Type: resp.BulkString, Str: "dest"},
			{Type: resp.BulkString, Str: "REPLACE"},
		})

		// Assert
		require.NoError(t, err)
		assert.Equal(t, resp.Integer, result.Type)
		assert.Equal(t, int64(1), result.Int)

		// Verify source key still exists
		assert.True(t, store.Exists("source"))

		// Verify destination key has the new content (replaced)
		newDestHash := store.GetOrCreateHash("dest")
		assert.Equal(t, 2, newDestHash.HLen())
		fields := newDestHash.HGetAll()
		assert.Equal(t, "value1", fields["field1"])
		assert.Equal(t, "value2", fields["field2"])
	})
}
