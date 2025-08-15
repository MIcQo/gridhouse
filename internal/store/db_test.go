package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDBSetGet(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Test basic set/get
	db.Set("key1", "value1", time.Time{})
	value, exists := db.Get("key1")
	require.True(t, exists)
	require.Equal(t, "value1", value)

	// Test non-existent key
	value, exists = db.Get("nonexistent")
	require.False(t, exists)
	require.Equal(t, "", value)
}

func TestDBDel(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Set a key
	db.Set("key1", "value1", time.Time{})
	require.True(t, db.Exists("key1"))

	// Delete the key
	require.True(t, db.Del("key1"))
	require.False(t, db.Exists("key1"))

	// Try to delete non-existent key
	require.False(t, db.Del("nonexistent"))
}

func TestDBTTL(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Test key without TTL
	db.Set("key1", "value1", time.Time{})
	ttl := db.TTL("key1")
	require.Equal(t, int64(-1), ttl) // No expiration

	// Test non-existent key
	ttl = db.TTL("nonexistent")
	require.Equal(t, int64(-2), ttl) // Key doesn't exist

	// Test key with TTL
	expiration := time.Now().Add(2 * time.Second)
	db.Set("key2", "value2", expiration)
	ttl = db.TTL("key2")
	require.Greater(t, ttl, int64(0))
	require.LessOrEqual(t, ttl, int64(2))

	// Test expired key
	expiration = time.Now().Add(-1 * time.Second)
	db.Set("key3", "value3", expiration)
	ttl = db.TTL("key3")
	require.Equal(t, int64(-2), ttl) // Expired
}

func TestDBPTTL(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Test key with TTL in milliseconds
	expiration := time.Now().Add(1 * time.Second)
	db.Set("key1", "value1", expiration)
	pttl := db.PTTL("key1")
	require.Greater(t, pttl, int64(0))
	require.LessOrEqual(t, pttl, int64(1000))

	// Test non-existent key
	pttl = db.PTTL("nonexistent")
	require.Equal(t, int64(-2), pttl)
}

func TestDBExpire(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Set a key without expiration
	db.Set("key1", "value1", time.Time{})
	require.Equal(t, int64(-1), db.TTL("key1"))

	// Set expiration
	require.True(t, db.Expire("key1", 2*time.Second))
	ttl := db.TTL("key1")
	require.Greater(t, ttl, int64(0))
	require.LessOrEqual(t, ttl, int64(2))

	// Try to expire non-existent key
	require.False(t, db.Expire("nonexistent", time.Second))
}

func TestDBExpiration(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Set key with short expiration
	expiration := time.Now().Add(100 * time.Millisecond)
	db.Set("key1", "value1", expiration)

	// Key should exist immediately
	value, exists := db.Get("key1")
	require.True(t, exists)
	require.Equal(t, "value1", value)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Key should be expired
	value, exists = db.Get("key1")
	require.False(t, exists)
	require.Equal(t, "", value)
}

func TestDBKeys(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Add some keys
	db.Set("key1", "value1", time.Time{})
	db.Set("key2", "value2", time.Time{})
	db.Set("key3", "value3", time.Time{})

	keys := db.Keys()
	require.Len(t, keys, 3)
	require.Contains(t, keys, "key1")
	require.Contains(t, keys, "key2")
	require.Contains(t, keys, "key3")
}

func TestDBConcurrentAccess(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Test concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				db.Set(key, fmt.Sprintf("value_%d_%d", id, j), time.Time{})
			}
		}(i)
	}

	// Test concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				db.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys were written
	expectedKeys := numGoroutines * numOperations
	keys := db.Keys()
	require.Len(t, keys, expectedKeys)
}

// TestDBStats removed - optimized DB doesn't have Stats method

func TestDBBackgroundCleanup(t *testing.T) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Add keys with different expiration times
	db.Set("key1", "value1", time.Now().Add(50*time.Millisecond))
	db.Set("key2", "value2", time.Now().Add(150*time.Millisecond))
	db.Set("key3", "value3", time.Time{}) // No expiration

	// Initially all keys should exist
	require.True(t, db.Exists("key1"))
	require.True(t, db.Exists("key2"))
	require.True(t, db.Exists("key3"))

	// Wait for first key to expire
	time.Sleep(100 * time.Millisecond)

	// key1 should be expired, others should still exist
	require.False(t, db.Exists("key1"))
	require.True(t, db.Exists("key2"))
	require.True(t, db.Exists("key3"))

	// Wait for second key to expire
	time.Sleep(100 * time.Millisecond)

	// key2 should be expired, key3 should still exist
	require.False(t, db.Exists("key1"))
	require.False(t, db.Exists("key2"))
	require.True(t, db.Exists("key3"))
}
