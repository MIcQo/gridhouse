package aof

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore implements the Store interface for testing
type mockStore struct {
	data map[string]string
	ttls map[string]int64
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string]string),
		ttls: make(map[string]int64),
	}
}

func (m *mockStore) Keys() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *mockStore) Get(key string) (string, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *mockStore) TTL(key string) int64 {
	return m.ttls[key]
}

func (m *mockStore) Set(key, value string, ttl int64) {
	m.data[key] = value
	if ttl > 0 {
		m.ttls[key] = ttl
	}
}

func TestRewriteConfig(t *testing.T) {
	t.Run("default config", func(t *testing.T) {
		config := DefaultRewriteConfig()

		assert.True(t, config.Enabled)
		assert.Equal(t, int64(64*1024*1024), config.GrowthThreshold) // 64MB
		assert.Equal(t, int64(32*1024*1024), config.MinRewriteSize)  // 32MB
		assert.Equal(t, 100, config.RewritePercentage)
	})
}

func TestRewriteManager(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "test.aof")

	t.Run("should not rewrite when disabled", func(t *testing.T) {
		config := &RewriteConfig{Enabled: false}
		manager := NewRewriteManager(aofPath, config)

		assert.False(t, manager.ShouldRewrite(100*1024*1024)) // 100MB
	})

	t.Run("should not rewrite when below minimum size", func(t *testing.T) {
		config := &RewriteConfig{
			Enabled:        true,
			MinRewriteSize: 32 * 1024 * 1024, // 32MB
		}
		manager := NewRewriteManager(aofPath, config)

		assert.False(t, manager.ShouldRewrite(16*1024*1024)) // 16MB
	})

	t.Run("should rewrite when growth threshold exceeded", func(t *testing.T) {
		config := &RewriteConfig{
			Enabled:         true,
			MinRewriteSize:  1 * 1024 * 1024,  // 1MB
			GrowthThreshold: 64 * 1024 * 1024, // 64MB
		}
		manager := NewRewriteManager(aofPath, config)

		// Set initial size
		manager.baseSize = 10 * 1024 * 1024 // 10MB

		// Current size exceeds growth threshold
		currentSize := int64(10*1024*1024 + 65*1024*1024) // 10MB + 65MB = 75MB
		assert.True(t, manager.ShouldRewrite(currentSize))
	})

	t.Run("should rewrite when percentage threshold exceeded", func(t *testing.T) {
		config := &RewriteConfig{
			Enabled:           true,
			MinRewriteSize:    1 * 1024 * 1024, // 1MB
			RewritePercentage: 100,             // 100%
		}
		manager := NewRewriteManager(aofPath, config)

		// Set initial size
		manager.baseSize = 10 * 1024 * 1024 // 10MB

		// Current size is 200% of original (100% growth)
		currentSize := int64(20 * 1024 * 1024) // 20MB
		assert.True(t, manager.ShouldRewrite(currentSize))
	})
}

func TestPerformRewrite(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "test.aof")

	t.Run("perform rewrite with data", func(t *testing.T) {
		// Create initial AOF file with some data
		initialData := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
		err := os.WriteFile(aofPath, []byte(initialData), 0644)
		require.NoError(t, err)

		config := &RewriteConfig{
			Enabled:        true,
			MinRewriteSize: 1, // 1 byte for testing
		}
		manager := NewRewriteManager(aofPath, config)

		// Create mock store with data
		store := newMockStore()
		store.Set("key1", "value1", 0)
		store.Set("key2", "value2", 3600) // 1 hour TTL

		// Perform rewrite
		err = manager.PerformRewrite(store)
		require.NoError(t, err)

		// Check that file was rewritten
		content, err := os.ReadFile(aofPath)
		require.NoError(t, err)

		// Should contain SET commands for both keys
		assert.Contains(t, string(content), "SET")
		assert.Contains(t, string(content), "key1")
		assert.Contains(t, string(content), "key2")
		assert.Contains(t, string(content), "value1")
		assert.Contains(t, string(content), "value2")

		// Should contain EXPIRE command for key2
		assert.Contains(t, string(content), "EXPIRE")
		assert.Contains(t, string(content), "3600")
	})

	t.Run("perform rewrite with empty store", func(t *testing.T) {
		// Create initial AOF file
		initialData := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
		err := os.WriteFile(aofPath, []byte(initialData), 0644)
		require.NoError(t, err)

		config := &RewriteConfig{
			Enabled:        true,
			MinRewriteSize: 1,
		}
		manager := NewRewriteManager(aofPath, config)

		// Create empty store
		store := newMockStore()

		// Perform rewrite
		err = manager.PerformRewrite(store)
		require.NoError(t, err)

		// Check that file was rewritten (should be empty or minimal)
		content, err := os.ReadFile(aofPath)
		require.NoError(t, err)

		// Should not contain the original data
		assert.NotContains(t, string(content), "key")
		assert.NotContains(t, string(content), "value")
	})
}

func TestGetStats(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "test.aof")

	t.Run("get stats", func(t *testing.T) {
		config := &RewriteConfig{Enabled: true}
		manager := NewRewriteManager(aofPath, config)

		// Create a file with some size
		err := os.WriteFile(aofPath, []byte("test data"), 0644)
		require.NoError(t, err)

		stats := manager.GetStats()

		assert.False(t, stats["aof_rewrite_in_progress"].(bool))
		assert.False(t, stats["aof_rewrite_scheduled"].(bool))
		// lastRewrite time should be 0 when never rewritten
		assert.Equal(t, int64(0), stats["aof_last_rewrite_time_sec"].(int64))
		assert.Equal(t, 0, stats["aof_current_rewrite_time_sec"].(int))
		assert.Equal(t, 0, stats["aof_rewrite_buffer_length"].(int))
		assert.False(t, stats["aof_pending_rewrite"].(bool))
		assert.Equal(t, 0, stats["aof_delayed_fsync"].(int))
		assert.Equal(t, int64(0), stats["aof_base_size"].(int64))
		assert.Equal(t, int64(9), stats["aof_current_size"].(int64)) // "test data" is 9 bytes
		assert.Equal(t, int64(0), stats["aof_growth"].(int64))
	})
}
