package persistence

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gridhouse/internal/aof"
	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistenceManagerAOFOnly(t *testing.T) {
	dir := t.TempDir()

	// Create store
	db := store.NewUltraOptimizedDB()

	// Create persistence manager with AOF only
	config := &Config{
		Dir:         dir,
		AOFEnabled:  true,
		AOFSyncMode: aof.EverySec,
		RDBEnabled:  false,
	}

	manager, err := NewManager(config, db)
	require.NoError(t, err)
	defer manager.Close()

	// Add some data
	db.Set("key1", "value1", time.Time{})
	db.Set("key2", "value2", time.Now().Add(time.Hour))

	// Append commands to AOF
	require.NoError(t, manager.AppendCommand("SET", []string{"key1", "value1"}))
	require.NoError(t, manager.AppendCommand("SET", []string{"key2", "value2"}))
	require.NoError(t, manager.AppendMultiCommands("SET", []string{"key3", "value3"}))

	time.Sleep(100 * time.Millisecond)

	// Verify AOF file exists
	aofPath := filepath.Join(dir, "appendonly.aof")
	_, err = os.Stat(aofPath)
	require.NoError(t, err)

	// Check stats
	stats := manager.Stats()
	require.True(t, stats["aof_enabled"].(bool))
	require.False(t, stats["rdb_enabled"].(bool))
	require.Equal(t, 0, stats["changes_since"].(int)) // cannot track changes
	require.Equal(t, config, manager.GetConfig())
}

func TestPersistenceManagerRDBOnly(t *testing.T) {
	dir := t.TempDir()

	// Create store
	db := store.NewUltraOptimizedDB()

	// Create persistence manager with RDB only (no background save for test)
	config := &Config{
		Dir:        dir,
		AOFEnabled: false,
		RDBEnabled: true,
		// No RDBSaveConfig means no background saves
	}

	manager, err := NewManager(config, db)
	require.NoError(t, err)
	defer manager.Close()

	// Add some data
	db.Set("key1", "value1", time.Time{})
	db.Set("key2", "value2", time.Now().Add(time.Hour))

	require.NoError(t, manager.AppendCommand("SET", []string{"key1", "value1"}))
	require.NoError(t, manager.AppendCommand("SET", []string{"key2", "value2"}))
	time.Sleep(100 * time.Millisecond)

	statsBefore := manager.Stats()
	require.Equal(t, 2, statsBefore["changes_since"].(int)) // cannot track changes

	// Manual save
	require.NoError(t, manager.SaveRDB())

	// Verify RDB file exists
	rdbPath := filepath.Join(dir, "dump.rdb")
	_, err = os.Stat(rdbPath)
	require.NoError(t, err)

	// Check stats
	stats := manager.Stats()
	require.False(t, stats["aof_enabled"].(bool))
	require.True(t, stats["rdb_enabled"].(bool))
	require.Equal(t, 0, stats["changes_since"].(int)) // cannot track changes
}

func TestPersistenceManagerBoth(t *testing.T) {
	dir := t.TempDir()

	// Create store
	db := store.NewUltraOptimizedDB()

	// Create persistence manager with both AOF and RDB (no background save for test)
	config := &Config{
		Dir:         dir,
		AOFEnabled:  true,
		AOFSyncMode: aof.Always,
		RDBEnabled:  true,
		// No RDBSaveConfig means no background saves
	}

	manager, err := NewManager(config, db)
	require.NoError(t, err)
	defer manager.Close()

	// Add some data
	db.Set("key1", "value1", time.Time{})
	db.Set("key2", "value2", time.Now().Add(time.Hour))

	// Append commands
	require.NoError(t, manager.AppendCommand("SET", []string{"key1", "value1"}))
	require.NoError(t, manager.AppendCommand("SET", []string{"key2", "value2"}))

	// Manual save
	require.NoError(t, manager.SaveRDB())

	// Verify both files exist
	aofPath := filepath.Join(dir, "appendonly.aof")
	rdbPath := filepath.Join(dir, "dump.rdb")

	_, err = os.Stat(aofPath)
	require.NoError(t, err)

	_, err = os.Stat(rdbPath)
	require.NoError(t, err)

	// Check stats
	stats := manager.Stats()
	require.True(t, stats["aof_enabled"].(bool))
	require.True(t, stats["rdb_enabled"].(bool))
}

func TestPersistenceManagerLoadData(t *testing.T) {
	dir := t.TempDir()

	// Create store and add data
	db := store.NewUltraOptimizedDB()
	db.Set("key1", "value1", time.Time{})
	db.Set("key2", "value2", time.Now().Add(time.Hour))

	// Create persistence manager
	config := &Config{
		Dir:         dir,
		AOFEnabled:  true,
		AOFSyncMode: aof.Always,
		RDBEnabled:  true,
		// No RDBSaveConfig means no background saves
	}

	manager, err := NewManager(config, db)
	require.NoError(t, err)

	// Save RDB
	require.NoError(t, manager.SaveRDB())

	// Close manager
	require.NoError(t, manager.Close())

	// Create new store and manager
	newDB := store.NewUltraOptimizedDB()
	newManager, err := NewManager(config, newDB)
	require.NoError(t, err)
	defer newManager.Close()

	// Load data
	require.NoError(t, newManager.LoadData())

	// Verify data was loaded
	value1, exists1 := newDB.Get("key1")
	require.True(t, exists1)
	require.Equal(t, "value1", value1)

	value2, exists2 := newDB.Get("key2")
	require.True(t, exists2)
	require.Equal(t, "value2", value2)
}

func TestPersistenceManagerManualSave(t *testing.T) {
	dir := t.TempDir()

	// Create store
	db := store.NewUltraOptimizedDB()

	// Create persistence manager
	config := &Config{
		Dir:        dir,
		AOFEnabled: false,
		RDBEnabled: true,
	}

	manager, err := NewManager(config, db)
	require.NoError(t, err)
	defer manager.Close()

	// Add some data
	db.Set("key1", "value1", time.Time{})
	db.Set("key2", "value2", time.Now().Add(time.Hour))

	// Manual save
	require.NoError(t, manager.SaveRDB())

	// Verify RDB file exists
	rdbPath := filepath.Join(dir, "dump.rdb")
	_, err = os.Stat(rdbPath)
	require.NoError(t, err)

	// Check stats
	stats := manager.Stats()
	require.Equal(t, 0, stats["changes_since"].(int))
	require.False(t, stats["last_save"].(time.Time).IsZero())
}

func TestPersistenceManagerDirectoryCreation(t *testing.T) {
	dir := t.TempDir()
	subDir := filepath.Join(dir, "nonexistent", "subdir")

	// Create store
	db := store.NewUltraOptimizedDB()

	// Create persistence manager with non-existent directory
	config := &Config{
		Dir:         subDir,
		AOFEnabled:  true,
		AOFSyncMode: aof.EverySec,
		RDBEnabled:  false,
	}

	manager, err := NewManager(config, db)
	require.NoError(t, err)
	defer manager.Close()

	// Verify directory was created
	_, err = os.Stat(subDir)
	require.NoError(t, err)
}

func TestManagerBGSaveAsync(t *testing.T) {
	t.Run("BGSaveAsync with RDB disabled", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: false,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		require.NoError(t, err)

		// Should not error when RDB is disabled
		err = manager.BGSaveAsync()
		require.NoError(t, err)
	})

	t.Run("BGSaveAsync with RDB enabled", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: true,
			AOFEnabled: false,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		require.NoError(t, err)

		// First call should succeed
		err = manager.BGSaveAsync()
		require.NoError(t, err)

		// Second call should fail (already running)
		err = manager.BGSaveAsync()
		require.Error(t, err)
		require.Contains(t, err.Error(), "background save already in progress")
	})
}

func TestManagerGenerateRDBData(t *testing.T) {
	t.Run("GenerateRDBData with empty database", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: true,
			AOFEnabled: false,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		data, err := manager.GenerateRDBData()
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Greater(t, len(data), 0)
	})

	t.Run("GenerateRDBData with data", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: true,
			AOFEnabled: false,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Add some test data
		db.Set("testkey", "testvalue", time.Time{})
		db.Set("testkey2", "testvalue2", time.Now().Add(time.Hour))

		data, err := manager.GenerateRDBData()
		require.NoError(t, err)
		require.NotNil(t, data)
		require.Greater(t, len(data), 0)
	})
}

func TestManagerClearData(t *testing.T) {
	t.Run("ClearData with AOF and RDB", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: true,
			AOFEnabled: true,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Add some data and trigger persistence
		db.Set("testkey", "testvalue", time.Time{})
		manager.AppendCommand("SET", []string{"testkey", "testvalue"})

		// Clear data
		require.NoError(t, manager.ClearData())
	})

	t.Run("ClearData with only RDB", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: true,
			AOFEnabled: false,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Add some data
		db.Set("testkey", "testvalue", time.Time{})

		// Clear data
		require.NoError(t, manager.ClearData())
	})

	t.Run("ClearData with only AOF", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: true,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Add some data and trigger AOF
		manager.AppendCommand("SET", []string{"testkey", "testvalue"})

		// Clear data
		require.NoError(t, manager.ClearData())
	})
}

func TestManagerFlushMultiCommand(t *testing.T) {
	t.Run("FlushMultiCommand with AOF disabled", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: false,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Should not error when AOF is disabled
		require.NoError(t, manager.FlushMultiCommand())
	})

	t.Run("FlushMultiCommand with empty batch", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: true,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Should not error with empty batch
		require.NoError(t, manager.FlushMultiCommand())
	})

	t.Run("FlushMultiCommand with commands in batch", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: true,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Add commands to batch
		manager.AppendMultiCommands("SET", []string{"key1", "value1"})
		manager.AppendMultiCommands("SET", []string{"key2", "value2"})

		// Flush the batch
		require.NoError(t, manager.FlushMultiCommand())
	})
}

func TestManagerLoadFromAOF(t *testing.T) {
	t.Run("LoadFromAOF with AOF disabled", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: false,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Should error when AOF is disabled
		mErr := manager.loadFromAOF()
		require.Error(t, mErr)
		require.Contains(t, mErr.Error(), "AOF not enabled")
	})

	t.Run("LoadFromAOF with AOF enabled but no file", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: true,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Should not error when AOF file doesn't exist
		require.NoError(t, manager.loadFromAOF())
	})
}

func TestManagerBackgroundRDBSave(t *testing.T) {
	t.Run("backgroundRDBSave basic test", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: true,
			AOFEnabled: false,
			RDBSaveConfig: &RDBSaveConfig{
				SaveInterval: 100 * time.Millisecond,
				MinChanges:   1,
			},
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Start background save
		go manager.backgroundRDBSave()

		// Add some changes
		db.Set("testkey", "testvalue", time.Time{})

		// Wait a bit for background save to potentially trigger
		time.Sleep(200 * time.Millisecond)

		// Stop the background save
		close(manager.stopChan)
	})
}

func TestManagerBackgroundAOFRewriteCheck(t *testing.T) {
	t.Run("backgroundAOFRewriteCheck basic test", func(t *testing.T) {
		config := Config{
			Dir:        t.TempDir(),
			RDBEnabled: false,
			AOFEnabled: true,
		}

		db := store.NewUltraOptimizedDB()
		manager, err := NewManager(&config, db)
		assert.NoError(t, err)

		// Start background AOF rewrite check
		go manager.backgroundAOFRewriteCheck()

		// Wait a bit for background check to potentially trigger
		time.Sleep(50 * time.Millisecond)

		// Stop the background check
		close(manager.stopChan)
	})
}
