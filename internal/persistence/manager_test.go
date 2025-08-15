package persistence

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gridhouse/internal/aof"
	"gridhouse/internal/store"

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
