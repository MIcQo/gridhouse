package persistence

import (
	"fmt"
	"gridhouse/internal/aof"
	"gridhouse/internal/logger"
	rdb "gridhouse/internal/rdb/v2"
	"gridhouse/internal/store"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Dir              string
	AOFEnabled       bool
	AOFSyncMode      aof.SyncMode
	AOFRewriteConfig *aof.RewriteConfig
	RDBEnabled       bool
	RDBSaveConfig    *RDBSaveConfig
}

type RDBSaveConfig struct {
	SaveInterval time.Duration
	MinChanges   int
}

type cmd struct {
	cmd  string
	args []string
}

type Manager struct {
	config *Config
	db     store.DataStore
	aof    *aof.Writer
	mu     sync.RWMutex

	// RDB state
	lastSave      time.Time
	changesSince  int64
	stopChan      chan struct{}
	bgSaveRunning int32 // 0 = idle, 1 = running

	// Pipeline command batching for AOF
	commandBatch []*cmd
	batchMu      sync.Mutex
}

func NewManager(config *Config, db store.DataStore) (*Manager, error) {
	logger.Infof("Initializing persistence manager with directory: %s", config.Dir)

	m := &Manager{
		config:       config,
		db:           db,
		stopChan:     make(chan struct{}),
		commandBatch: make([]*cmd, 0),
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		logger.Errorf("Failed to create persistence directory %s: %v", config.Dir, err)
		return nil, fmt.Errorf("failed to create persistence directory: %w", err)
	}

	// Initialize AOF if enabled
	if config.AOFEnabled {
		aofPath := filepath.Join(config.Dir, "appendonly.aof")
		logger.Infof("Initializing AOF writer at %s", aofPath)
		aofWriter, err := aof.NewWriterWithRewrite(aofPath, config.AOFSyncMode, config.AOFRewriteConfig)
		if err != nil {
			logger.Errorf("Failed to create AOF writer at %s: %v", aofPath, err)
			return nil, fmt.Errorf("failed to create AOF writer: %w", err)
		}
		m.aof = aofWriter
		logger.Info("AOF writer initialized successfully")
	}

	// Start background RDB save if enabled
	if config.RDBEnabled && config.RDBSaveConfig != nil {
		logger.Info("Starting background RDB save goroutine")
		go m.backgroundRDBSave()
	}

	// Start background AOF rewrite check if AOF is enabled
	if config.AOFEnabled {
		logger.Info("Starting background AOF rewrite check goroutine")
		go m.backgroundAOFRewriteCheck()
	}

	return m, nil
}

func (m *Manager) LoadData() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.Info("Loading data from persistence files...")

	// Try to load from RDB first (if enabled)
	if m.config.RDBEnabled {
		rdbPath := filepath.Join(m.config.Dir, "dump.rdb")
		logger.Infof("Attempting to load from RDB file: %s", rdbPath)
		if err := m.loadFromRDB(rdbPath); err == nil {
			logger.Info("Successfully loaded data from RDB file")
			return nil // Successfully loaded from RDB
		} else {
			// Do not exit; warn and continue to AOF if enabled
			logger.Warnf("Failed to load from RDB file: %v", err)
		}
		// If RDB load fails, continue to AOF
	}

	// Try to load from AOF (if enabled)
	if m.config.AOFEnabled {
		logger.Info("Attempting to load from AOF file")
		if err := m.loadFromAOF(); err != nil {
			logger.Fatalf("Failed to load from AOF file: %v", err)
			return err
		}
		logger.Info("Successfully loaded data from AOF file")
		return nil
	}

	logger.Info("No persistence files to load")
	return nil // No persistence files to load
}

func (m *Manager) loadFromRDB(path string) error {
	reader, err := rdb.NewReader(path)
	if err != nil {
		logger.Warn(err)
		return err
	}
	defer func() {
		_ = reader.Close()
	}()

	return reader.ReadAll(m.db)
}

func (m *Manager) loadFromAOF() error {
	if m.aof == nil {
		return fmt.Errorf("AOF not enabled")
	}

	// Get AOF file path from the writer
	aofPath := filepath.Join(m.config.Dir, "appendonly.aof")

	// Check if AOF file exists
	if _, err := os.Stat(aofPath); os.IsNotExist(err) {
		// AOF file doesn't exist, which is fine for a new server
		return nil
	}

	loader, err := aof.NewLoader(aofPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = loader.Close()
	}()

	// Replay commands
	return loader.Replay(func(cmd aof.Command) error {
		// Convert AOF command to store operation (case-insensitive)
		switch strings.ToUpper(cmd.Name) {
		case "SET":
			if len(cmd.Args) >= 2 {
				// For simplicity, we'll set without expiration
				// In a full implementation, you'd parse EX/PX options
				m.db.Set(cmd.Args[0], cmd.Args[1], time.Time{})
			}
		case "DEL":
			if len(cmd.Args) >= 1 {
				m.db.Del(cmd.Args[0])
			}
		}
		return nil
	})
}

func (m *Manager) AppendMultiCommands(cmdName string, args []string) error {
	if !m.config.AOFEnabled {
		return nil
	}

	// Accumulate command in batch instead of encoding immediately
	m.batchMu.Lock()
	defer m.batchMu.Unlock()

	m.commandBatch = append(m.commandBatch, &cmd{
		cmd:  cmdName,
		args: args,
	})

	return nil
}

func (m *Manager) AppendCommand(cmd string, args []string) error {
	switch {

	case m.config.AOFEnabled && m.config.RDBEnabled:
		atomic.AddInt64(&m.changesSince, 1)
		fallthrough
	case m.config.AOFEnabled:
		var n = EncodeRESPArrayFast(cmd, args)
		if err := m.aof.Append(n); err != nil {
			return err
		}
		return nil

	case m.config.RDBEnabled:
		atomic.AddInt64(&m.changesSince, 1)
		return nil
	default:
		return nil
	}
}

func (m *Manager) FlushMultiCommand() error {
	if !m.config.AOFEnabled {
		return nil
	}

	m.batchMu.Lock()
	defer m.batchMu.Unlock()

	// If no commands to flush, return early
	if len(m.commandBatch) == 0 {
		return nil
	}

	// Encode all commands in batch at once - much more efficient
	var batchData []byte
	for _, command := range m.commandBatch {
		cmdData := EncodeRESPArrayFast(command.cmd, command.args)
		batchData = append(batchData, cmdData...)
	}

	// Single AOF append for entire batch
	if len(batchData) > 0 {
		if err := m.aof.Append(batchData); err != nil {
			return err
		}
	}

	// Clear the batch after successful flush
	m.commandBatch = m.commandBatch[:0]

	return nil
}

// ClearData clears all persistence files
func (m *Manager) ClearData() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear AOF file if enabled
	if m.aof != nil {
		if err := m.aof.Truncate(); err != nil {
			logger.Errorf("Failed to truncate AOF file: %v", err)
			return fmt.Errorf("failed to truncate AOF: %w", err)
		}
		logger.Info("AOF file truncated due to FLUSHDB")
	}

	// Clear RDB file if it exists
	rdbPath := filepath.Join(m.config.Dir, "dump.rdb")
	if _, err := os.Stat(rdbPath); err == nil {
		if err := os.Remove(rdbPath); err != nil {
			logger.Errorf("Failed to remove RDB file: %v", err)
			return fmt.Errorf("failed to remove RDB file: %w", err)
		}
		logger.Info("RDB file removed due to FLUSHDB")
	}

	// Reset change tracking
	atomic.StoreInt64(&m.changesSince, 0)
	m.lastSave = time.Now()

	return nil
}

func (m *Manager) backgroundRDBSave() {
	ticker := time.NewTicker(m.config.RDBSaveConfig.SaveInterval)
	defer ticker.Stop()

	logger.Infof("Background RDB save started with interval: %v", m.config.RDBSaveConfig.SaveInterval)

	for {
		select {
		case <-ticker.C:
			changes := atomic.LoadInt64(&m.changesSince)
			if changes >= int64(m.config.RDBSaveConfig.MinChanges) {
				logger.Debugf("Triggering background RDB save with %d changes", changes)
				// Call SaveRDB which will handle its own locking
				if err := m.SaveRDB(); err != nil {
					// Log error but continue
					logger.Errorf("Background RDB save failed: %v", err)
				} else {
					logger.Info("Background RDB save completed successfully")
				}
			}
		case <-m.stopChan:
			logger.Info("Background RDB save stopped")
			return
		}
	}
}

func (m *Manager) backgroundAOFRewriteCheck() {
	ticker := time.NewTicker(10 * time.Second) // Check every 10 seconds
	defer ticker.Stop()

	logger.Info("Background AOF rewrite check started")

	for {
		select {
		case <-ticker.C:
			if m.aof != nil {
				logger.Infof("Background AOF rewrite check started")
				if err := m.aof.CheckAndRewrite(m.db); err != nil {
					// Log error but continue
					logger.Errorf("Background AOF rewrite failed: %v", err)
				}
			}
		case <-m.stopChan:
			logger.Info("Background AOF rewrite check stopped")
			return
		}
	}
}

func (m *Manager) saveRDB() error {
	// Don't lock here since this is called from methods that already hold the lock
	rdbPath := filepath.Join(m.config.Dir, "dump.rdb")
	logger.Infof("Saving RDB to %s", rdbPath)

	writer, err := rdb.NewWriter(rdbPath)
	if err != nil {
		logger.Errorf("Failed to create RDB writer: %v", err)
		return err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			logger.Errorf("Failed to close RDB writer: %v", err)
		}
	}()

	// Get all keys and write them
	keys := m.db.Keys()
	logger.Debugf("Writing %d keys to RDB", len(keys))

	var ttlCount = 0
	for _, key := range keys {
		ttl := m.db.TTL(key)
		if ttl > 0 {
			ttlCount++
		}
	}

	// Write header
	if err := writer.WriteHeader(uint64(len(keys)), uint64(ttlCount)); err != nil {
		logger.Errorf("Failed to write RDB header: %v", err)
		return err
	}

	for _, key := range keys {
		// Get expiration
		ttl := m.db.TTL(key)
		var expiration time.Time
		if ttl > 0 {
			expiration = time.Now().Add(time.Duration(ttl) * time.Second)
		}

		// Persist all supported data structures
		v := m.db.GetDataType(key)
		switch v {
		case store.TypeString:
			if value, exists := m.db.Get(key); exists {
				if err := writer.WriteString(key, value, expiration); err != nil {
					logger.Errorf("Failed to write string key '%s' to RDB: %v", key, err)
					return err
				}
			}
		case store.TypeList:
			lst := m.db.GetOrCreateList(key)
			vals := lst.LRange(0, -1)
			if err := writer.WriteList(key, vals, expiration); err != nil {
				logger.Errorf("Failed to write list key '%s' to RDB: %v", key, err)
				return err
			}
		case store.TypeSet:
			set := m.db.GetOrCreateSet(key)
			members := set.SMembers()
			if err := writer.WriteSet(key, members, expiration); err != nil {
				logger.Errorf("Failed to write set key '%s' to RDB: %v", key, err)
				return err
			}
		case store.TypeHash:
			h := m.db.GetOrCreateHash(key)
			fields := h.HGetAll()
			if err := writer.WriteHash(key, fields, expiration); err != nil {
				logger.Errorf("Failed to write hash key '%s' to RDB: %v", key, err)
				return err
			}
		case store.TypeSortedSet:
			z := m.db.GetOrCreateSortedSet(key)
			pairs := make(map[string]float64)
			arr := z.ZRange(0, -1, true)
			for i := 0; i+1 < len(arr); i += 2 {
				if s, err := strconv.ParseFloat(arr[i+1], 64); err == nil {
					pairs[arr[i]] = s
				}
			}
			if err := writer.WriteZSet(key, pairs, expiration); err != nil {
				logger.Errorf("Failed to write zset key '%s' to RDB: %v", key, err)
				return err
			}
		case store.TypeStream:
			st := m.db.GetOrCreateStream(key)
			entries := st.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
			wEntries := make([]store.StreamEntry, len(entries))
			for i, e := range entries {
				wEntries[i] = store.StreamEntry{ID: e.ID, Fields: e.Fields}
			}
			if err := writer.WriteStream(key, wEntries, expiration); err != nil {
				logger.Errorf("Failed to write stream key '%s' to RDB: %v", key, err)
				return err
			}
		default:
			fmt.Println("unsupported")
			// Unknown/unsupported type – skip
		}
	}

	// Write EOF
	if err := writer.WriteEOF(); err != nil {
		logger.Errorf("Failed to write RDB EOF: %v", err)
		return err
	}

	// Reset change counter
	atomic.StoreInt64(&m.changesSince, 0)
	m.lastSave = time.Now()

	logger.Infof("RDB save completed successfully with %d keys", len(keys))

	return nil
}

// GetConfig returns persistence configuration
func (m *Manager) GetConfig() *Config {
	return m.config
}

func (m *Manager) SaveRDB() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	logger.Info("Starting RDB save...")
	return m.saveRDB()
}

// GenerateRDBData generates RDB data in memory for replication
func (m *Manager) GenerateRDBData() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a temporary file to generate RDB data
	tempFile, err := os.CreateTemp("", "gridhouse-rdb-repl-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp RDB file: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Use the same logic as saveRDB but write to temp file
	writer, err := rdb.NewWriter(tempFile.Name())
	if err != nil {
		logger.Errorf("Failed to create RDB writer: %v", err)
		return nil, err
	}
	defer func() {
		if err := writer.Close(); err != nil {
			logger.Errorf("Failed to close RDB writer: %v", err)
		}
	}()

	// Get all keys and write them
	keys := m.db.Keys()
	logger.Debugf("Writing %d keys to RDB for replication", len(keys))

	var ttlCount = 0
	for _, key := range keys {
		ttl := m.db.TTL(key)
		if ttl > 0 {
			ttlCount++
		}
	}

	// Write header
	if err := writer.WriteHeader(uint64(len(keys)), uint64(ttlCount)); err != nil {
		logger.Errorf("Failed to write RDB header: %v", err)
		return nil, err
	}

	for _, key := range keys {
		// Get expiration
		ttl := m.db.TTL(key)
		var expiration time.Time
		if ttl > 0 {
			expiration = time.Now().Add(time.Duration(ttl) * time.Second)
		}

		// Persist all supported data structures
		v := m.db.GetDataType(key)
		switch v {
		case store.TypeString:
			if value, exists := m.db.Get(key); exists {
				if err := writer.WriteString(key, value, expiration); err != nil {
					logger.Errorf("Failed to write string key '%s' to RDB: %v", key, err)
					return nil, err
				}
			}
		case store.TypeList:
			lst := m.db.GetOrCreateList(key)
			vals := lst.LRange(0, -1)
			if err := writer.WriteList(key, vals, expiration); err != nil {
				logger.Errorf("Failed to write list key '%s' to RDB: %v", key, err)
				return nil, err
			}
		case store.TypeSet:
			set := m.db.GetOrCreateSet(key)
			members := set.SMembers()
			if err := writer.WriteSet(key, members, expiration); err != nil {
				logger.Errorf("Failed to write set key '%s' to RDB: %v", key, err)
				return nil, err
			}
		case store.TypeHash:
			h := m.db.GetOrCreateHash(key)
			fields := h.HGetAll()
			if err := writer.WriteHash(key, fields, expiration); err != nil {
				logger.Errorf("Failed to write hash key '%s' to RDB: %v", key, err)
				return nil, err
			}
		case store.TypeSortedSet:
			z := m.db.GetOrCreateSortedSet(key)
			pairs := make(map[string]float64)
			arr := z.ZRange(0, -1, true)
			for i := 0; i+1 < len(arr); i += 2 {
				if s, err := strconv.ParseFloat(arr[i+1], 64); err == nil {
					pairs[arr[i]] = s
				}
			}
			if err := writer.WriteZSet(key, pairs, expiration); err != nil {
				logger.Errorf("Failed to write zset key '%s' to RDB: %v", key, err)
				return nil, err
			}
		case store.TypeStream:
			st := m.db.GetOrCreateStream(key)
			entries := st.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
			wEntries := make([]store.StreamEntry, len(entries))
			for i, e := range entries {
				wEntries[i] = store.StreamEntry{ID: e.ID, Fields: e.Fields}
			}
			if err := writer.WriteStream(key, wEntries, expiration); err != nil {
				logger.Errorf("Failed to write stream key '%s' to RDB: %v", key, err)
				return nil, err
			}
		default:
			logger.Warnf("Unknown data type for key '%s': %v", key, v)
		}
	}

	if err := writer.WriteEOF(); err != nil {
		logger.Errorf("Failed to write RDB EOF: %v", err)
		return nil, err
	}

	// Read the generated RDB data
	if err := tempFile.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync temp file: %w", err)
	}

	rdbData, err := os.ReadFile(tempFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read RDB data: %w", err)
	}

	logger.Infof("Generated RDB data of size: %d bytes for replication", len(rdbData))
	return rdbData, nil
}

// BGSaveAsync starts an asynchronous background RDB save.
// Returns error if a background save is already in progress.
func (m *Manager) BGSaveAsync() error {
	if !m.config.RDBEnabled {
		// Even if RDB isn't configured for periodic saves, allow on-demand BGSAVE by proceeding.
		// This matches the spirit of Redis which can BGSAVE regardless of auto-save config.
		return nil
	}
	if !atomic.CompareAndSwapInt32(&m.bgSaveRunning, 0, 1) {
		return fmt.Errorf("background save already in progress")
	}
	go func() {
		defer atomic.StoreInt32(&m.bgSaveRunning, 0)
		m.mu.Lock()
		_ = m.saveRDB()
		m.mu.Unlock()
	}()
	return nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.Info("Closing persistence manager...")

	// Stop background goroutines (RDB save or AOF rewrite checks)
	logger.Debug("Stopping background goroutines")
	close(m.stopChan)

	// Save final RDB snapshot
	if m.config.RDBEnabled {
		logger.Info("Saving final RDB snapshot")
		if err := m.saveRDB(); err != nil {
			logger.Errorf("Failed to save final RDB: %v", err)
			return fmt.Errorf("failed to save final RDB: %w", err)
		}
		logger.Info("Final RDB snapshot saved successfully")
	}

	// Close AOF
	if m.aof != nil {
		logger.Debug("Closing AOF writer")
		if err := m.aof.Close(); err != nil {
			logger.Errorf("Failed to close AOF: %v", err)
			return fmt.Errorf("failed to close AOF: %w", err)
		}
		logger.Info("AOF writer closed successfully")
	}

	logger.Info("Persistence manager closed successfully")
	return nil
}

func (m *Manager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"aof_enabled":   m.config.AOFEnabled,
		"rdb_enabled":   m.config.RDBEnabled,
		"changes_since": int(atomic.LoadInt64(&m.changesSince)),
		"last_save":     m.lastSave,
	}

	if m.aof != nil {
		if size, err := m.aof.Size(); err == nil {
			stats["aof_size"] = size
		}

		// Add AOF rewrite stats
		rewriteStats := m.aof.GetRewriteStats()
		for k, v := range rewriteStats {
			stats[k] = v
		}
	}

	return stats
}
