package aof

import (
	"fmt"
	"os"
	"sync"
	"time"

	"gridhouse/internal/logger"
)

// RewriteConfig holds configuration for AOF rewrite
type RewriteConfig struct {
	Enabled           bool
	GrowthThreshold   int64 // Default: 64MB (64 * 1024 * 1024)
	MinRewriteSize    int64 // Default: 32MB (32 * 1024 * 1024)
	RewritePercentage int   // Default: 100 (rewrite when growth >= 100% of original size)
}

// DefaultRewriteConfig returns default AOF rewrite configuration
func DefaultRewriteConfig() *RewriteConfig {
	return &RewriteConfig{
		Enabled:           true,
		GrowthThreshold:   64 * 1024 * 1024, // 64MB
		MinRewriteSize:    32 * 1024 * 1024, // 32MB
		RewritePercentage: 100,
	}
}

// RewriteManager manages AOF rewrite operations
type RewriteManager struct {
	config      *RewriteConfig
	aofPath     string
	mu          sync.RWMutex
	baseSize    int64 // Size when rewrite was last performed
	lastRewrite time.Time
	stopChan    chan struct{}
}

// NewRewriteManager creates a new AOF rewrite manager
func NewRewriteManager(aofPath string, config *RewriteConfig) *RewriteManager {
	if config == nil {
		config = DefaultRewriteConfig()
	}

	logger.Infof("Creating AOF rewrite manager for %s", aofPath)
	logger.Debugf("Rewrite config: enabled=%v, growth_threshold=%d, min_size=%d, percentage=%d",
		config.Enabled, config.GrowthThreshold, config.MinRewriteSize, config.RewritePercentage)

	rm := &RewriteManager{
		config:   config,
		aofPath:  aofPath,
		stopChan: make(chan struct{}),
	}

	logger.Infof("AOF rewrite manager created with base size: %d bytes", rm.baseSize)
	return rm
}

// ShouldRewrite checks if AOF rewrite should be triggered
func (rm *RewriteManager) ShouldRewrite(currentSize int64) bool {
	// Write lock because we may initialize/mutate baseSize
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.config.Enabled {
		return false
	}

	// Initialize base size on first check to establish a baseline
	if rm.baseSize == 0 {
		rm.baseSize = currentSize
		return false
	}

	logger.Infof("Current size: %d, minRewrite: %d, growthThreshold: %d, baseSize: %d", currentSize, rm.config.MinRewriteSize, rm.config.GrowthThreshold, rm.baseSize)

	// Check minimum size requirement
	if currentSize < rm.config.MinRewriteSize {
		return false
	}

	// Compute growth; if not positive, no rewrite
	growth := currentSize - rm.baseSize
	if growth <= 0 {
		return false
	}

	// Check absolute growth threshold
	if growth >= rm.config.GrowthThreshold {
		return true
	}

	// Check percentage growth (baseSize > 0 guaranteed)
	growthPercentage := int((growth * 100) / rm.baseSize)
	return growthPercentage >= rm.config.RewritePercentage
}

// PerformRewrite performs the AOF rewrite operation
func (rm *RewriteManager) PerformRewrite(store Store) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	logger.Info("Starting AOF rewrite...")

	// Create temporary file for rewrite
	tempPath := rm.aofPath + ".rewrite"
	logger.Debugf("Creating temporary rewrite file: %s", tempPath)

	tempFile, err := os.Create(tempPath)
	if err != nil {
		logger.Errorf("Failed to create rewrite file %s: %v", tempPath, err)
		return fmt.Errorf("failed to create rewrite file: %w", err)
	}
	defer func() {
		_ = tempFile.Close()
	}()

	// Write all current data to the rewrite file
	logger.Debug("Writing current data to rewrite file")
	if err := rm.writeCurrentData(tempFile, store); err != nil {
		logger.Errorf("Failed to write data during rewrite: %v", err)
		_ = os.Remove(tempPath) // Clean up temp file, ignore error
		return fmt.Errorf("failed to write data during rewrite: %w", err)
	}

	// Sync the temp file
	if err := tempFile.Sync(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to sync rewrite file: %w", err)
	}

	// Close temp file
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to close rewrite file: %w", err)
	}

	// Backup original file
	backupPath := rm.aofPath + ".backup"
	if err := os.Rename(rm.aofPath, backupPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to backup original AOF: %w", err)
	}

	// Replace original with rewritten file
	if err := os.Rename(tempPath, rm.aofPath); err != nil {
		// Restore backup
		_ = os.Rename(backupPath, rm.aofPath)
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to replace AOF with rewritten file: %w", err)
	}

	// Update base size and timestamp
	rm.baseSize = rm.getFileSize(rm.aofPath)
	rm.lastRewrite = time.Now()

	logger.Infof("AOF rewrite completed successfully. New base size: %d bytes", rm.baseSize)

	// Remove backup after successful rewrite
	_ = os.Remove(backupPath)

	return nil
}

// writeCurrentData writes all current data to the rewrite file
func (rm *RewriteManager) writeCurrentData(file *os.File, store Store) error {
	// Get all keys from the store
	keys := store.Keys()

	// Write each key-value pair
	for _, key := range keys {
		value, exists := store.Get(key)
		if !exists {
			continue
		}

		// Get TTL
		ttl := store.TTL(key)
		var expiration time.Time
		if ttl > 0 {
			expiration = time.Now().Add(time.Duration(ttl) * time.Second)
		}

		// Write SET command
		cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
			len(key), key, len(value), value)

		if _, err := file.WriteString(cmd); err != nil {
			return err
		}

		// Write EXPIRE command if key has expiration
		if !expiration.IsZero() {
			expireCmd := fmt.Sprintf("*3\r\n$8\r\nEXPIRE\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n",
				len(key), key, len(fmt.Sprintf("%d", int(ttl))), int(ttl))

			if _, err := file.WriteString(expireCmd); err != nil {
				return err
			}
		}
	}

	return nil
}

// getFileSize gets the size of a file
func (rm *RewriteManager) getFileSize(path string) int64 {
	stat, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return stat.Size()
}

// GetStats returns rewrite statistics
func (rm *RewriteManager) GetStats() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	currentSize := rm.getFileSize(rm.aofPath)
	growth := int64(0)
	if rm.baseSize > 0 {
		growth = currentSize - rm.baseSize
	}

	lastRewriteTime := int64(0)
	if !rm.lastRewrite.IsZero() {
		lastRewriteTime = rm.lastRewrite.Unix()
	}

	return map[string]interface{}{
		"aof_rewrite_in_progress":      false, // implement in-progress tracking
		"aof_rewrite_scheduled":        false, // implement scheduled tracking
		"aof_last_rewrite_time_sec":    lastRewriteTime,
		"aof_current_rewrite_time_sec": 0,     // implement current rewrite tracking
		"aof_rewrite_buffer_length":    0,     // implement buffer tracking
		"aof_pending_rewrite":          false, // implement pending tracking
		"aof_delayed_fsync":            0,     // implement delayed fsync tracking
		"aof_base_size":                rm.baseSize,
		"aof_current_size":             currentSize,
		"aof_growth":                   growth,
	}
}

// Close stops the rewrite manager
func (rm *RewriteManager) Close() {
	close(rm.stopChan)
}

// Store interface for accessing data during rewrite
type Store interface {
	Keys() []string
	Get(key string) (string, bool)
	TTL(key string) int64
}
