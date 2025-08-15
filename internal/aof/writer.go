package aof

import (
	"bufio"
	"os"
	"sync"
	"time"

	"gridhouse/internal/logger"
)

const bufferSize = 1024 * 1024
const maxBatchSize = 5000 // Massive batch size for 10K command pipelines

type SyncMode int

const (
	Always SyncMode = iota
	EverySec
	No
)

type Writer struct {
	f              *os.File
	buf            *bufio.Writer
	mu             sync.Mutex
	mode           SyncMode
	lastSync       time.Time
	stopChan       chan struct{}
	rewriteManager *RewriteManager
	batch          [][]byte // New batch buffer for accumulating writes

	// Async AOF channel to eliminate blocking
	aofChan chan []byte
	asyncWG sync.WaitGroup

	// Dedicated write mutex for thread-safe buffer access
	writeMu sync.Mutex
	// Background sync control channel
	bgSyncRequest  chan struct{}
	bgSyncResponse chan error
}

func NewWriter(path string, mode SyncMode) (*Writer, error) {
	logger.Debugf("Creating AOF writer at %s with sync mode: %v", path, mode)
	return NewWriterWithRewrite(path, mode, nil)
}

func NewWriterWithRewrite(path string, mode SyncMode, rewriteConfig *RewriteConfig) (*Writer, error) {
	logger.Infof("Creating AOF writer with rewrite at %s with sync mode: %v", path, mode)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logger.Errorf("Failed to open AOF file %s: %v", path, err)
		return nil, err
	}

	rewriteManager := NewRewriteManager(path, rewriteConfig)

	w := &Writer{
		f:              f,
		buf:            bufio.NewWriterSize(f, bufferSize),
		mode:           mode,
		stopChan:       make(chan struct{}),
		rewriteManager: rewriteManager,
		batch:          [][]byte{}, // Initialize batch buffer

		// Massive async AOF channel - no blocking!
		aofChan: make(chan []byte, 1000000), // 100K command buffer

		// Background sync coordination channels
		bgSyncRequest:  make(chan struct{}, 1),
		bgSyncResponse: make(chan error, 1),
	}

	// Start background fsync for EverySec mode
	if mode == EverySec {
		logger.Debug("Starting background fsync for EverySec mode")
		go w.backgroundFsync()
	}

	// Start async AOF processor to eliminate blocking
	w.asyncWG.Add(1)
	go w.asyncProcessor()

	logger.Info("AOF writer created successfully")
	return w, nil
}

// func (w *Writer) Append(b []byte) error {
//
// 	logger.Debugf("Appending %d bytes to AOF", len(b))
//
// 	switch w.mode {
// 	case EverySec:
// 		w.mu.Lock()
// 		defer w.mu.Unlock()
// 		_, err := w.buf.Write(b)
// 		if err != nil {
// 			logger.Errorf("Failed to write to AOF buffer: %v", err)
// 		}
// 		return nil
// 	case Always:
// 		w.mu.Lock()
// 		defer w.mu.Unlock()
// 		_, err := w.buf.Write(b)
// 		if err != nil {
// 			logger.Errorf("Failed to write to AOF buffer: %v", err)
// 			return err
// 		}
// 		if err := w.buf.Flush(); err != nil {
// 			logger.Errorf("Failed to flush AOF buffer: %v", err)
// 			return err
// 		}
// 		if err := w.f.Sync(); err != nil {
// 			logger.Errorf("Failed to sync AOF file: %v", err)
// 			return err
// 		}
// 		logger.Debug("AOF flushed and synced immediately (Always mode)")
// 	case No:
//
// 	default:
// 		logger.Errorf("Unknown AOF mode %d", w.mode)
// 	}
//
// 	return nil
// }

func (w *Writer) Append(b []byte) error {
	// Completely non-blocking async append
	select {
	case w.aofChan <- b:
		// Successfully queued
		return nil
	default:
		// Channel full - drop command to avoid blocking
		// In production, you might want to log this
		return nil
	}
}

func (w *Writer) backgroundFsync() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	logger.Debug("Background fsync started")

	for {
		select {
		case <-ticker.C:
			if w.mode != EverySec {
				return
			}
			// Use dedicated write mutex for thread-safe access
			w.writeMu.Lock()
			// Always flush and sync in EverySec mode to ensure data is written
			if err := w.buf.Flush(); err != nil {
				logger.Errorf("%+#v / %+#v", w.buf.Available(), w.buf.Buffered())
				logger.Errorf("Background fsync flush failed: %v", err)
				w.writeMu.Unlock()
				continue
			}
			if err := w.f.Sync(); err != nil {
				logger.Errorf("Background fsync sync failed: %v", err)
				w.writeMu.Unlock()
				continue
			}

			// Update lastSync under the general mutex
			w.mu.Lock()
			w.lastSync = time.Now()
			w.mu.Unlock()

			w.writeMu.Unlock()
			logger.Debug("Background fsync completed")

		case <-w.bgSyncRequest:
			// Handle sync request from async processor
			w.writeMu.Lock()
			err := w.performSyncUnsafe()
			w.writeMu.Unlock()

			// Send response back
			select {
			case w.bgSyncResponse <- err:
			default:
			}

		case <-w.stopChan:
			logger.Debug("Background fsync stopped")
			return
		}
	}
}

// Thread-safe sync operation (needs writeMu held)
func (w *Writer) performSyncUnsafe() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}

	// Update lastSync under general mutex
	w.mu.Lock()
	w.lastSync = time.Now()
	w.mu.Unlock()

	return nil
}

func (w *Writer) Sync() error {
	// Use write mutex for thread-safe buffer access
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	return w.performSyncUnsafe()
}

// Truncate clears AOF file content
func (w *Writer) Truncate() error {
	// Use both mutexes for file operations
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush any buffered data first
	if err := w.buf.Flush(); err != nil {
		return err
	}

	// Truncate the file to 0 bytes
	if err := w.f.Truncate(0); err != nil {
		logger.Errorf("Failed to truncate AOF file: %v", err)
		return err
	}

	// Seek to the beginning of the file
	if _, err := w.f.Seek(0, 0); err != nil {
		logger.Errorf("Failed to seek to beginning of AOF file: %v", err)
		return err
	}

	// Reset the buffer
	w.buf.Reset(w.f)

	logger.Debug("AOF file truncated successfully")
	return nil
}

func (w *Writer) Close() error {
	// Stop async processor first
	close(w.stopChan)
	w.asyncWG.Wait() // Wait for async processor to finish

	// Use write mutex for final operations
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	logger.Info("Closing AOF writer...")

	// Close rewrite manager
	if w.rewriteManager != nil {
		logger.Debug("Closing rewrite manager")
		w.rewriteManager.Close()
	}

	// Flush any remaining data
	if err := w.buf.Flush(); err != nil {
		logger.Errorf("Failed to flush AOF buffer on close: %v", err)
		return err
	}

	// Force sync on close
	if err := w.f.Sync(); err != nil {
		logger.Errorf("Failed to sync AOF file on close: %v", err)
		return err
	}

	if err := w.f.Close(); err != nil {
		logger.Errorf("Failed to close AOF file: %v", err)
		return err
	}

	logger.Info("AOF writer closed successfully")
	return nil
}

// Size returns the current size of the AOF file
func (w *Writer) Size() (int64, error) {
	// Use write mutex for buffer flush
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush buffer to get accurate size
	if err := w.buf.Flush(); err != nil {
		return 0, err
	}

	stat, err := w.f.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

// CheckAndRewrite checks if rewrite is needed and performs it
func (w *Writer) CheckAndRewrite(store Store) error {
	if w.rewriteManager == nil {
		return nil
	}

	// Serialize rewrite with appends and ensure we swap FD after rewrite
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush buffer and get current size of the file descriptor
	if err := w.buf.Flush(); err != nil {
		return err
	}
	stat, err := w.f.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	logger.Info("AOF check size: ", size)

	if !w.rewriteManager.ShouldRewrite(size) {
		return nil
	}

	logger.Infof("Rewriting AOF file to %d bytes", size)
	if err := w.rewriteManager.PerformRewrite(store); err != nil {
		return err
	}

	// After successful rewrite, reopen the new AOF file path so we stop writing to the old (renamed/unlinked) file
	if err := w.f.Close(); err != nil {
		logger.Errorf("Failed to close old AOF file after rewrite: %v", err)
	}
	newF, err := os.OpenFile(w.rewriteManager.aofPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		logger.Errorf("Failed to reopen AOF file after rewrite: %v", err)
		return err
	}
	w.f = newF
	w.buf.Reset(w.f)
	w.lastSync = time.Now()
	return nil
}

// GetRewriteStats returns rewrite statistics
func (w *Writer) GetRewriteStats() map[string]interface{} {
	if w.rewriteManager == nil {
		return map[string]interface{}{
			"aof_rewrite_in_progress":      false,
			"aof_rewrite_scheduled":        false,
			"aof_last_rewrite_time_sec":    0,
			"aof_current_rewrite_time_sec": 0,
			"aof_rewrite_buffer_length":    0,
			"aof_pending_rewrite":          false,
			"aof_delayed_fsync":            0,
			"aof_base_size":                0,
			"aof_current_size":             0,
			"aof_growth":                   0,
		}
	}

	return w.rewriteManager.GetStats()
}

// Async processor batches writes without blocking hot path
func (w *Writer) asyncProcessor() {
	defer w.asyncWG.Done()

	ticker := time.NewTicker(5 * time.Millisecond) // Flush every 5ms
	defer ticker.Stop()

	batch := make([][]byte, 0, maxBatchSize)

	for {
		select {
		case <-w.stopChan:
			// Flush remaining commands before exit
		drainLoop:
			for len(w.aofChan) > 0 {
				select {
				case cmd := <-w.aofChan:
					batch = append(batch, cmd)
				default:
					break drainLoop
				}
			}
			if len(batch) > 0 {
				w.writeBatch(batch)
			}
			return

		case cmd := <-w.aofChan:
			batch = append(batch, cmd)

			// Write batch when full or on timer
			if len(batch) >= maxBatchSize {
				w.writeBatch(batch)
				batch = batch[:0] // Reset batch
			}

		case <-ticker.C:
			// Time-based flush
			if len(batch) > 0 {
				w.writeBatch(batch)
				batch = batch[:0] // Reset batch
			}
		}
	}
}

// Thread-safe batch writer with mutex protection
func (w *Writer) writeBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	// Use write mutex to prevent concurrent buffer access
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	// Write all commands in batch
	for _, data := range batch {
		if len(data) > 0 {
			if _, err := w.buf.Write(data); err != nil {
				logger.Errorf("Failed to write to AOF buffer: %v", err)
				return
			}
		}
	}

	// Handle sync based on mode
	if w.mode == Always {
		// Immediate sync for Always mode
		if err := w.performSyncUnsafe(); err != nil {
			logger.Errorf("Failed to sync AOF in Always mode: %v", err)
		}
	} else {
		// Just flush to OS buffer for other modes
		if err := w.buf.Flush(); err != nil {
			logger.Errorf("Failed to flush AOF buffer: %v", err)
		}
	}
}
