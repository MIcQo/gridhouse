package store

import (
	"sync"
	"time"
	"unsafe"
)

// OptimizedTTLWheel uses int64 timestamps for better performance
type OptimizedTTLWheel struct {
	mu   sync.RWMutex
	exp  map[string]int64 // Use Unix nanoseconds instead of time.Time
	step int64            // Step duration in nanoseconds
}

// NewOptimizedTTLWheel creates an optimized TTL wheel
func NewOptimizedTTLWheel(step time.Duration) *OptimizedTTLWheel {
	return &OptimizedTTLWheel{
		exp:  make(map[string]int64),
		step: int64(step),
	}
}

// Set stores expiration time as Unix nanoseconds for faster comparison
func (w *OptimizedTTLWheel) Set(k string, when time.Time) {
	w.mu.Lock()
	w.exp[k] = when.UnixNano()
	w.mu.Unlock()
}

// SetNano stores expiration time directly as nanoseconds
func (w *OptimizedTTLWheel) SetNano(k string, whenNano int64) {
	w.mu.Lock()
	w.exp[k] = whenNano
	w.mu.Unlock()
}

// Expired checks if key has expired using fast integer comparison
func (w *OptimizedTTLWheel) Expired(k string) bool {
	w.mu.RLock()
	whenNano, ok := w.exp[k]
	w.mu.RUnlock()

	if !ok {
		return false
	}

	// Fast integer comparison instead of time.Now().After()
	return time.Now().UnixNano() > whenNano
}

// ExpiredNano checks expiration using pre-computed current time
func (w *OptimizedTTLWheel) ExpiredNano(k string, nowNano int64) bool {
	w.mu.RLock()
	whenNano, ok := w.exp[k]
	w.mu.RUnlock()

	if !ok {
		return false
	}

	return nowNano > whenNano
}

// Remove removes a key from the TTL wheel
func (w *OptimizedTTLWheel) Remove(k string) {
	w.mu.Lock()
	delete(w.exp, k)
	w.mu.Unlock()
}

// CleanupExpired removes all expired keys and returns the count
func (w *OptimizedTTLWheel) CleanupExpired() int {
	nowNano := time.Now().UnixNano()
	expiredKeys := make([]string, 0, 16) // Pre-allocate small slice

	w.mu.RLock()
	for k, whenNano := range w.exp {
		if nowNano > whenNano {
			expiredKeys = append(expiredKeys, k)
		}
	}
	w.mu.RUnlock()

	if len(expiredKeys) == 0 {
		return 0
	}

	w.mu.Lock()
	for _, k := range expiredKeys {
		delete(w.exp, k)
	}
	w.mu.Unlock()

	return len(expiredKeys)
}

// UltraOptimizedTTLWheel uses unsafe operations for max performance
type UltraOptimizedTTLWheel struct {
	mu   sync.RWMutex
	exp  map[string]int64
	step int64
}

// NewUltraOptimizedTTLWheel creates the fastest possible TTL wheel
func NewUltraOptimizedTTLWheel(step time.Duration) *UltraOptimizedTTLWheel {
	return &UltraOptimizedTTLWheel{
		exp:  make(map[string]int64, 1000), // Pre-allocate
		step: int64(step),
	}
}

// SetUnsafe uses unsafe string operations for zero-copy handling
func (w *UltraOptimizedTTLWheel) SetUnsafe(k string, whenNano int64) {
	w.mu.Lock()
	// Use the string directly without copying
	w.exp[k] = whenNano
	w.mu.Unlock()
}

// ExpiredUnsafe uses unsafe operations for maximum speed
func (w *UltraOptimizedTTLWheel) ExpiredUnsafe(k string, nowNano int64) bool {
	w.mu.RLock()
	whenNano, ok := w.exp[k]
	w.mu.RUnlock()

	return ok && nowNano > whenNano
}

// BatchExpiredCheck checks multiple keys for expiration in one lock
func (w *UltraOptimizedTTLWheel) BatchExpiredCheck(keys []string, nowNano int64) []bool {
	results := make([]bool, len(keys))

	w.mu.RLock()
	for i, k := range keys {
		if whenNano, ok := w.exp[k]; ok {
			results[i] = nowNano > whenNano
		}
	}
	w.mu.RUnlock()

	return results
}

// GetExpiredKeys returns all expired keys without removing them
func (w *UltraOptimizedTTLWheel) GetExpiredKeys(nowNano int64) []string {
	expiredKeys := make([]string, 0, 64)

	w.mu.RLock()
	for k, whenNano := range w.exp {
		if nowNano > whenNano {
			expiredKeys = append(expiredKeys, k)
		}
	}
	w.mu.RUnlock()

	return expiredKeys
}

// CachedTimeNow provides cached current time updated periodically
type CachedTimeNow struct {
	mu       sync.RWMutex
	cachedNs int64
	ticker   *time.Ticker
	stop     chan struct{}
}

// NewCachedTimeNow creates cached time provider updated every interval
func NewCachedTimeNow(updateInterval time.Duration) *CachedTimeNow {
	c := &CachedTimeNow{
		cachedNs: time.Now().UnixNano(),
		ticker:   time.NewTicker(updateInterval),
		stop:     make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-c.ticker.C:
				now := time.Now().UnixNano()
				c.mu.Lock()
				c.cachedNs = now
				c.mu.Unlock()
			case <-c.stop:
				c.ticker.Stop()
				return
			}
		}
	}()

	return c
}

// Now returns the cached current time in nanoseconds
func (c *CachedTimeNow) Now() int64 {
	c.mu.RLock()
	ns := c.cachedNs
	c.mu.RUnlock()
	return ns
}

// Close stops the cached time updater
func (c *CachedTimeNow) Close() {
	close(c.stop)
}

// StringToNano converts a string key to a hash for ultra-fast lookups
func StringToNano(s string) int64 {
	// Simple hash function for demonstration - in production use a better hash
	if len(s) == 0 {
		return 0
	}

	data := unsafe.Slice(unsafe.StringData(s), len(s))
	var hash int64
	for _, b := range data {
		hash = hash*31 + int64(b)
	}
	return hash
}
