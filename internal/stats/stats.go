package stats

import (
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gridhouse/internal/logger"
)

var Version = "7.0.0"
var Commit = "HEAD"
var BuildDate = "now"

// StatsSnapshot represents a point-in-time snapshot of all statistics
type StatsSnapshot struct {
	RedisVersion string
	OS           string
	Port         int
	Role         string
	Replicating  bool

	MaxConnections           int64
	TotalConnectionsReceived int64
	ActiveConnections        int64
	RejectedConnections      int64
	TotalCommandsProcessed   int64
	CommandsByType           map[string]int64
	DatabaseKeys             map[int]int64
	DatabaseExpires          map[int]int64
	ExpiredKeys              int64
	EvictedKeys              int64
	KeyspaceHits             int64
	KeyspaceMisses           int64
	TotalNetInputBytes       int64
	TotalNetOutputBytes      int64
	InstantaneousInputKPS    int64
	InstantaneousOutputKPS   int64
	UsedMemory               int64
	PeakMemory               int64
	MemoryFragmentationRatio float64
	Uptime                   int64
	UptimeInDays             float64
	// CPU tracking fields - cumulative CPU time in seconds
	UsedCPUSys            float64
	UsedCPUUser           float64
	UsedCPUSysChildren    float64
	UsedCPUUserChildren   float64
	UsedCPUSysMainThread  float64
	UsedCPUUserMainThread float64
}

// OptimizedStatsManager uses atomic operations and minimal locking for high-performance stats tracking
type OptimizedStatsManager struct {
	// Atomic counters for frequently updated stats
	maxConnections           int64
	totalConnectionsReceived int64
	activeConnections        int64
	rejectedConnections      int64
	totalCommandsProcessed   int64
	expiredKeys              int64
	evictedKeys              int64
	keyspaceHits             int64
	keyspaceMisses           int64
	totalNetInputBytes       int64
	totalNetOutputBytes      int64
	instantaneousInputKPS    int64
	instantaneousOutputKPS   int64
	usedMemory               int64
	peakMemory               int64

	// Less frequently updated fields with mutex protection
	mu                       sync.RWMutex
	redisVersion             string
	os                       string
	port                     int
	role                     string
	replicating              bool
	commandsByType           map[string]int64
	memoryFragmentationRatio float64
	databaseKeys             map[int]int64
	databaseExpires          map[int]int64
	startTime                time.Time

	// Latency tracking with lock-free ring buffer
	latencyMu        sync.RWMutex
	latencyByCommand map[string]*ringBuffer

	// CPU tracking fields - cumulative CPU time in seconds
	cpuMu                 sync.RWMutex
	lastCPUUpdate         time.Time
	usedCPUSys            float64
	usedCPUUser           float64
	usedCPUSysChildren    float64
	usedCPUUserChildren   float64
	usedCPUSysMainThread  float64
	usedCPUUserMainThread float64
}

// ringBuffer is a lock-free circular buffer for latency samples
type ringBuffer struct {
	data   []time.Duration
	size   int
	pos    int64 // atomic position
	filled int64 // atomic filled indicator
}

// newRingBuffer creates a new ring buffer
func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		data: make([]time.Duration, size),
		size: size,
	}
}

// add adds a value to the ring buffer
func (rb *ringBuffer) add(value time.Duration) {
	pos := atomic.AddInt64(&rb.pos, 1) % int64(rb.size)
	rb.data[pos] = value

	// Mark as filled once we've wrapped around
	if pos == 0 {
		atomic.StoreInt64(&rb.filled, 1)
	}
}

// getAverage calculates the average of all samples
func (rb *ringBuffer) getAverage() time.Duration {
	filled := atomic.LoadInt64(&rb.filled)
	pos := atomic.LoadInt64(&rb.pos)

	var sum time.Duration
	var count int

	if filled == 1 {
		// Buffer is full, use all samples
		count = rb.size
		for _, v := range rb.data {
			sum += v
		}
	} else {
		// Buffer not full, use only filled samples
		count = int(pos)
		for i := 0; i < count; i++ {
			sum += rb.data[i]
		}
	}

	if count == 0 {
		return 0
	}
	return sum / time.Duration(count)
}

// NewOptimizedStatsManager creates a new high-performance stats manager
func NewOptimizedStatsManager() *OptimizedStatsManager {
	now := time.Now()

	var osm = &OptimizedStatsManager{
		redisVersion:     "7.0.0",
		os:               runtime.GOOS + " " + runtime.GOARCH,
		commandsByType:   make(map[string]int64),
		databaseKeys:     make(map[int]int64),
		databaseExpires:  make(map[int]int64),
		startTime:        now,
		latencyByCommand: make(map[string]*ringBuffer),
		lastCPUUpdate:    now,
	}

	return osm
}

// Connection tracking methods - OPTIMIZED with atomic operations
func (s *OptimizedStatsManager) IncrementConnectionsReceived() {
	atomic.AddInt64(&s.totalConnectionsReceived, 1)
}

func (s *OptimizedStatsManager) GetTotalConnectionsReceived() int64 {
	return atomic.LoadInt64(&s.totalConnectionsReceived)
}

func (s *OptimizedStatsManager) SetActiveConnections(count int64) {
	atomic.StoreInt64(&s.activeConnections, count)
}

func (s *OptimizedStatsManager) IncrementActiveConnection() {
	atomic.AddInt64(&s.activeConnections, 1)
}

func (s *OptimizedStatsManager) DecrementActiveConnection() {
	atomic.AddInt64(&s.activeConnections, -1)
}

func (s *OptimizedStatsManager) GetActiveConnections() int64 {
	return atomic.LoadInt64(&s.activeConnections)
}

func (s *OptimizedStatsManager) IncrementRejectedConnections() {
	atomic.AddInt64(&s.rejectedConnections, 1)
}

func (s *OptimizedStatsManager) GetRejectedConnections() int64 {
	return atomic.LoadInt64(&s.rejectedConnections)
}

// Command tracking methods - OPTIMIZED
func (s *OptimizedStatsManager) IncrementCommandsProcessed() {
	atomic.AddInt64(&s.totalCommandsProcessed, 1)
}

func (s *OptimizedStatsManager) GetTotalCommandsProcessed() int64 {
	return atomic.LoadInt64(&s.totalCommandsProcessed)
}

func (s *OptimizedStatsManager) IncrementCommandByType(commandType string) {
	// Use read lock first for common case (command already exists)
	s.mu.RLock()
	if _, exists := s.commandsByType[commandType]; exists {
		s.mu.RUnlock()
		s.mu.Lock()
		s.commandsByType[commandType]++
		s.mu.Unlock()
		return
	}
	s.mu.RUnlock()

	// Command doesn't exist, need write lock to create it
	s.mu.Lock()
	s.commandsByType[commandType]++
	s.mu.Unlock()
}

func (s *OptimizedStatsManager) GetCommandsByType() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]int64, len(s.commandsByType))
	for cmd, count := range s.commandsByType {
		result[cmd] = count
	}
	return result
}

// Keyspace tracking methods - OPTIMIZED with atomic operations
func (s *OptimizedStatsManager) IncrementExpiredKeys() {
	atomic.AddInt64(&s.expiredKeys, 1)
}

func (s *OptimizedStatsManager) GetExpiredKeys() int64 {
	return atomic.LoadInt64(&s.expiredKeys)
}

func (s *OptimizedStatsManager) IncrementEvictedKeys() {
	atomic.AddInt64(&s.evictedKeys, 1)
}

func (s *OptimizedStatsManager) GetEvictedKeys() int64 {
	return atomic.LoadInt64(&s.evictedKeys)
}

func (s *OptimizedStatsManager) IncrementKeyspaceHits() {
	atomic.AddInt64(&s.keyspaceHits, 1)
}

func (s *OptimizedStatsManager) GetKeyspaceHits() int64 {
	return atomic.LoadInt64(&s.keyspaceHits)
}

func (s *OptimizedStatsManager) IncrementKeyspaceMisses() {
	atomic.AddInt64(&s.keyspaceMisses, 1)
}

func (s *OptimizedStatsManager) GetKeyspaceMisses() int64 {
	return atomic.LoadInt64(&s.keyspaceMisses)
}

// Network tracking methods - OPTIMIZED with atomic operations
func (s *OptimizedStatsManager) AddNetInputBytes(bytes int64) {
	atomic.AddInt64(&s.totalNetInputBytes, bytes)
}

func (s *OptimizedStatsManager) GetTotalNetInputBytes() int64 {
	return atomic.LoadInt64(&s.totalNetInputBytes)
}

func (s *OptimizedStatsManager) AddNetOutputBytes(bytes int64) {
	atomic.AddInt64(&s.totalNetOutputBytes, bytes)
}

func (s *OptimizedStatsManager) GetTotalNetOutputBytes() int64 {
	return atomic.LoadInt64(&s.totalNetOutputBytes)
}

func (s *OptimizedStatsManager) SetInstantaneousInputKPS(kps int64) {
	atomic.StoreInt64(&s.instantaneousInputKPS, kps)
}

func (s *OptimizedStatsManager) GetInstantaneousInputKPS() int64 {
	return atomic.LoadInt64(&s.instantaneousInputKPS)
}

func (s *OptimizedStatsManager) SetInstantaneousOutputKPS(kps int64) {
	atomic.StoreInt64(&s.instantaneousOutputKPS, kps)
}

func (s *OptimizedStatsManager) GetInstantaneousOutputKPS() int64 {
	return atomic.LoadInt64(&s.instantaneousOutputKPS)
}

// Memory tracking methods - OPTIMIZED with atomic operations
func (s *OptimizedStatsManager) SetUsedMemory(bytes int64) {
	atomic.StoreInt64(&s.usedMemory, bytes)

	// Update peak memory atomically
	for {
		peak := atomic.LoadInt64(&s.peakMemory)
		if bytes <= peak || atomic.CompareAndSwapInt64(&s.peakMemory, peak, bytes) {
			break
		}
	}
}

func (s *OptimizedStatsManager) GetUsedMemory() int64 {
	return atomic.LoadInt64(&s.usedMemory)
}

func (s *OptimizedStatsManager) GetPeakMemory() int64 {
	return atomic.LoadInt64(&s.peakMemory)
}

func (s *OptimizedStatsManager) SetMemoryFragmentationRatio(ratio float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.memoryFragmentationRatio = ratio
}

func (s *OptimizedStatsManager) GetMemoryFragmentationRatio() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memoryFragmentationRatio
}

// Latency tracking methods - OPTIMIZED with lock-free ring buffer
func (s *OptimizedStatsManager) RecordCommandLatency(commandType string, latency time.Duration) {
	// Try to get existing ring buffer with read lock
	s.latencyMu.RLock()
	rb, exists := s.latencyByCommand[commandType]
	s.latencyMu.RUnlock()

	if !exists {
		// Create new ring buffer with write lock
		s.latencyMu.Lock()
		if rb, exists = s.latencyByCommand[commandType]; !exists {
			rb = newRingBuffer(1000)
			s.latencyByCommand[commandType] = rb
		}
		s.latencyMu.Unlock()
	}

	// Add to ring buffer (lock-free)
	rb.add(latency)
}

// GetAverageLatency calculates the average latency for a command type
func (s *OptimizedStatsManager) GetAverageLatency(commandType string) time.Duration {
	s.latencyMu.RLock()
	rb, exists := s.latencyByCommand[commandType]
	s.latencyMu.RUnlock()

	if !exists {
		return 0
	}

	return rb.getAverage()
}

// CPU tracking methods - OPTIMIZED with periodic updates
func (s *OptimizedStatsManager) UpdateCPUStats() {
	s.cpuMu.Lock()
	defer s.cpuMu.Unlock()

	now := time.Now()

	// Update CPU stats every 100ms to avoid excessive overhead
	if now.Sub(s.lastCPUUpdate) < 100*time.Millisecond {
		return
	}

	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		logger.Errorf("Failed to get rusage: %v", err)
		return
	}

	// Calculate cumulative CPU time
	s.usedCPUUser += float64(rusage.Utime.Sec)
	s.usedCPUSys += float64(rusage.Stime.Sec)

	var rusageThread syscall.Rusage
	if err := syscall.Getrusage(0x1, &rusageThread); err == nil {
		// Main thread CPU (95% of total)
		s.usedCPUUserMainThread += float64(rusageThread.Utime.Sec)
		s.usedCPUSysMainThread += float64(rusageThread.Stime.Sec)
	}

	var rusageChild syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &rusageChild); err == nil {
		// Children CPU (5% of total)
		s.usedCPUUserChildren += float64(rusageChild.Utime.Sec)
		s.usedCPUSysChildren += float64(rusageChild.Stime.Sec)
	}

	s.lastCPUUpdate = now
}

func (s *OptimizedStatsManager) GetUsedCPUSys() float64 {
	s.UpdateCPUStats()
	s.cpuMu.RLock()
	defer s.cpuMu.RUnlock()
	return s.usedCPUSys
}

func (s *OptimizedStatsManager) GetUsedCPUUser() float64 {
	s.UpdateCPUStats()
	s.cpuMu.RLock()
	defer s.cpuMu.RUnlock()
	return s.usedCPUUser
}

func (s *OptimizedStatsManager) GetUsedCPUSysChildren() float64 {
	s.UpdateCPUStats()
	s.cpuMu.RLock()
	defer s.cpuMu.RUnlock()
	return s.usedCPUSysChildren
}

func (s *OptimizedStatsManager) GetUsedCPUUserChildren() float64 {
	s.UpdateCPUStats()
	s.cpuMu.RLock()
	defer s.cpuMu.RUnlock()
	return s.usedCPUUserChildren
}

func (s *OptimizedStatsManager) GetUsedCPUSysMainThread() float64 {
	s.UpdateCPUStats()
	s.cpuMu.RLock()
	defer s.cpuMu.RUnlock()
	return s.usedCPUSysMainThread
}

func (s *OptimizedStatsManager) GetUsedCPUUserMainThread() float64 {
	s.UpdateCPUStats()
	s.cpuMu.RLock()
	defer s.cpuMu.RUnlock()
	return s.usedCPUUserMainThread
}

// GetSnapshot returns a snapshot of all statistics - OPTIMIZED
func (s *OptimizedStatsManager) GetSnapshot() StatsSnapshot {
	// Get atomic values first (no locking needed)
	snapshot := StatsSnapshot{
		MaxConnections:           atomic.LoadInt64(&s.maxConnections),
		TotalConnectionsReceived: atomic.LoadInt64(&s.totalConnectionsReceived),
		ActiveConnections:        atomic.LoadInt64(&s.activeConnections),
		RejectedConnections:      atomic.LoadInt64(&s.rejectedConnections),
		TotalCommandsProcessed:   atomic.LoadInt64(&s.totalCommandsProcessed),
		ExpiredKeys:              atomic.LoadInt64(&s.expiredKeys),
		EvictedKeys:              atomic.LoadInt64(&s.evictedKeys),
		KeyspaceHits:             atomic.LoadInt64(&s.keyspaceHits),
		KeyspaceMisses:           atomic.LoadInt64(&s.keyspaceMisses),
		TotalNetInputBytes:       atomic.LoadInt64(&s.totalNetInputBytes),
		TotalNetOutputBytes:      atomic.LoadInt64(&s.totalNetOutputBytes),
		InstantaneousInputKPS:    atomic.LoadInt64(&s.instantaneousInputKPS),
		InstantaneousOutputKPS:   atomic.LoadInt64(&s.instantaneousOutputKPS),
		UsedMemory:               atomic.LoadInt64(&s.usedMemory),
		PeakMemory:               atomic.LoadInt64(&s.peakMemory),
	}

	// Get mutex-protected values
	s.mu.RLock()
	snapshot.RedisVersion = s.redisVersion
	snapshot.OS = s.os
	snapshot.Port = s.port
	snapshot.Role = s.role
	snapshot.Replicating = s.replicating
	snapshot.MemoryFragmentationRatio = s.memoryFragmentationRatio

	// Copy maps
	snapshot.CommandsByType = make(map[string]int64, len(s.commandsByType))
	for cmd, count := range s.commandsByType {
		snapshot.CommandsByType[cmd] = count
	}

	snapshot.DatabaseKeys = make(map[int]int64, len(s.databaseKeys))
	for db, count := range s.databaseKeys {
		snapshot.DatabaseKeys[db] = count
	}

	snapshot.DatabaseExpires = make(map[int]int64, len(s.databaseExpires))
	for db, count := range s.databaseExpires {
		snapshot.DatabaseExpires[db] = count
	}

	uptime := time.Since(s.startTime)
	s.mu.RUnlock()

	snapshot.Uptime = int64(uptime.Seconds())
	snapshot.UptimeInDays = uptime.Hours() / 24.0

	// Get CPU stats
	s.UpdateCPUStats()
	s.cpuMu.RLock()
	snapshot.UsedCPUSys = s.usedCPUSys
	snapshot.UsedCPUUser = s.usedCPUUser
	snapshot.UsedCPUSysChildren = s.usedCPUSysChildren
	snapshot.UsedCPUUserChildren = s.usedCPUUserChildren
	snapshot.UsedCPUSysMainThread = s.usedCPUSysMainThread
	snapshot.UsedCPUUserMainThread = s.usedCPUUserMainThread
	s.cpuMu.RUnlock()

	return snapshot
}

func (s *OptimizedStatsManager) SetMaxConnections(connections int64) {
	atomic.StoreInt64(&s.maxConnections, connections)
}
