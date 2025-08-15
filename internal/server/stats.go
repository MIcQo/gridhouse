package server

import (
	"runtime"

	"gridhouse/internal/stats"
)

// ServerStats wraps the stats manager and provides server-specific functionality
type ServerStats struct {
	statsManager *stats.OptimizedStatsManager
	port         int
}

// NewServerStats creates a new ServerStats instance
func NewServerStats(port int, persistenceManager interface{}) *ServerStats {
	statsManager := stats.NewOptimizedStatsManager()

	// Set initial memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	statsManager.SetUsedMemory(int64(m.Alloc))

	return &ServerStats{
		statsManager: statsManager,
		port:         port,
	}
}

// GetStats returns the stats manager
func (s *ServerStats) GetStats() *stats.OptimizedStatsManager {
	return s.statsManager
}

// UpdateMemoryStats updates memory statistics
func (s *ServerStats) UpdateMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s.statsManager.SetUsedMemory(int64(m.Alloc))
}

// IncrementConnectedClients increments the connected clients counter
func (s *ServerStats) IncrementConnectedClients() {
	s.statsManager.SetActiveConnections(s.statsManager.GetActiveConnections() + 1)
}

// DecrementConnectedClients decrements the connected clients counter
func (s *ServerStats) DecrementConnectedClients() {
	current := s.statsManager.GetActiveConnections()
	if current > 0 {
		s.statsManager.SetActiveConnections(current - 1)
	}
}

// IncrementCommandsProcessed increments the commands processed counter
func (s *ServerStats) IncrementCommandsProcessed() {
	s.statsManager.IncrementCommandsProcessed()
}

// IncrementConnectionsReceived increments the connections received counter
func (s *ServerStats) IncrementConnectionsReceived() {
	s.statsManager.IncrementConnectionsReceived()
}

// AddNetInputBytes adds bytes to the total input bytes counter
func (s *ServerStats) AddNetInputBytes(bytes int64) {
	s.statsManager.AddNetInputBytes(bytes)
}

// AddNetOutputBytes adds bytes to the total output bytes counter
func (s *ServerStats) AddNetOutputBytes(bytes int64) {
	s.statsManager.AddNetOutputBytes(bytes)
}
