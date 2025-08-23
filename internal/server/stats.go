package server

import (
	"fmt"
	"runtime"
	"strings"

	"gridhouse/internal/stats"
)

// ServerStats wraps the stats manager and provides server-specific functionality
type ServerStats struct {
	statsManager *stats.OptimizedStatsManager
	port         int
}

// NewServerStats creates a new ServerStats instance
func NewServerStats(cfg *Config) *ServerStats {
	statsManager := stats.NewOptimizedStatsManager()

	port := 6380 // default
	if cfg.Addr != "" {
		// Parse port from address like ":6381" or "127.0.0.1:6381"
		if strings.Contains(cfg.Addr, ":") {
			parts := strings.Split(cfg.Addr, ":")
			if len(parts) > 1 {
				if parsedPort, err := fmt.Sscanf(parts[len(parts)-1], "%d", &port); err != nil || parsedPort != 1 {
					port = 6380 // fallback to default
				}
			}
		}
	}

	// Set initial memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	statsManager.SetUsedMemory(int64(m.Alloc))
	statsManager.SetMaxConnections(cfg.MaxConnections)

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

// IncrementRejectedConnections increments the rejected connection received counter
func (s *ServerStats) IncrementRejectedConnections() {
	s.statsManager.IncrementRejectedConnections()
}

// AddNetInputBytes adds bytes to the total input bytes counter
func (s *ServerStats) AddNetInputBytes(bytes int64) {
	s.statsManager.AddNetInputBytes(bytes)
}

// AddNetOutputBytes adds bytes to the total output bytes counter
func (s *ServerStats) AddNetOutputBytes(bytes int64) {
	s.statsManager.AddNetOutputBytes(bytes)
}
