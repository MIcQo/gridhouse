package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServerStats(t *testing.T) {
	stats := NewServerStats(&Config{Addr: ":6380"})

	t.Run("initial stats", func(t *testing.T) {
		statsManager := stats.GetStats()

		assert.Equal(t, int64(0), statsManager.GetTotalConnectionsReceived())
		assert.Equal(t, int64(0), statsManager.GetTotalCommandsProcessed())
		assert.Equal(t, int64(0), statsManager.GetActiveConnections())
		snapshot := statsManager.GetSnapshot()
		assert.GreaterOrEqual(t, snapshot.Uptime, int64(0))
		assert.Greater(t, statsManager.GetUsedMemory(), int64(0))
	})

	t.Run("connection tracking", func(t *testing.T) {
		// Reset stats
		stats = NewServerStats(&Config{Addr: ":6380"})

		// Simulate connections
		stats.IncrementConnectedClients()
		stats.IncrementConnectedClients()

		statsManager := stats.GetStats()
		assert.Equal(t, int64(2), statsManager.GetActiveConnections())

		// Simulate disconnections
		stats.DecrementConnectedClients()
		statsManager = stats.GetStats()
		assert.Equal(t, int64(1), statsManager.GetActiveConnections())
	})

	t.Run("command tracking", func(t *testing.T) {
		// Reset stats
		stats = NewServerStats(&Config{Addr: ":6380"})

		// Simulate commands
		stats.IncrementCommandsProcessed()
		stats.IncrementCommandsProcessed()
		stats.IncrementCommandsProcessed()

		statsManager := stats.GetStats()
		assert.Equal(t, int64(3), statsManager.GetTotalCommandsProcessed())
	})

	t.Run("uptime tracking", func(t *testing.T) {
		// Reset stats
		stats = NewServerStats(&Config{Addr: ":6380"})

		initialSnapshot := stats.GetStats().GetSnapshot()
		initialUptime := initialSnapshot.Uptime

		// Wait a bit longer to ensure measurable difference
		time.Sleep(200 * time.Millisecond)

		newSnapshot := stats.GetStats().GetSnapshot()
		newUptime := newSnapshot.Uptime
		assert.GreaterOrEqual(t, newUptime, initialUptime)
	})

	t.Run("memory tracking", func(t *testing.T) {
		statsManager := stats.GetStats()

		// Memory should be tracked
		assert.Greater(t, statsManager.GetUsedMemory(), int64(0))
	})

	t.Run("network tracking", func(t *testing.T) {
		// Reset stats
		stats = NewServerStats(&Config{Addr: ":6380"})

		// Test network byte tracking
		stats.AddNetInputBytes(100)
		stats.AddNetOutputBytes(200)
		stats.AddNetInputBytes(50)
		stats.AddNetOutputBytes(75)

		statsManager := stats.GetStats()
		assert.Equal(t, int64(150), statsManager.GetTotalNetInputBytes())  // 100 + 50
		assert.Equal(t, int64(275), statsManager.GetTotalNetOutputBytes()) // 200 + 75
	})

	t.Run("connection received tracking", func(t *testing.T) {
		// Reset stats
		stats = NewServerStats(&Config{Addr: ":6380"})

		// Test connection tracking
		stats.IncrementConnectionsReceived()
		stats.IncrementConnectionsReceived()
		stats.IncrementConnectionsReceived()

		statsManager := stats.GetStats()
		assert.Equal(t, int64(3), statsManager.GetTotalConnectionsReceived())
	})

	t.Run("connection received rejection", func(t *testing.T) {
		// Reset stats
		stats = NewServerStats(&Config{Addr: ":6380"})

		// Test connection tracking
		stats.IncrementRejectedConnections()
		stats.IncrementRejectedConnections()
		stats.IncrementRejectedConnections()

		statsManager := stats.GetStats()
		assert.Equal(t, int64(3), statsManager.GetRejectedConnections())
	})

	t.Run("invalid server addr", func(t *testing.T) {
		stats = NewServerStats(&Config{Addr: "127.0.0.1:"})

		assert.Equal(t, stats.port, 6380)
	})

	t.Run("update memory stats", func(t *testing.T) {
		stats = NewServerStats(&Config{Addr: ":6380"})

		stats.UpdateMemoryStats()

		assert.NotZero(t, stats.GetStats().GetUsedMemory())
		assert.NotZero(t, stats.GetStats().GetPeakMemory())
	})
}
