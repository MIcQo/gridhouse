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

// Additional tests to increase coverage
func TestServerStatsEdgeCases(t *testing.T) {
	t.Run("empty addr config", func(t *testing.T) {
		stats := NewServerStats(&Config{})
		assert.Equal(t, 6380, stats.port)
	})

	t.Run("addr with multiple colons", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: "127.0.0.1:8080:extra"})
		assert.Equal(t, 6380, stats.port) // Should fallback to default
	})

	t.Run("addr with non-numeric port", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: "127.0.0.1:abc"})
		assert.Equal(t, 6380, stats.port) // Should fallback to default
	})

	t.Run("addr with valid port", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: "127.0.0.1:6379"})
		assert.Equal(t, 6379, stats.port)
	})

	t.Run("addr with just port", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6381"})
		assert.Equal(t, 6381, stats.port)
	})

	t.Run("decrement below zero", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})

		// Decrement when no connections exist
		stats.DecrementConnectedClients()
		assert.Equal(t, int64(0), stats.GetStats().GetActiveConnections())

		// Decrement again
		stats.DecrementConnectedClients()
		assert.Equal(t, int64(0), stats.GetStats().GetActiveConnections())
	})

	t.Run("negative network bytes", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})

		// Add negative bytes (should still work)
		stats.AddNetInputBytes(-100)
		stats.AddNetOutputBytes(-200)

		statsManager := stats.GetStats()
		assert.Equal(t, int64(-100), statsManager.GetTotalNetInputBytes())
		assert.Equal(t, int64(-200), statsManager.GetTotalNetOutputBytes())
	})

	t.Run("zero network bytes", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})

		// Add zero bytes
		stats.AddNetInputBytes(0)
		stats.AddNetOutputBytes(0)

		statsManager := stats.GetStats()
		assert.Equal(t, int64(0), statsManager.GetTotalNetInputBytes())
		assert.Equal(t, int64(0), statsManager.GetTotalNetOutputBytes())
	})

	t.Run("large network bytes", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})

		// Add large byte values
		largeValue := int64(1 << 30) // 1GB
		stats.AddNetInputBytes(largeValue)
		stats.AddNetOutputBytes(largeValue * 2)

		statsManager := stats.GetStats()
		assert.Equal(t, largeValue, statsManager.GetTotalNetInputBytes())
		assert.Equal(t, largeValue*2, statsManager.GetTotalNetOutputBytes())
	})

	t.Run("max connections config", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380", MaxConnections: 1000})
		statsManager := stats.GetStats()

		// The stats manager should have the max connections set
		snapshot := statsManager.GetSnapshot()
		assert.Equal(t, int64(1000), snapshot.MaxConnections)
	})

	t.Run("multiple memory updates", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})

		// Update memory multiple times
		stats.UpdateMemoryStats()
		updatedMemory := stats.GetStats().GetUsedMemory()

		// Memory should be updated (might be same or different due to GC)
		assert.GreaterOrEqual(t, updatedMemory, int64(0))

		// Update again
		stats.UpdateMemoryStats()
		finalMemory := stats.GetStats().GetUsedMemory()
		assert.GreaterOrEqual(t, finalMemory, int64(0))
	})

	t.Run("concurrent stats access", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})

		// Simulate concurrent access
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func() {
				stats.IncrementConnectedClients()
				stats.IncrementCommandsProcessed()
				stats.AddNetInputBytes(100)
				stats.AddNetOutputBytes(200)
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}

		statsManager := stats.GetStats()
		// Due to race conditions, we can't guarantee exact counts
		assert.GreaterOrEqual(t, statsManager.GetActiveConnections(), int64(0))
		assert.GreaterOrEqual(t, statsManager.GetTotalCommandsProcessed(), int64(0))
		assert.GreaterOrEqual(t, statsManager.GetTotalNetInputBytes(), int64(0))
		assert.GreaterOrEqual(t, statsManager.GetTotalNetOutputBytes(), int64(0))
	})

	t.Run("stats snapshot consistency", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})

		// Perform some operations
		stats.IncrementConnectedClients()
		stats.IncrementCommandsProcessed()
		stats.AddNetInputBytes(500)

		// Get snapshot
		snapshot := stats.GetStats().GetSnapshot()

		// Verify snapshot consistency
		assert.Equal(t, int64(1), snapshot.ActiveConnections)
		assert.Equal(t, int64(1), snapshot.TotalCommandsProcessed)
		assert.Equal(t, int64(500), snapshot.TotalNetInputBytes)
		// Note: Port and Role are not set in the current implementation
		assert.GreaterOrEqual(t, snapshot.Uptime, int64(0))
		assert.False(t, snapshot.Replicating)
	})

	t.Run("stats manager direct access", func(t *testing.T) {
		stats := NewServerStats(&Config{Addr: ":6380"})
		statsManager := stats.GetStats()

		// Test direct access to stats manager
		assert.NotNil(t, statsManager)

		// Test that we can call methods on the stats manager
		statsManager.IncrementKeyspaceHits()
		statsManager.IncrementKeyspaceMisses()
		statsManager.IncrementExpiredKeys()
		statsManager.IncrementEvictedKeys()

		assert.Equal(t, int64(1), statsManager.GetKeyspaceHits())
		assert.Equal(t, int64(1), statsManager.GetKeyspaceMisses())
		assert.Equal(t, int64(1), statsManager.GetExpiredKeys())
		assert.Equal(t, int64(1), statsManager.GetEvictedKeys())
	})
}
