package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/stats"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockServerStatsProvider implements ServerStatsProvider for testing
type mockServerStatsProvider struct {
	stats *stats.OptimizedStatsManager
}

func (m *mockServerStatsProvider) GetStats() *stats.OptimizedStatsManager {
	return m.stats
}

// Mock replication manager removed - not used in current tests

func TestInfoHandlerBasic(t *testing.T) {
	// Create a mock stats provider
	mockStats := &mockServerStatsProvider{
		stats: stats.NewOptimizedStatsManager(),
	}

	handler := InfoHandler(mockStats)

	// Test basic INFO command
	result, err := handler([]resp.Value{})
	assert.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Contains(t, result.Str, "# Server")
	assert.Contains(t, result.Str, "# Clients")
	assert.Contains(t, result.Str, "# Memory")
	assert.Contains(t, result.Str, "# Stats")
	assert.Contains(t, result.Str, "# Commands")
	assert.Contains(t, result.Str, "# Keyspace")
	assert.Contains(t, result.Str, "# CPU")
}

func TestInfoHandlerCPU(t *testing.T) {
	// Create a mock stats provider
	mockStats := &mockServerStatsProvider{
		stats: stats.NewOptimizedStatsManager(),
	}

	handler := InfoHandler(mockStats)

	// Test CPU section specifically
	result, err := handler([]resp.Value{{Type: resp.BulkString, Str: "cpu"}})
	assert.NoError(t, err)
	assert.Equal(t, resp.BulkString, result.Type)
	assert.Contains(t, result.Str, "# CPU")
	assert.Contains(t, result.Str, "used_cpu_sys:")
	assert.Contains(t, result.Str, "used_cpu_user:")
	assert.Contains(t, result.Str, "used_cpu_sys_children:")
	assert.Contains(t, result.Str, "used_cpu_user_children:")
	assert.Contains(t, result.Str, "used_cpu_sys_main_thread:")
	assert.Contains(t, result.Str, "used_cpu_user_main_thread:")
}
