package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/stats"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestAuthHandler(t *testing.T) {
	// Test with no password required
	handler := AuthHandler("")

	tests := []struct {
		name     string
		args     []resp.Value
		expected resp.Value
		wantErr  bool
	}{
		{
			name:     "auth with no password when no password set",
			args:     []resp.Value{},
			expected: resp.Value{Type: resp.SimpleString, Str: "OK"},
			wantErr:  false,
		},
		{
			name:     "auth with any password when no password set",
			args:     []resp.Value{{Type: resp.BulkString, Str: "anypassword"}},
			expected: resp.Value{Type: resp.SimpleString, Str: "OK"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handler(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}

	// Test with password required
	handlerWithPassword := AuthHandler("secret123")

	testsWithPassword := []struct {
		name     string
		args     []resp.Value
		expected resp.Value
		wantErr  bool
	}{
		{
			name:     "auth with correct password",
			args:     []resp.Value{{Type: resp.BulkString, Str: "secret123"}},
			expected: resp.Value{Type: resp.SimpleString, Str: "OK"},
			wantErr:  false,
		},
		{
			name:     "auth with wrong password",
			args:     []resp.Value{{Type: resp.BulkString, Str: "wrongpassword"}},
			expected: resp.Value{},
			wantErr:  true,
		},
		{
			name:     "auth with no password when password required",
			args:     []resp.Value{},
			expected: resp.Value{},
			wantErr:  true,
		},
		{
			name:     "auth with wrong number of arguments",
			args:     []resp.Value{{Type: resp.BulkString, Str: "pass1"}, {Type: resp.BulkString, Str: "pass2"}},
			expected: resp.Value{},
			wantErr:  true,
		},
	}

	for _, tt := range testsWithPassword {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handlerWithPassword(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
