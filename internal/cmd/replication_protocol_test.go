package cmd

import (
	"testing"

	"gridhouse/internal/repl"
	"gridhouse/internal/resp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockReplicationManager implements ReplicationManager for testing
type mockReplicationManager struct {
	role        repl.Role
	runID       string
	offset      int64
	stats       map[string]interface{}
	replicaInfo map[string]map[string]string
}

func (m *mockReplicationManager) Role() repl.Role               { return m.role }
func (m *mockReplicationManager) RunID() string                 { return m.runID }
func (m *mockReplicationManager) Offset() int64                 { return m.offset }
func (m *mockReplicationManager) Stats() map[string]interface{} { return m.stats }

func (m *mockReplicationManager) HandlePSync(replID string, offset int64) (string, int64, error) {
	return m.runID, m.offset, nil
}

func (m *mockReplicationManager) HandleReplConf(args []string) (string, error) {
	if len(args) < 2 {
		return "", assert.AnError
	}
	return "OK", nil
}

func (m *mockReplicationManager) HandleSync() error {
	return nil
}

func (m *mockReplicationManager) SetReplicaInfo(addr string, info map[string]string) {
	m.replicaInfo[addr] = info
}

func (m *mockReplicationManager) GetReplicaInfo(addr string) map[string]string {
	return m.replicaInfo[addr]
}

func TestPSyncHandler(t *testing.T) {
	manager := &mockReplicationManager{
		role:   repl.RoleMaster,
		runID:  "test-run-id",
		offset: 12345,
	}

	handler := PSyncHandler(manager)

	t.Run("valid psync", func(t *testing.T) {
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "?"},
			{Type: resp.BulkString, Str: "-1"},
		})
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "FULLRESYNC test-run-id 12345", result.Str)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		_, err := handler([]resp.Value{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")
	})

	t.Run("invalid offset", func(t *testing.T) {
		_, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "?"},
			{Type: resp.BulkString, Str: "invalid"},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid offset")
	})
}

func TestReplConfHandler(t *testing.T) {
	manager := &mockReplicationManager{
		replicaInfo: make(map[string]map[string]string),
	}

	handler := ReplConfHandler(manager)

	t.Run("listening-port", func(t *testing.T) {
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "listening-port"},
			{Type: resp.BulkString, Str: "6381"},
		})
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)
	})

	t.Run("capability", func(t *testing.T) {
		result, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "capability"},
			{Type: resp.BulkString, Str: "eof"},
		})
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)
	})

	t.Run("insufficient arguments", func(t *testing.T) {
		_, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "listening-port"},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")
	})
}

func TestSyncHandler(t *testing.T) {
	manager := &mockReplicationManager{}
	handler := SyncHandler(manager)

	t.Run("valid sync", func(t *testing.T) {
		result, err := handler([]resp.Value{})
		require.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		_, err := handler([]resp.Value{
			{Type: resp.BulkString, Str: "extra"},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments")
	})
}
