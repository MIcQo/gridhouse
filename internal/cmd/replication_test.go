package cmd

import (
	"gridhouse/internal/repl"
	"gridhouse/internal/resp"
	"testing"

	"github.com/stretchr/testify/require"
)

// MockReplicationManager implements ReplicationManager for testing
type MockReplicationManager struct {
	role        repl.Role
	runID       string
	offset      int64
	stats       map[string]interface{}
	replicaInfo map[string]map[string]string
}

func NewMockReplicationManager(role repl.Role) *MockReplicationManager {
	return &MockReplicationManager{
		role:   role,
		runID:  "test-run-id",
		offset: 100,
		stats: map[string]interface{}{
			"role":               role,
			"run_id":             "test-run-id",
			"offset":             int64(100),
			"backlog_size":       50,
			"backlog_capacity":   1024,
			"replica_count":      2,
			"connected_replicas": 1,
		},
	}
}

func (m *MockReplicationManager) Role() repl.Role {
	return m.role
}

func (m *MockReplicationManager) RunID() string {
	return m.runID
}

func (m *MockReplicationManager) Offset() int64 {
	return m.offset
}

func (m *MockReplicationManager) Stats() map[string]interface{} {
	return m.stats
}

func (m *MockReplicationManager) HandlePSync(replID string, offset int64) (string, int64, error) {
	return m.runID, m.offset, nil
}

func (m *MockReplicationManager) HandleReplConf(args []string) (string, error) {
	return "OK", nil
}

func (m *MockReplicationManager) HandleSync() error {
	return nil
}

func (m *MockReplicationManager) SetReplicaInfo(addr string, info map[string]string) {
	if m.replicaInfo == nil {
		m.replicaInfo = make(map[string]map[string]string)
	}
	m.replicaInfo[addr] = info
}

func (m *MockReplicationManager) GetReplicaInfo(addr string) map[string]string {
	if m.replicaInfo == nil {
		return make(map[string]string)
	}
	return m.replicaInfo[addr]
}

func TestRoleCommand(t *testing.T) {
	registry := NewRegistry()
	manager := NewMockReplicationManager(repl.RoleMaster)

	registry.Register(&Command{
		Name:     "ROLE",
		Arity:    0,
		Handler:  RoleHandler(manager),
		ReadOnly: true,
	})

	// Test ROLE command
	result, err := registry.Execute("ROLE", []resp.Value{})
	require.NoError(t, err)
	require.Equal(t, resp.Array, result.Type)
	require.Len(t, result.Array, 3)

	// Check role
	require.Equal(t, resp.BulkString, result.Array[0].Type)
	require.Equal(t, "master", result.Array[0].Str)

	// Check run ID
	require.Equal(t, resp.BulkString, result.Array[1].Type)
	require.Equal(t, "test-run-id", result.Array[1].Str)

	// Check offset
	require.Equal(t, resp.Integer, result.Array[2].Type)
	require.Equal(t, int64(100), result.Array[2].Int)
}

func TestRoleCommandSlave(t *testing.T) {
	registry := NewRegistry()
	manager := NewMockReplicationManager(repl.RoleSlave)

	registry.Register(&Command{
		Name:     "ROLE",
		Arity:    0,
		Handler:  RoleHandler(manager),
		ReadOnly: true,
	})

	// Test ROLE command for slave
	result, err := registry.Execute("ROLE", []resp.Value{})
	require.NoError(t, err)
	require.Equal(t, resp.Array, result.Type)
	require.Len(t, result.Array, 3)

	// Check role
	require.Equal(t, resp.BulkString, result.Array[0].Type)
	require.Equal(t, "slave", result.Array[0].Str)
}

func TestRoleCommandWrongArity(t *testing.T) {
	registry := NewRegistry()
	manager := NewMockReplicationManager(repl.RoleMaster)

	registry.Register(&Command{
		Name:     "ROLE",
		Arity:    0,
		Handler:  RoleHandler(manager),
		ReadOnly: true,
	})

	// Test ROLE with arguments (should fail)
	_, err := registry.Execute("ROLE", []resp.Value{{Type: resp.BulkString, Str: "arg"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong number of arguments")
}

func TestInfoReplicationCommand(t *testing.T) {
	registry := NewRegistry()
	manager := NewMockReplicationManager(repl.RoleMaster)

	registry.Register(&Command{
		Name:     "INFO",
		Arity:    1,
		Handler:  InfoReplicationHandler(manager),
		ReadOnly: true,
	})

	// Test INFO replication command
	result, err := registry.Execute("INFO", []resp.Value{{Type: resp.BulkString, Str: "replication"}})
	require.NoError(t, err)
	require.Equal(t, resp.BulkString, result.Type)
	require.Contains(t, result.Str, "role:master")
	require.Contains(t, result.Str, "run_id:test-run-id")
	require.Contains(t, result.Str, "offset:100")
	require.Contains(t, result.Str, "backlog_size:50")
	require.Contains(t, result.Str, "replica_count:2")
}

func TestInfoReplicationCommandSlave(t *testing.T) {
	registry := NewRegistry()
	manager := NewMockReplicationManager(repl.RoleSlave)

	registry.Register(&Command{
		Name:     "INFO",
		Arity:    1,
		Handler:  InfoReplicationHandler(manager),
		ReadOnly: true,
	})

	// Test INFO replication command for slave
	result, err := registry.Execute("INFO", []resp.Value{{Type: resp.BulkString, Str: "replication"}})
	require.NoError(t, err)
	require.Equal(t, resp.BulkString, result.Type)
	require.Contains(t, result.Str, "role:slave")
}

func TestInfoReplicationCommandWrongSection(t *testing.T) {
	registry := NewRegistry()
	manager := NewMockReplicationManager(repl.RoleMaster)

	registry.Register(&Command{
		Name:     "INFO",
		Arity:    1,
		Handler:  InfoReplicationHandler(manager),
		ReadOnly: true,
	})

	// Test INFO with wrong section
	_, err := registry.Execute("INFO", []resp.Value{{Type: resp.BulkString, Str: "memory"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported INFO section")
}

func TestInfoReplicationCommandWrongArity(t *testing.T) {
	registry := NewRegistry()
	manager := NewMockReplicationManager(repl.RoleMaster)

	registry.Register(&Command{
		Name:     "INFO",
		Arity:    1,
		Handler:  InfoReplicationHandler(manager),
		ReadOnly: true,
	})

	// Test INFO without arguments (should fail)
	_, err := registry.Execute("INFO", []resp.Value{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong number of arguments")
}
