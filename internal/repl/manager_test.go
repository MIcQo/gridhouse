package repl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockReplicaConnection is a mock implementation of ReplicaConnection for testing
type mockReplicaConnection struct {
	addr string
}

func (m *mockReplicaConnection) SendCommand(data []byte) error {
	return nil
}

func (m *mockReplicaConnection) RemoteAddr() string {
	return m.addr
}

func (m *mockReplicaConnection) Close() error {
	return nil
}

func TestManagerCreation(t *testing.T) {
	manager := NewManager(RoleMaster, 1024)

	require.Equal(t, RoleMaster, manager.Role())
	require.NotEmpty(t, manager.RunID())
	require.Equal(t, int64(0), manager.Offset())

	stats := manager.Stats()
	require.Equal(t, RoleMaster, stats["role"])
	require.Equal(t, manager.RunID(), stats["run_id"])
	require.Equal(t, int64(0), stats["offset"])
	require.Equal(t, 0, stats["replica_count"])
}

func TestManagerReplicas(t *testing.T) {
	manager := NewManager(RoleMaster, 1024)

	// Add replicas with mock connections
	manager.AddReplica("replica1", &mockReplicaConnection{addr: "127.0.0.1:6379"})
	manager.AddReplica("replica2", &mockReplicaConnection{addr: "127.0.0.1:6380"})

	// Check replica count
	stats := manager.Stats()
	require.Equal(t, 2, stats["replica_count"])

	// Get replica
	replica, exists := manager.GetReplica("replica1")
	require.True(t, exists)
	require.Equal(t, "replica1", replica.ID)

	// List replicas
	replicas := manager.ListReplicas()
	require.Len(t, replicas, 2)

	// Remove replica
	manager.RemoveReplica("replica1")
	stats = manager.Stats()
	require.Equal(t, 1, stats["replica_count"])

	// Check non-existent replica
	_, exists = manager.GetReplica("nonexistent")
	require.False(t, exists)
}

func TestManagerBacklog(t *testing.T) {
	manager := NewManager(RoleMaster, 100)

	// Append commands
	manager.AppendCommand([]byte("command1"))
	manager.AppendCommand([]byte("command2"))

	require.Equal(t, int64(16), manager.Offset()) // 8 bytes per command

	// Read from offset
	data := manager.ReadFromOffset(0, 20)
	require.Len(t, data, 16)

	// Check partial sync capability
	require.True(t, manager.CanPartialSync(0))
	require.True(t, manager.CanPartialSync(8))
	require.False(t, manager.CanPartialSync(-1))
}

func TestManagerBacklogOverflow(t *testing.T) {
	manager := NewManager(RoleMaster, 20) // Small capacity

	// Append commands that exceed capacity
	manager.AppendCommand([]byte("command1")) // 8 bytes
	manager.AppendCommand([]byte("command2")) // 8 bytes
	manager.AppendCommand([]byte("command3")) // 8 bytes
	manager.AppendCommand([]byte("command4")) // 8 bytes

	// Should have dropped oldest commands due to capacity limit
	// But the offset still tracks the total bytes written
	require.Equal(t, int64(32), manager.Offset()) // 4 commands * 8 bytes each

	// Read should only return recent data within capacity
	data := manager.ReadFromOffset(0, 50)
	require.Len(t, data, 20) // Limited by capacity
}

func TestManagerStats(t *testing.T) {
	manager := NewManager(RoleSlave, 1024)

	// Add some replicas
	manager.AddReplica("replica1", &mockReplicaConnection{addr: "127.0.0.1:6379"})
	manager.AddReplica("replica2", &mockReplicaConnection{addr: "127.0.0.1:6380"})

	// Append some commands
	manager.AppendCommand([]byte("test"))

	stats := manager.Stats()
	require.Equal(t, RoleSlave, stats["role"])
	require.Equal(t, manager.RunID(), stats["run_id"])
	require.Equal(t, int64(4), stats["offset"])
	require.Equal(t, 2, stats["replica_count"])
	require.Equal(t, 2, stats["connected_replicas"]) // All replicas are recent
	require.Equal(t, 4, stats["backlog_size"])
	require.Equal(t, 1024, stats["backlog_capacity"])
}

func TestManagerConcurrentAccess(t *testing.T) {
	manager := NewManager(RoleMaster, 1024)

	// Test concurrent replica operations
	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func(id int) {
			manager.AddReplica(fmt.Sprintf("replica%d", id), &mockReplicaConnection{addr: fmt.Sprintf("127.0.0.1:%d", 6379+id)})
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func(id int) {
			manager.RemoveReplica(fmt.Sprintf("replica%d", id))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic and stats should be consistent
	stats := manager.Stats()
	require.GreaterOrEqual(t, stats["replica_count"], 0)
}
