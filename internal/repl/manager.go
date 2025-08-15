package repl

import (
	"fmt"
	"net"
	"sync"
	"time"

	"gridhouse/internal/logger"
)

// Role represents the replication role
type Role int

const (
	RoleMaster Role = iota
	RoleSlave
)

// ReplicaConnection represents a connection to a replica
type ReplicaConnection interface {
	SendCommand(data []byte) error
	RemoteAddr() string
	Close() error
}

// Replica represents a connected replica
type Replica struct {
	ID       string
	Offset   int64
	LastPing time.Time
	conn     ReplicaConnection
}

// Manager handles replication state and commands
type Manager struct {
	mu       sync.RWMutex
	role     Role
	runID    string
	backlog  *Backlog
	replicas map[string]*Replica
	offset   int64
	// Full replication protocol fields
	replicaInfo  map[string]map[string]string // replica addr -> info map
	capabilities map[string]bool              // supported capabilities
}

// NewManager creates a new replication manager
func NewManager(role Role, backlogSize int) *Manager {
	logger.Infof("Creating replication manager with role: %v, backlog size: %d", role, backlogSize)

	return &Manager{
		role:         role,
		runID:        generateRunID(),
		backlog:      NewBacklog(backlogSize),
		replicas:     make(map[string]*Replica),
		replicaInfo:  make(map[string]map[string]string),
		capabilities: make(map[string]bool),
	}
}

// Role returns the current replication role
func (m *Manager) Role() Role {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.role
}

// RunID returns the replication run ID
func (m *Manager) RunID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.runID
}

// Offset returns the current replication offset
func (m *Manager) Offset() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.offset
}

// AddReplica adds a new replica
func (m *Manager) AddReplica(id string, conn ReplicaConnection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.replicas[id] = &Replica{
		ID:       id,
		Offset:   0,
		LastPing: time.Now(),
		conn:     conn,
	}

	logger.Infof("Added replica %s from %s", id, conn.RemoteAddr())
}

// RemoveReplica removes a replica
func (m *Manager) RemoveReplica(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if replica, exists := m.replicas[id]; exists {
		if replica.conn != nil {
			if err := replica.conn.Close(); err != nil {
				logger.Errorf("Failed to close replica connection %s: %v", id, err)
			}
		}
		delete(m.replicas, id)
		logger.Infof("Removed replica %s", id)
	}
}

// GetReplica returns a replica by ID
func (m *Manager) GetReplica(id string) (*Replica, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	replica, exists := m.replicas[id]
	return replica, exists
}

// ListReplicas returns all connected replicas
func (m *Manager) ListReplicas() []*Replica {
	m.mu.RLock()
	defer m.mu.RUnlock()

	replicas := make([]*Replica, 0, len(m.replicas))
	for _, replica := range m.replicas {
		replicas = append(replicas, replica)
	}
	return replicas
}

// ListReplicas returns all connected replicas
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.replicas)
}

// AppendCommand appends a command to the replication backlog and forwards to replicas
func (m *Manager) AppendCommand(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Add to backlog
	m.backlog.Append(data)
	m.offset = m.backlog.Offset()

	// Forward to all connected replicas
	for id, replica := range m.replicas {
		if replica.conn != nil {
			if err := replica.conn.SendCommand(data); err != nil {
				logger.Errorf("Failed to send command to replica %s: %v", id, err)
				// Mark replica for removal
				go m.RemoveReplica(id)
			} else {
				// Update replica offset
				replica.Offset = m.offset
			}
		}
	}
}

// ReadFromOffset reads data from the specified offset
func (m *Manager) ReadFromOffset(offset int64, n int) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.backlog.ReadFrom(offset, n)
}

// CanPartialSync checks if partial sync is possible
func (m *Manager) CanPartialSync(offset int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return offset >= m.backlog.base
}

// Stats returns replication statistics
func (m *Manager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	replicaCount := len(m.replicas)
	connectedReplicas := 0
	for _, replica := range m.replicas {
		if time.Since(replica.LastPing) < 10*time.Second {
			connectedReplicas++
		}
	}

	return map[string]interface{}{
		"role":               m.role,
		"run_id":             m.runID,
		"offset":             m.offset,
		"backlog_size":       m.backlog.Size(),
		"backlog_capacity":   m.backlog.Capacity(),
		"replica_count":      replicaCount,
		"connected_replicas": connectedReplicas,
	}
}

// HandlePSync handles PSYNC command from replicas
func (m *Manager) HandlePSync(replID string, offset int64) (string, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.Infof("Handling PSYNC from replica %s with offset %d", replID, offset)

	// For now, we always do a full resync
	// In a full implementation, you'd check if partial sync is possible
	if replID != "?" && replID != m.runID {
		logger.Debugf("Replica run ID %s doesn't match master run ID %s, doing full resync", replID, m.runID)
	}

	// Return current run ID and offset for full resync
	return m.runID, m.offset, nil
}

// RegisterReplica registers a replica connection for ongoing command replication
func (m *Manager) RegisterReplica(conn net.Conn) error {
	replicaConn := NewReplicaConn(conn)
	replicaID := conn.RemoteAddr().String()

	m.AddReplica(replicaID, replicaConn)
	logger.Infof("Registered replica connection %s for ongoing replication", replicaID)
	return nil
}

// HandleReplConf handles REPLCONF command from replicas
func (m *Manager) HandleReplConf(args []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(args) < 2 {
		return "", fmt.Errorf("ERR wrong number of arguments for REPLCONF")
	}

	subcommand := args[0]
	logger.Debugf("Handling REPLCONF %s", subcommand)

	switch subcommand {
	case "listening-port":
		if len(args) != 2 {
			return "", fmt.Errorf("ERR wrong number of arguments for REPLCONF listening-port")
		}
		// Store replica listening port
		return "OK", nil

	case "capability":
		if len(args) != 2 {
			return "", fmt.Errorf("ERR wrong number of arguments for REPLCONF capability")
		}
		capability := args[1]
		m.capabilities[capability] = true
		logger.Debugf("Replica capability: %s", capability)
		return "OK", nil

	case "ack":
		if len(args) != 2 {
			return "", fmt.Errorf("ERR wrong number of arguments for REPLCONF ack")
		}
		// Handle replica acknowledgment
		return "OK", nil

	case "getack":
		// Return current offset
		return fmt.Sprintf("%d", m.offset), nil

	default:
		return "", fmt.Errorf("ERR unknown REPLCONF subcommand: %s", subcommand)
	}
}

// HandleSync handles SYNC command (legacy)
func (m *Manager) HandleSync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.Info("Handling legacy SYNC command")
	// Legacy SYNC always does full resync
	return nil
}

// SetReplicaInfo sets information for a specific replica
func (m *Manager) SetReplicaInfo(addr string, info map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.replicaInfo[addr] = info
	logger.Debugf("Set replica info for %s: %v", addr, info)
}

// GetReplicaInfo gets information for a specific replica
func (m *Manager) GetReplicaInfo(addr string) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.replicaInfo[addr]
	if !exists {
		return make(map[string]string)
	}
	return info
}

// generateRunID generates a unique run ID for replication
func generateRunID() string {
	// Simple implementation - in production, use crypto/rand
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
