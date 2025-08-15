package repl

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"gridhouse/internal/logger"
)

// ReplicaConn represents a connection to a replica
type ReplicaConn struct {
	conn   net.Conn
	writer *bufio.Writer
	mu     sync.Mutex
}

// NewReplicaConn creates a new replica connection
func NewReplicaConn(conn net.Conn) *ReplicaConn {
	return &ReplicaConn{
		conn:   conn,
		writer: bufio.NewWriter(conn),
	}
}

// SendCommand sends a command to the replica
func (rc *ReplicaConn) SendCommand(data []byte) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Write the command data
	if _, err := rc.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write command: %w", err)
	}

	// Flush to ensure it's sent immediately
	if err := rc.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush command: %w", err)
	}

	logger.Debugf("Sent command to replica %s: %d bytes", rc.conn.RemoteAddr(), len(data))
	return nil
}

// RemoteAddr returns the remote address of the connection
func (rc *ReplicaConn) RemoteAddr() string {
	return rc.conn.RemoteAddr().String()
}

// Close closes the connection
func (rc *ReplicaConn) Close() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	logger.Debugf("Closing replica connection %s", rc.conn.RemoteAddr())
	return rc.conn.Close()
}
