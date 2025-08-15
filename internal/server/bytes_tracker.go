package server

import (
	"fmt"
	"net"
	"sync/atomic"
)

// connectionID is a unique identifier for each connection
type connectionID uint64

// connectionTracker wraps a connection with unique ID and tracks bytes read and written
type connectionTracker struct {
	net.Conn
	stats *ServerStats
	id    connectionID
}

// ID returns the unique connection ID
func (ct *connectionTracker) ID() string {
	return fmt.Sprintf("conn_%d", ct.id)
}

// Read tracks bytes read from the connection
func (ct *connectionTracker) Read(p []byte) (n int, err error) {
	n, err = ct.Conn.Read(p)
	if n > 0 {
		ct.stats.AddNetInputBytes(int64(n))
	}
	return n, err
}

// Write tracks bytes written to the connection
func (ct *connectionTracker) Write(p []byte) (n int, err error) {
	n, err = ct.Conn.Write(p)
	if n > 0 {
		ct.stats.AddNetOutputBytes(int64(n))
	}
	return n, err
}

// newConnectionTracker creates a new connection tracker with unique ID
func newConnectionTracker(conn net.Conn, stats *ServerStats) *connectionTracker {
	return &connectionTracker{
		Conn:  conn,
		stats: stats,
		id:    connectionID(atomic.AddUint64(&nextConnectionID, 1)),
	}
}

// Global counter for connection IDs
var nextConnectionID uint64
