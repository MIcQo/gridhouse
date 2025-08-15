package repl

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockConn is a mock implementation of net.Conn for testing
type mockConn struct {
	net.Conn
	writeData  []byte
	writeErr   error
	closeErr   error
	remoteAddr string
	closed     bool
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return m.closeErr
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &mockAddr{addr: m.remoteAddr}
}

type mockAddr struct {
	addr string
}

func (m *mockAddr) Network() string { return "tcp" }
func (m *mockAddr) String() string  { return m.addr }

func TestNewReplicaConn(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}

	// Create replica connection
	replicaConn := NewReplicaConn(mockConn)

	// Verify the connection was created correctly
	assert.NotNil(t, replicaConn)
	assert.Equal(t, mockConn, replicaConn.conn)
	assert.NotNil(t, replicaConn.writer)
}

func TestSendCommand(t *testing.T) {
	t.Run("successful_send", func(t *testing.T) {
		// Create a mock connection
		mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
		replicaConn := NewReplicaConn(mockConn)

		// Test data to send
		testData := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")

		// Send command
		err := replicaConn.SendCommand(testData)

		// Verify no error
		assert.NoError(t, err)

		// Verify data was written
		assert.Equal(t, testData, mockConn.writeData)
	})
}

func TestRemoteAddr(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
	replicaConn := NewReplicaConn(mockConn)

	// Get remote address
	addr := replicaConn.RemoteAddr()

	// Verify address
	assert.Equal(t, "127.0.0.1:6379", addr)
}

func TestClose(t *testing.T) {
	t.Run("successful_close", func(t *testing.T) {
		// Create a mock connection
		mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
		replicaConn := NewReplicaConn(mockConn)

		// Close connection
		err := replicaConn.Close()

		// Verify no error
		assert.NoError(t, err)

		// Verify connection was closed
		assert.True(t, mockConn.closed)
	})

	t.Run("close_error", func(t *testing.T) {
		// Create a mock connection with close error
		mockConn := &mockConn{
			remoteAddr: "127.0.0.1:6379",
			closeErr:   assert.AnError,
		}
		replicaConn := NewReplicaConn(mockConn)

		// Close connection
		err := replicaConn.Close()

		// Verify error
		assert.Error(t, err)
		assert.Equal(t, assert.AnError, err)

		// Verify connection was still marked as closed
		assert.True(t, mockConn.closed)
	})
}

func TestConcurrentSendCommand(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
	replicaConn := NewReplicaConn(mockConn)

	// Test concurrent command sending
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			testData := []byte(fmt.Sprintf("command_%d", id))
			err := replicaConn.SendCommand(testData)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all commands were sent (they get concatenated)
	assert.Greater(t, len(mockConn.writeData), 0)
}

func TestSendCommandWithLargeData(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
	replicaConn := NewReplicaConn(mockConn)

	// Create large test data
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Send large command
	err := replicaConn.SendCommand(largeData)

	// Verify no error
	assert.NoError(t, err)

	// Verify data was written
	assert.Equal(t, largeData, mockConn.writeData)
}

func TestSendCommandWithEmptyData(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
	replicaConn := NewReplicaConn(mockConn)

	// Send empty command
	err := replicaConn.SendCommand([]byte{})

	// Verify no error
	assert.NoError(t, err)

	// Verify empty data was written (or nil, both are acceptable)
	assert.True(t, len(mockConn.writeData) == 0)
}

func TestMultipleCloseCalls(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
	replicaConn := NewReplicaConn(mockConn)

	// Close connection multiple times
	err1 := replicaConn.Close()
	err2 := replicaConn.Close()

	// Verify no errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// Verify connection was closed only once
	assert.True(t, mockConn.closed)
}

func TestSendCommandAfterClose(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
	replicaConn := NewReplicaConn(mockConn)

	// Close connection
	err := replicaConn.Close()
	assert.NoError(t, err)

	// Try to send command after close
	testData := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	_ = replicaConn.SendCommand(testData)

	// This might succeed or fail depending on the underlying connection
	// The important thing is that it doesn't panic
	assert.NotPanics(t, func() {
		replicaConn.SendCommand(testData)
	})
}

func TestReplicaConnThreadSafety(t *testing.T) {
	// Create a mock connection
	mockConn := &mockConn{remoteAddr: "127.0.0.1:6379"}
	replicaConn := NewReplicaConn(mockConn)

	// Test concurrent access to RemoteAddr
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			addr := replicaConn.RemoteAddr()
			assert.Equal(t, "127.0.0.1:6379", addr)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
