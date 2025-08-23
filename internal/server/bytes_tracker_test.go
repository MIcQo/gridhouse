package server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBytesTracker(t *testing.T) {
	stats := NewServerStats(&Config{Addr: ":6380"})

	// Create a mock connection
	mockConnection := &mockConn{
		readData:  []byte("test data"),
		writeData: make([]byte, 0),
	}

	tracker := newConnectionTracker(mockConnection, stats)

	t.Run("track read bytes", func(t *testing.T) {
		buf := make([]byte, 10)
		n, err := tracker.Read(buf)

		assert.NoError(t, err)
		assert.Equal(t, 9, n) // "test data" is 9 bytes

		// Check that bytes were tracked
		statsManager := stats.GetStats()
		assert.Equal(t, int64(9), statsManager.GetTotalNetInputBytes())
	})

	t.Run("track write bytes", func(t *testing.T) {
		testData := []byte("response data")
		n, err := tracker.Write(testData)

		assert.NoError(t, err)
		assert.Equal(t, 13, n) // "response data" is 13 bytes

		// Check that bytes were tracked
		statsManager := stats.GetStats()
		assert.Equal(t, int64(13), statsManager.GetTotalNetOutputBytes())
	})

	t.Run("cumulative tracking", func(t *testing.T) {
		// Reset stats
		stats = NewServerStats(&Config{Addr: ":6380"})

		// Create a new mock connection for this test
		newMockConnection := &mockConn{
			readData:  []byte("test data for cumulative"),
			writeData: make([]byte, 0),
		}
		tracker = newConnectionTracker(newMockConnection, stats)

		// Multiple reads and writes
		_, _ = tracker.Read(make([]byte, 5))
		_, _ = tracker.Write([]byte("hello"))
		_, _ = tracker.Read(make([]byte, 3))
		_, _ = tracker.Write([]byte("world"))

		statsManager := stats.GetStats()
		assert.Equal(t, int64(8), statsManager.GetTotalNetInputBytes())   // 5 + 3
		assert.Equal(t, int64(10), statsManager.GetTotalNetOutputBytes()) // 5 + 5
	})
}

// mockConn is a simple mock connection for testing
type mockConn struct {
	readData  []byte
	writeData []byte
	readPos   int
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	if m.readPos >= len(m.readData) {
		return 0, nil
	}

	n = copy(p, m.readData[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	m.writeData = append(m.writeData, p...)
	return len(p), nil
}

func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }
