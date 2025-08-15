package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This test reproduces the reported issue: `sadd myset element:000000097155`
func TestSAddWithColonMember(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// SADD myset element:000000097155
	payload := "*3\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$20\r\nelement:000000097155\r\n"
	_, err = conn.Write([]byte(payload))
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	// Expect integer reply: :1\r\n
	require.Equal(t, ":1\r\n", line)
}
