package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSAddInlineProtocolWithColonMember(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Send inline command (legacy) terminated with CRLF
	_, err = conn.Write([]byte("sadd myset element:000000097155\r\n"))
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, ":1\r\n", line)
}
