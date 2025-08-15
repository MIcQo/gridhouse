package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPingCommand(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())

	defer func() {
		_ = srv.Close()
	}()

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+PONG\r\n", line)
}

func TestPingWithMessage(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("*2\r\n$4\r\nPING\r\n$5\r\nHello\r\n"))
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+Hello\r\n", line)
}

func TestEchoCommand(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("*2\r\n$4\r\nECHO\r\n$11\r\nHello World\r\n"))
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	r := bufio.NewReader(conn)

	// Read bulk string length
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "$11\r\n", line)

	// Read bulk string content
	content, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "Hello World\r\n", content)
}

func TestEchoCommandWrongArity(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("*1\r\n$4\r\nECHO\r\n"))
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, "ERR wrong number of arguments")
}

func TestSetGetWithTTL(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	defer srv.Close()

	c, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer c.Close()

	// SET k v EX 1
	payload := "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n1\r\n"
	_, err = c.Write([]byte(payload))
	require.NoError(t, err)

	// GET k immediately
	_, err = c.Write([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	require.NoError(t, err)

	r := bufio.NewReader(c)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+OK\r\n", line)

	// Read bulk reply for GET
	bulkLen, _ := r.ReadString('\n')
	require.Equal(t, "$1\r\n", bulkLen)
	val, _ := r.ReadString('\n')
	require.Equal(t, "v\r\n", val)

	time.Sleep(1100 * time.Millisecond)
	_, err = c.Write([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	require.NoError(t, err)
	line, err = r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "$-1\r\n", line) // Null response for expired key
}

func TestUnknownCommand(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("*1\r\n$7\r\nUNKNOWN\r\n"))
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, "ERR unknown command")
}

func TestSetWithPX(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	defer srv.Close()

	c, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer c.Close()

	// SET k v PX 100
	payload := "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$3\r\n100\r\n"
	_, err = c.Write([]byte(payload))
	require.NoError(t, err)

	// GET k immediately
	_, err = c.Write([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	require.NoError(t, err)

	r := bufio.NewReader(c)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+OK\r\n", line)

	// Read bulk reply for GET
	bulkLen, _ := r.ReadString('\n')
	require.Equal(t, "$1\r\n", bulkLen)
	val, _ := r.ReadString('\n')
	require.Equal(t, "v\r\n", val)

	time.Sleep(150 * time.Millisecond) // Wait for 100ms + buffer
	_, err = c.Write([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"))
	require.NoError(t, err)
	line, err = r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "$-1\r\n", line) // Null response for expired key
}
