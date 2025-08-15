package server

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestZSetBasic(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, srv.Start())
	defer srv.Close()

	c, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)
	defer c.Close()

	// ZADD myz 1 one 2 two 1.5 onefive
	payload := strings.Join([]string{
		"*8\r\n",
		"$4\r\nZADD\r\n",
		"$3\r\nmyz\r\n",
		"$1\r\n1\r\n",
		"$3\r\none\r\n",
		"$1\r\n2\r\n",
		"$3\r\ntwo\r\n",
		"$3\r\n1.5\r\n",
		"$7\r\nonefive\r\n",
	}, "")
	_, err = c.Write([]byte(payload))
	require.NoError(t, err)

	r := bufio.NewReader(c)
	c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	// 3 new insertions
	require.Equal(t, ":3\r\n", line)

	// ZRANGE myz 0 -1 WITHSCORES
	_, err = c.Write([]byte("*5\r\n$6\r\nZRANGE\r\n$3\r\nmyz\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n"))
	require.NoError(t, err)

	// Expect array of 6 bulk strings: one,1, onefive,1.5, two,2
	line, err = r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "*6\r\n", line)
	// read pairs quickly
	for i := 0; i < 6; i++ {
		l1, err := r.ReadString('\n')
		require.NoError(t, err)
		// next should be content line
		l2, err := r.ReadString('\n')
		require.NoError(t, err)
		_ = l1
		_ = l2
	}
}
