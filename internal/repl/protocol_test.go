package repl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBacklogRing(t *testing.T) {
	b := NewBacklog(1024)
	b.Append([]byte("abc"))
	off := b.Offset()
	chunk := b.ReadFrom(off-2, 2)
	require.Equal(t, []byte("bc"), chunk)
}
