package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTTLWheelBasic(t *testing.T) {
	w := NewOptimizedTTLWheel(time.Millisecond * 10)
	w.Set("k", time.Now().Add(15*time.Millisecond))
	time.Sleep(25 * time.Millisecond)
	require.True(t, w.Expired("k"))
}
