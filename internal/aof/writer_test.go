package aof

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func init() {
	logrus.SetOutput(io.Discard)
	logrus.SetLevel(logrus.PanicLevel)
}

func TestAOFAppendAndLoad(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "appendonly.aof")
	w, err := NewWriter(file, EverySec)
	require.NoError(t, err)
	defer w.Close()

	require.NoError(t, w.Append([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")))
	require.NoError(t, w.Sync())

	time.Sleep(100 * time.Millisecond)

	b, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Contains(t, string(b), "SET")
}

func TestAOFSyncModes(t *testing.T) {
	dir := t.TempDir()

	t.Run("Always mode", func(t *testing.T) {
		file := filepath.Join(dir, "always.aof")
		w, err := NewWriter(file, Always)
		require.NoError(t, err)
		defer w.Close()

		// Write some data
		require.NoError(t, w.Append([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")))

		time.Sleep(100 * time.Millisecond)

		// Check that data is immediately synced
		b, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Contains(t, string(b), "SET")
	})

	t.Run("EverySec mode", func(t *testing.T) {
		file := filepath.Join(dir, "everysec.aof")
		w, err := NewWriter(file, EverySec)
		require.NoError(t, err)
		defer w.Close()

		// Write some data
		require.NoError(t, w.Append([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")))

		// Data might not be synced immediately
		time.Sleep(100 * time.Millisecond)

		// Force sync
		require.NoError(t, w.Sync())
		time.Sleep(100 * time.Millisecond)

		b, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Contains(t, string(b), "SET")
	})
}

func TestAOFSize(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "size.aof")
	w, err := NewWriter(file, EverySec)
	require.NoError(t, err)
	defer w.Close()

	// Initial size should be 0
	size, err := w.Size()
	require.NoError(t, err)
	require.Equal(t, int64(0), size)

	// Write some data
	data := []byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
	require.NoError(t, w.Append(data))

	// Force sync to get accurate size
	require.NoError(t, w.Sync())

	time.Sleep(100 * time.Millisecond)

	size, err = w.Size()
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), size)
}

func TestAOFClose(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "close.aof")
	w, err := NewWriter(file, EverySec)
	require.NoError(t, err)

	// Write some data
	require.NoError(t, w.Append([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")))

	// Close should flush and sync
	require.NoError(t, w.Close())

	// Verify data was written
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Contains(t, string(b), "SET")
}
