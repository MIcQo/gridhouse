package aof

import (
	"bytes"
	"fmt"
	"gridhouse/internal/resp"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAOFLoaderLoadAll(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "test.aof")

	// Create a test AOF file
	w, err := NewWriter(file, EverySec)
	require.NoError(t, err)

	// Write some commands using the encoder
	commands := []resp.Value{
		{Type: resp.Array, Array: []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "k1"},
			{Type: resp.BulkString, Str: "v1"},
		}},
		{Type: resp.Array, Array: []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "k2"},
			{Type: resp.BulkString, Str: "v2"},
		}},
		{Type: resp.Array, Array: []resp.Value{
			{Type: resp.BulkString, Str: "DEL"},
			{Type: resp.BulkString, Str: "k1"},
		}},
	}

	for _, cmd := range commands {
		var buf bytes.Buffer
		require.NoError(t, resp.Encode(&buf, cmd))
		require.NoError(t, w.Append(buf.Bytes()))
	}

	require.NoError(t, w.Close())

	// Load and verify commands
	loader, err := NewLoader(file)
	require.NoError(t, err)
	defer func() {
		_ = loader.Close()
	}()

	loadedCommands, err := loader.LoadAll()
	require.NoError(t, err)
	require.Len(t, loadedCommands, 3)

	// Verify first command
	require.Equal(t, "SET", loadedCommands[0].Name)
	require.Equal(t, []string{"k1", "v1"}, loadedCommands[0].Args)

	// Verify second command
	require.Equal(t, "SET", loadedCommands[1].Name)
	require.Equal(t, []string{"k2", "v2"}, loadedCommands[1].Args)

	// Verify third command
	require.Equal(t, "DEL", loadedCommands[2].Name)
	require.Equal(t, []string{"k1"}, loadedCommands[2].Args)
}

func TestAOFLoaderReplay(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "test.aof")

	// Create a test AOF file
	w, err := NewWriter(file, EverySec)
	require.NoError(t, err)

	// Write some commands using the encoder
	commands := []resp.Value{
		{Type: resp.Array, Array: []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "k1"},
			{Type: resp.BulkString, Str: "v1"},
		}},
		{Type: resp.Array, Array: []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "k2"},
			{Type: resp.BulkString, Str: "v2"},
		}},
	}

	for _, cmd := range commands {
		var buf bytes.Buffer
		require.NoError(t, resp.Encode(&buf, cmd))
		require.NoError(t, w.Append(buf.Bytes()))
	}

	require.NoError(t, w.Close())

	// Replay commands
	loader, err := NewLoader(file)
	require.NoError(t, err)
	defer func() {
		_ = loader.Close()
	}()

	var replayedCommands []Command
	err = loader.Replay(func(cmd Command) error {
		replayedCommands = append(replayedCommands, cmd)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, replayedCommands, 2)

	// Verify replayed commands
	require.Equal(t, "SET", replayedCommands[0].Name)
	require.Equal(t, []string{"k1", "v1"}, replayedCommands[0].Args)

	require.Equal(t, "SET", replayedCommands[1].Name)
	require.Equal(t, []string{"k2", "v2"}, replayedCommands[1].Args)
}

func TestAOFLoaderEmptyFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "empty.aof")

	// Create an empty file
	_, err := os.Create(file)
	require.NoError(t, err)

	// Load from empty file
	loader, err := NewLoader(file)
	require.NoError(t, err)
	defer func() {
		_ = loader.Close()
	}()

	commands, err := loader.LoadAll()
	require.NoError(t, err)
	require.Len(t, commands, 0)
}

func TestAOFLoaderInvalidFormat(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "invalid.aof")

	// Create a file with invalid RESP format
	err := os.WriteFile(file, []byte("invalid resp format"), 0644)
	require.NoError(t, err)

	// Try to load invalid file
	loader, err := NewLoader(file)
	require.NoError(t, err)
	defer loader.Close()

	_, err = loader.LoadAll()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse AOF")
}

func TestAOFLoaderReplayError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "test.aof")

	// Create a test AOF file
	w, err := NewWriter(file, EverySec)
	require.NoError(t, err)

	// Write a command using the encoder
	cmd := resp.Value{Type: resp.Array, Array: []resp.Value{
		{Type: resp.BulkString, Str: "SET"},
		{Type: resp.BulkString, Str: "k1"},
		{Type: resp.BulkString, Str: "v1"},
	}}

	var buf bytes.Buffer
	require.NoError(t, resp.Encode(&buf, cmd))
	require.NoError(t, w.Append(buf.Bytes()))
	require.NoError(t, w.Close())

	// Replay with error
	loader, err := NewLoader(file)
	require.NoError(t, err)
	defer loader.Close()

	err = loader.Replay(func(cmd Command) error {
		return fmt.Errorf("replay error")
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to replay command SET")
}
