package aof

import (
	"bufio"
	"fmt"
	"gridhouse/internal/resp"
	"os"
)

// Command represents a command to be replayed
type Command struct {
	Name string
	Args []string
}

// Loader reads and parses AOF files
type Loader struct {
	file *os.File
}

// NewLoader creates a new AOF loader
func NewLoader(path string) (*Loader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &Loader{file: file}, nil
}

// Close closes the loader
func (l *Loader) Close() error {
	return l.file.Close()
}

// LoadAll reads all commands from the AOF file
func (l *Loader) LoadAll() ([]Command, error) {
	var commands []Command
	reader := bufio.NewReader(l.file)

	for {
		value, err := resp.Parse(reader)
		if err != nil {
			// EOF is expected at the end of file
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("failed to parse AOF: %w", err)
		}

		if value.Type != resp.Array {
			return nil, fmt.Errorf("invalid AOF format: expected array, got %v", value.Type)
		}

		if len(value.Array) == 0 {
			continue
		}

		// Extract command name and arguments
		cmdName := getBulkString(value.Array[0])
		if cmdName == "" {
			return nil, fmt.Errorf("invalid command name in AOF")
		}

		args := make([]string, len(value.Array)-1)
		for i, arg := range value.Array[1:] {
			args[i] = getBulkString(arg)
		}

		commands = append(commands, Command{
			Name: cmdName,
			Args: args,
		})
	}

	return commands, nil
}

// ReplayCallback is called for each command during replay
type ReplayCallback func(cmd Command) error

// Replay replays all commands from the AOF file
func (l *Loader) Replay(callback ReplayCallback) error {
	reader := bufio.NewReader(l.file)

	for {
		value, err := resp.Parse(reader)
		if err != nil {
			// EOF is expected at the end of file
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to parse AOF: %w", err)
		}

		if value.Type != resp.Array {
			return fmt.Errorf("invalid AOF format: expected array, got %v", value.Type)
		}

		if len(value.Array) == 0 {
			continue
		}

		// Extract command name and arguments
		cmdName := getBulkString(value.Array[0])
		if cmdName == "" {
			return fmt.Errorf("invalid command name in AOF")
		}

		args := make([]string, len(value.Array)-1)
		for i, arg := range value.Array[1:] {
			args[i] = getBulkString(arg)
		}

		cmd := Command{
			Name: cmdName,
			Args: args,
		}

		if err := callback(cmd); err != nil {
			return fmt.Errorf("failed to replay command %s: %w", cmdName, err)
		}
	}

	return nil
}

// getBulkString extracts a string from a RESP bulk string value
func getBulkString(v resp.Value) string {
	if v.Type != resp.BulkString {
		return ""
	}
	return v.Str
}
