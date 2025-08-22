package cmd

import (
	"gridhouse/internal/cli"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestCLICommand(t *testing.T) {
	// Test command creation
	cmd := cliCmd
	assert.NotNil(t, cmd)
	assert.Equal(t, "cli", cmd.Use)
	assert.Equal(t, "Interactive GridHouse command-line interface", cmd.Short)
}

func TestCLIConfig(t *testing.T) {
	cmd := &cobra.Command{}

	// Add flags
	cmd.Flags().String("host", "127.0.0.1", "GridHouse server host")
	cmd.Flags().IntP("port", "p", 6380, "GridHouse server port")
	cmd.Flags().String("password", "", "GridHouse server password")
	cmd.Flags().Int("db", 0, "Database number")
	cmd.Flags().Duration("timeout", 5*time.Second, "Connection timeout")
	cmd.Flags().Bool("tls", false, "Use TLS connection")
	cmd.Flags().Bool("raw", false, "Use raw formatting for replies")
	cmd.Flags().String("eval", "", "Send specified command")
	cmd.Flags().String("file", "", "Execute commands from file")
	cmd.Flags().Bool("pipe", false, "Pipe mode - read from stdin and write to stdout")

	// Test default values
	config := &cli.CLIConfig{
		Host:     getStringFlag(cmd, "host", "127.0.0.1"),
		Port:     getIntFlag(cmd, "port", 6380),
		Password: getStringFlag(cmd, "password", ""),
		Database: getIntFlag(cmd, "db", 0),
		Timeout:  getDurationFlag(cmd, "timeout", 5*time.Second),
		TLS:      getBoolFlag(cmd, "tls"),
		Raw:      getBoolFlag(cmd, "raw"),
		Eval:     getStringFlag(cmd, "eval", ""),
		File:     getStringFlag(cmd, "file", ""),
		Pipe:     getBoolFlag(cmd, "pipe"),
	}

	assert.Equal(t, "127.0.0.1", config.Host)
	assert.Equal(t, 6380, config.Port)
	assert.Equal(t, "", config.Password)
	assert.Equal(t, 0, config.Database)
	assert.Equal(t, 5*time.Second, config.Timeout)
	assert.False(t, config.TLS)
	assert.False(t, config.Raw)
	assert.Equal(t, "", config.Eval)
	assert.Equal(t, "", config.File)
	assert.False(t, config.Pipe)
}
