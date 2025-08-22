package cmd

import (
	"gridhouse/internal/benchmark"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestBenchmarkCommand(t *testing.T) {
	// Test command creation
	cmd := benchmarkCmd
	assert.NotNil(t, cmd)
	assert.Equal(t, "benchmark", cmd.Use)
	assert.Equal(t, "Run Redis benchmark tests", cmd.Short)
}

func TestBenchmarkConfig(t *testing.T) {
	cmd := &cobra.Command{}

	// Add flags
	cmd.Flags().String("host", "127.0.0.1", "Redis server host")
	cmd.Flags().Int("port", 6380, "Redis server port")
	cmd.Flags().String("password", "", "Redis server password")
	cmd.Flags().Int("db", 0, "Redis database number")
	cmd.Flags().Bool("tls", false, "Use TLS connection")
	cmd.Flags().Int("requests", 10000, "Total number of requests")
	cmd.Flags().Int("concurrency", 50, "Number of parallel connections")
	cmd.Flags().Int("pipeline", 1, "Pipeline requests")
	cmd.Flags().Duration("timeout", 5*time.Second, "Connection timeout")
	cmd.Flags().Bool("keep-alive", true, "Use keep-alive connections")
	cmd.Flags().String("commands", "PING,SET,GET", "Comma-separated list of commands to test")
	cmd.Flags().Int("data-size", 2, "Data size of SET/GET values in bytes")
	cmd.Flags().String("key-pattern", "key:__rand_int__", "Key pattern for testing")
	cmd.Flags().Int("keyspace", 1000000, "Keyspace size for random key generation")
	cmd.Flags().Bool("random-data", false, "Use random data for values")
	cmd.Flags().Bool("quiet", false, "Quiet mode (only show summary)")
	cmd.Flags().Bool("csv", false, "Output in CSV format")
	cmd.Flags().Bool("latency-hist", false, "Show latency histogram")

	// Test default values
	config := &benchmark.BenchmarkConfig{
		Host:        getStringFlag(cmd, "host", "127.0.0.1"),
		Port:        getIntFlag(cmd, "port", 6380),
		Password:    getStringFlag(cmd, "password", ""),
		Database:    getIntFlag(cmd, "db", 0),
		Requests:    getIntFlag(cmd, "requests", 10000),
		Concurrency: getIntFlag(cmd, "concurrency", 50),
		Pipeline:    getIntFlag(cmd, "pipeline", 1),
		Timeout:     getDurationFlag(cmd, "timeout", 5*time.Second),
		KeepAlive:   getBoolFlag(cmd, "keep-alive"),
		TLS:         getBoolFlag(cmd, "tls"),
		Commands:    strings.Split(getStringFlag(cmd, "commands", "PING,SET,GET"), ","),
		DataSize:    getIntFlag(cmd, "data-size", 2),
		KeyPattern:  getStringFlag(cmd, "key-pattern", "key:__rand_int__"),
		KeySpace:    getIntFlag(cmd, "keyspace", 1000000),
		RandomData:  getBoolFlag(cmd, "random-data"),
		Quiet:       getBoolFlag(cmd, "quiet"),
		CSV:         getBoolFlag(cmd, "csv"),
		LatencyHist: getBoolFlag(cmd, "latency-hist"),
	}

	assert.Equal(t, "127.0.0.1", config.Host)
	assert.Equal(t, 6380, config.Port)
	assert.Equal(t, "", config.Password)
	assert.Equal(t, 0, config.Database)
	assert.Equal(t, 10000, config.Requests)
	assert.Equal(t, 50, config.Concurrency)
	assert.Equal(t, 1, config.Pipeline)
	assert.Equal(t, 5*time.Second, config.Timeout)
	assert.False(t, config.TLS)
	assert.Equal(t, []string{"PING", "SET", "GET"}, config.Commands)
	assert.Equal(t, 2, config.DataSize)
	assert.Equal(t, "key:__rand_int__", config.KeyPattern)
	assert.Equal(t, 1000000, config.KeySpace)
	assert.False(t, config.RandomData)
	assert.False(t, config.Quiet)
	assert.False(t, config.CSV)
	assert.False(t, config.LatencyHist)
}
