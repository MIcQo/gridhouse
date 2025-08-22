package cmd

import (
	"fmt"
	"gridhouse/internal/benchmark"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// benchmarkCmd represents the benchmark command
var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Run Redis benchmark tests",
	Long: `Run comprehensive Redis benchmark tests similar to redis-benchmark.
	
Examples:
  gridhouse benchmark --requests 10000 --concurrency 10
  gridhouse benchmark --commands SET,GET,INCR --requests 5000
  gridhouse benchmark --pipeline 10 --requests 10000
  gridhouse benchmark --latency-hist --requests 1000`,
	Run: runBenchmark,
}

func init() {
	rootCmd.AddCommand(benchmarkCmd)

	// Connection flags
	benchmarkCmd.Flags().String("host", "127.0.0.1", "Redis server host")
	benchmarkCmd.Flags().IntP("port", "p", 6380, "Redis server port")
	benchmarkCmd.Flags().String("password", "", "Redis server password")
	benchmarkCmd.Flags().Int("db", 0, "Redis database number")
	benchmarkCmd.Flags().Bool("tls", false, "Use TLS connection")

	// Benchmark configuration
	benchmarkCmd.Flags().Int("requests", 10000, "Total number of requests")
	benchmarkCmd.Flags().IntP("concurrency", "c", 50, "Number of parallel connections")
	benchmarkCmd.Flags().IntP("pipeline", "P", 1, "Pipeline requests")
	benchmarkCmd.Flags().Duration("timeout", 5*time.Second, "Connection timeout")
	benchmarkCmd.Flags().Bool("keep-alive", true, "Use keep-alive connections")

	// Test configuration
	benchmarkCmd.Flags().String("commands", "PING,SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET,SPOP,ZADD,ZPOPMIN,LRANGE,MSET", "Comma-separated list of commands to test")
	benchmarkCmd.Flags().Int("data-size", 2, "Data size of SET/GET values in bytes")
	benchmarkCmd.Flags().String("key-pattern", "key:__rand_int__", "Key pattern for testing")
	benchmarkCmd.Flags().Int("keyspace", 1000000, "Keyspace size for random key generation")
	benchmarkCmd.Flags().Bool("random-data", false, "Use random data for values")

	// Output flags
	benchmarkCmd.Flags().BoolP("quiet", "q", false, "Quiet mode (only show summary)")
	benchmarkCmd.Flags().Bool("csv", false, "Output in CSV format")
	benchmarkCmd.Flags().Bool("latency-hist", false, "Show latency histogram")
}

func runBenchmark(cmd *cobra.Command, _ []string) {
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
		Commands:    strings.Split(getStringFlag(cmd, "commands", "PING,SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET,SPOP,ZADD,ZPOPMIN,LRANGE,MSET"), ","),
		DataSize:    getIntFlag(cmd, "data-size", 2),
		KeyPattern:  getStringFlag(cmd, "key-pattern", "key:__rand_int__"),
		KeySpace:    getIntFlag(cmd, "keyspace", 1000000),
		RandomData:  getBoolFlag(cmd, "random-data"),
		Quiet:       getBoolFlag(cmd, "quiet"),
		CSV:         getBoolFlag(cmd, "csv"),
		LatencyHist: getBoolFlag(cmd, "latency-hist"),
	}

	// Clean up command list
	for i, cmd := range config.Commands {
		config.Commands[i] = strings.TrimSpace(cmd)
	}

	if !config.Quiet {
		fmt.Printf("Redis Benchmark Tool\n")
		fmt.Printf("===================\n")
		fmt.Printf("Host: %s:%d\n", config.Host, config.Port)
		fmt.Printf("Requests: %d\n", config.Requests)
		fmt.Printf("Concurrency: %d\n", config.Concurrency)
		fmt.Printf("Pipeline: %d\n", config.Pipeline)
		fmt.Printf("Commands: %s\n", strings.Join(config.Commands, ", "))
		fmt.Printf("Data size: %d bytes\n", config.DataSize)
		fmt.Printf("Key pattern: %s\n", config.KeyPattern)
		fmt.Printf("Keyspace: %d\n", config.KeySpace)
		fmt.Printf("\n")
	}

	results := benchmark.RunBenchmarkTests(config)
	benchmark.PrintResults(results, config)
}

func getDurationFlag(cmd *cobra.Command, name string, defaultValue time.Duration) time.Duration {
	if value, err := cmd.Flags().GetDuration(name); err == nil {
		return value
	}
	return defaultValue
}
