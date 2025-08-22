package benchmark

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BenchmarkResult represents the result of a benchmark test
type BenchmarkResult struct {
	Command    string
	Requests   int64
	Duration   time.Duration
	Latencies  []time.Duration
	Errors     int64
	Throughput float64
	P50Latency time.Duration
	P95Latency time.Duration
	P99Latency time.Duration
}

// BenchmarkConfig holds the configuration for benchmarking
type BenchmarkConfig struct {
	Host        string
	Port        int
	Password    string
	Database    int
	Requests    int
	Concurrency int
	Pipeline    int
	Timeout     time.Duration
	KeepAlive   bool
	TLS         bool
	Commands    []string
	DataSize    int
	KeyPattern  string
	KeySpace    int
	RandomData  bool
	Quiet       bool
	CSV         bool
	LatencyHist bool
}

func RunBenchmarkTests(config *BenchmarkConfig) []BenchmarkResult {
	var results []BenchmarkResult
	var wg sync.WaitGroup

	for _, command := range config.Commands {
		if !config.Quiet {
			fmt.Printf("Testing %s...\n", command)
		}

		result := BenchmarkResult{
			Command:   command,
			Requests:  int64(config.Requests),
			Latencies: make([]time.Duration, 0, config.Requests),
		}

		start := time.Now()

		// Create worker goroutines
		requestsPerWorker := config.Requests / config.Concurrency
		remainingRequests := config.Requests % config.Concurrency

		for i := 0; i < config.Concurrency; i++ {
			wg.Add(1)
			workerRequests := requestsPerWorker
			if i < remainingRequests {
				workerRequests++
			}

			go func(workerID, reqs int) {
				defer wg.Done()
				workerResult := runWorker(config, command, reqs, workerID)

				// Merge results
				atomic.AddInt64(&result.Errors, workerResult.Errors)
				result.Latencies = append(result.Latencies, workerResult.Latencies...)
			}(i, workerRequests)
		}

		wg.Wait()
		result.Duration = time.Since(start)
		result.Throughput = float64(result.Requests) / result.Duration.Seconds()

		// Calculate percentiles
		if len(result.Latencies) > 0 {
			sort.Slice(result.Latencies, func(i, j int) bool {
				return result.Latencies[i] < result.Latencies[j]
			})
			result.P50Latency = result.Latencies[len(result.Latencies)*50/100]
			result.P95Latency = result.Latencies[len(result.Latencies)*95/100]
			result.P99Latency = result.Latencies[len(result.Latencies)*99/100]
		}

		results = append(results, result)
	}

	return results
}

type WorkerResult struct {
	Errors    int64
	Latencies []time.Duration
}

func runWorker(config *BenchmarkConfig, command string, requests, workerID int) WorkerResult {
	result := WorkerResult{
		Latencies: make([]time.Duration, 0, requests),
	}

	// Create connection
	conn, err := createConnection(config)
	if err != nil {
		atomic.AddInt64(&result.Errors, int64(requests))
		return result
	}
	defer conn.Close()

	// Authenticate if needed
	if config.Password != "" {
		if err := authenticate(conn, config.Password); err != nil {
			atomic.AddInt64(&result.Errors, int64(requests))
			return result
		}
	}

	// Select database if needed
	if config.Database != 0 {
		if err := selectDB(conn, config.Database); err != nil {
			atomic.AddInt64(&result.Errors, int64(requests))
			return result
		}
	}

	// Prepare test data
	testData := prepareTestData(config, workerID)

	// Run requests
	if config.Pipeline > 1 {
		runPipelinedRequests(conn, command, requests, config, testData, &result)
	} else {
		runSequentialRequests(conn, command, requests, config, testData, &result)
	}

	return result
}

func createConnection(config *BenchmarkConfig) (net.Conn, error) {
	address := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))

	if config.TLS {
		return tls.DialWithDialer(&net.Dialer{
			Timeout:   config.Timeout,
			KeepAlive: config.Timeout,
		}, "tcp", address, &tls.Config{InsecureSkipVerify: true})
	}

	return net.DialTimeout("tcp", address, config.Timeout)
}

func authenticate(conn net.Conn, password string) error {
	authCmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(password), password)
	_, err := conn.Write([]byte(authCmd))
	if err != nil {
		return err
	}

	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(string(response[:n]), "+OK") {
		return fmt.Errorf("authentication failed")
	}

	return nil
}

func selectDB(conn net.Conn, db int) error {
	selectCmd := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n", len(strconv.Itoa(db)), db)
	_, err := conn.Write([]byte(selectCmd))
	if err != nil {
		return err
	}

	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(string(response[:n]), "+OK") {
		return fmt.Errorf("database selection failed")
	}

	return nil
}

func prepareTestData(config *BenchmarkConfig, workerID int) map[string]string {
	testData := make(map[string]string)

	// Generate test values
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test:%d:%d", workerID, i)
		value := generateValue(config.DataSize, config.RandomData)
		testData[key] = value
	}

	return testData
}

func generateValue(size int, random bool) string {
	if random {
		// Generate random string
		const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		result := make([]byte, size)
		for i := range result {
			result[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		}
		return string(result)
	}

	// Generate simple pattern
	return strings.Repeat("x", size)
}

func runSequentialRequests(conn net.Conn, command string, requests int, config *BenchmarkConfig, testData map[string]string, result *WorkerResult) {
	reader := bufio.NewReader(conn)

	for i := 0; i < requests; i++ {
		start := time.Now()

		cmd := buildCommand(command, config, testData, i)
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			atomic.AddInt64(&result.Errors, 1)
			continue
		}

		// Read response
		_, err = reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			atomic.AddInt64(&result.Errors, 1)
			continue
		}

		latency := time.Since(start)
		result.Latencies = append(result.Latencies, latency)
	}
}

func runPipelinedRequests(conn net.Conn, command string, requests int, config *BenchmarkConfig, testData map[string]string, result *WorkerResult) {
	reader := bufio.NewReader(conn)

	for i := 0; i < requests; i += config.Pipeline {
		start := time.Now()

		// Send pipeline of commands
		pipelineSize := config.Pipeline
		if i+pipelineSize > requests {
			pipelineSize = requests - i
		}

		for j := 0; j < pipelineSize; j++ {
			cmd := buildCommand(command, config, testData, i+j)
			_, err := conn.Write([]byte(cmd))
			if err != nil {
				atomic.AddInt64(&result.Errors, 1)
				continue
			}
		}

		// Read all responses
		for j := 0; j < pipelineSize; j++ {
			_, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				atomic.AddInt64(&result.Errors, 1)
				continue
			}
		}

		latency := time.Since(start)
		// Distribute latency across pipeline requests
		avgLatency := latency / time.Duration(pipelineSize)
		for j := 0; j < pipelineSize; j++ {
			result.Latencies = append(result.Latencies, avgLatency)
		}
	}
}

func buildCommand(command string, config *BenchmarkConfig, testData map[string]string, requestID int) string {
	switch command {
	case "PING":
		return "*1\r\n$4\r\nPING\r\n"
	case "SET":
		key := fmt.Sprintf("key:%d", requestID%config.KeySpace)
		value := generateValue(config.DataSize, config.RandomData)
		return fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	case "GET":
		key := fmt.Sprintf("key:%d", requestID%config.KeySpace)
		return fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
	case "INCR":
		key := fmt.Sprintf("counter:%d", requestID%config.KeySpace)
		return fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n", len(key), key)
	case "LPUSH":
		key := fmt.Sprintf("list:%d", requestID%config.KeySpace)
		value := generateValue(config.DataSize, config.RandomData)
		return fmt.Sprintf("*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	case "RPUSH":
		key := fmt.Sprintf("list:%d", requestID%config.KeySpace)
		value := generateValue(config.DataSize, config.RandomData)
		return fmt.Sprintf("*3\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	case "LPOP":
		key := fmt.Sprintf("list:%d", requestID%config.KeySpace)
		return fmt.Sprintf("*2\r\n$4\r\nLPOP\r\n$%d\r\n%s\r\n", len(key), key)
	case "RPOP":
		key := fmt.Sprintf("list:%d", requestID%config.KeySpace)
		return fmt.Sprintf("*2\r\n$4\r\nRPOP\r\n$%d\r\n%s\r\n", len(key), key)
	case "SADD":
		key := fmt.Sprintf("set:%d", requestID%config.KeySpace)
		value := generateValue(config.DataSize, config.RandomData)
		return fmt.Sprintf("*3\r\n$4\r\nSADD\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
	case "HSET":
		key := fmt.Sprintf("hash:%d", requestID%config.KeySpace)
		field := fmt.Sprintf("field:%d", requestID%1000)
		value := generateValue(config.DataSize, config.RandomData)
		return fmt.Sprintf("*4\r\n$4\r\nHSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(field), field, len(value), value)
	case "SPOP":
		key := fmt.Sprintf("set:%d", requestID%config.KeySpace)
		return fmt.Sprintf("*2\r\n$4\r\nSPOP\r\n$%d\r\n%s\r\n", len(key), key)
	case "ZADD":
		key := fmt.Sprintf("zset:%d", requestID%config.KeySpace)
		score := requestID % 1000
		value := generateValue(config.DataSize, config.RandomData)
		return fmt.Sprintf("*4\r\n$4\r\nZADD\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n$%d\r\n%s\r\n", len(key), key, len(strconv.Itoa(score)), score, len(value), value)
	case "ZPOPMIN":
		key := fmt.Sprintf("zset:%d", requestID%config.KeySpace)
		return fmt.Sprintf("*2\r\n$7\r\nZPOPMIN\r\n$%d\r\n%s\r\n", len(key), key)
	case "LRANGE":
		key := fmt.Sprintf("list:%d", requestID%config.KeySpace)
		return fmt.Sprintf("*4\r\n$6\r\nLRANGE\r\n$%d\r\n%s\r\n$1\r\n0\r\n$3\r\n100\r\n", len(key), key)
	case "MSET":
		// MSET with 10 keys
		cmd := "*21\r\n$4\r\nMSET\r\n"
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("mset:%d:%d", requestID%config.KeySpace, j)
			value := generateValue(config.DataSize, config.RandomData)
			cmd += fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
		}
		return cmd
	default:
		return "*1\r\n$4\r\nPING\r\n"
	}
}

func PrintResults(results []BenchmarkResult, config *BenchmarkConfig) {
	if config.CSV {
		printCSVResults(results)
		return
	}

	if !config.Quiet {
		fmt.Printf("\nBenchmark Results:\n")
		fmt.Printf("=================\n")
	}

	for _, result := range results {
		if config.Quiet {
			fmt.Printf("%s: %.2f requests per second, p50=%s\n",
				result.Command, result.Throughput, formatDuration(result.P50Latency))
		} else {
			fmt.Printf("%s: %.2f requests per second\n", result.Command, result.Throughput)
			fmt.Printf("  Duration: %s\n", formatDuration(result.Duration))
			fmt.Printf("  Requests: %d\n", result.Requests)
			fmt.Printf("  Errors: %d\n", result.Errors)
			fmt.Printf("  Latency percentiles:\n")
			fmt.Printf("    p50: %s\n", formatDuration(result.P50Latency))
			fmt.Printf("    p95: %s\n", formatDuration(result.P95Latency))
			fmt.Printf("    p99: %s\n", formatDuration(result.P99Latency))

			if config.LatencyHist && len(result.Latencies) > 0 {
				printLatencyHistogram(result.Latencies)
			}
			fmt.Printf("\n")
		}
	}

	if !config.Quiet {
		printSummary(results)
	}
}

func printCSVResults(results []BenchmarkResult) {
	fmt.Printf("Command,Requests,Errors,Duration,Throughput,P50,P95,P99\n")
	for _, result := range results {
		fmt.Printf("%s,%d,%d,%s,%.2f,%s,%s,%s\n",
			result.Command,
			result.Requests,
			result.Errors,
			formatDuration(result.Duration),
			result.Throughput,
			formatDuration(result.P50Latency),
			formatDuration(result.P95Latency),
			formatDuration(result.P99Latency))
	}
}

func printLatencyHistogram(latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}

	// Create histogram buckets
	buckets := []time.Duration{
		1 * time.Microsecond,
		10 * time.Microsecond,
		100 * time.Microsecond,
		1 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		1 * time.Second,
	}

	fmt.Printf("  Latency histogram:\n")
	for _, bucket := range buckets {
		count := 0
		for _, latency := range latencies {
			if latency <= bucket {
				count++
			}
		}
		percentage := float64(count) / float64(len(latencies)) * 100
		fmt.Printf("    <=%s: %.1f%%\n", formatDuration(bucket), percentage)
	}
}

func printSummary(results []BenchmarkResult) {
	if len(results) == 0 {
		return
	}

	var totalRequests int64
	var totalErrors int64
	var totalThroughput float64

	for _, result := range results {
		totalRequests += result.Requests
		totalErrors += result.Errors
		totalThroughput += result.Throughput
	}

	fmt.Printf("Summary:\n")
	fmt.Printf("  Total requests: %d\n", totalRequests)
	fmt.Printf("  Total errors: %d\n", totalErrors)
	fmt.Printf("  Error rate: %.2f%%\n", float64(totalErrors)/float64(totalRequests)*100)
	fmt.Printf("  Average throughput: %.2f requests/second\n", totalThroughput/float64(len(results)))
}

func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.3f ns", float64(d.Nanoseconds()))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.3f µs", float64(d.Microseconds()))
	} else if d < time.Second {
		return fmt.Sprintf("%.3f ms", float64(d.Milliseconds()))
	} else {
		return fmt.Sprintf("%.3f s", d.Seconds())
	}
}
