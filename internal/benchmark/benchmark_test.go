package benchmark

import (
	"bytes"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateValue(t *testing.T) {
	// Test simple pattern
	value := generateValue(5, false)
	assert.Equal(t, "xxxxx", value)

	// Test random data
	value1 := generateValue(10, true)
	value2 := generateValue(10, true)
	assert.Len(t, value1, 10)
	assert.Len(t, value2, 10)
	// Random values should be different (though there's a small chance they could be the same)
	assert.NotEqual(t, value1, value2)
}

func TestBuildCommand(t *testing.T) {
	config := &BenchmarkConfig{
		DataSize:   3,
		KeySpace:   1000,
		RandomData: false,
	}

	// Test PING command
	cmd := buildCommand("PING", config, nil, 0)
	assert.Equal(t, "*1\r\n$4\r\nPING\r\n", cmd)

	// Test SET command
	cmd = buildCommand("SET", config, nil, 123)
	assert.Contains(t, cmd, "*3\r\n$3\r\nSET\r\n")
	assert.Contains(t, cmd, "key:123")
	assert.Contains(t, cmd, "xxx")

	// Test GET command
	cmd = buildCommand("GET", config, nil, 456)
	assert.Contains(t, cmd, "*2\r\n$3\r\nGET\r\n")
	assert.Contains(t, cmd, "key:456")

	// Test INCR command
	cmd = buildCommand("INCR", config, nil, 789)
	assert.Contains(t, cmd, "*2\r\n$4\r\nINCR\r\n")
	assert.Contains(t, cmd, "counter:789")

	// Test unknown command defaults to PING
	cmd = buildCommand("UNKNOWN", config, nil, 0)
	assert.Equal(t, "*1\r\n$4\r\nPING\r\n", cmd)
}

func TestFormatDuration(t *testing.T) {
	// Test nanoseconds
	d := 500 * time.Nanosecond
	assert.Equal(t, "500.000 ns", formatDuration(d))

	// Test microseconds
	d = 500 * time.Microsecond
	assert.Equal(t, "500.000 µs", formatDuration(d))

	// Test milliseconds
	d = 500 * time.Millisecond
	assert.Equal(t, "500.000 ms", formatDuration(d))

	// Test seconds
	d = 2 * time.Second
	assert.Equal(t, "2.000 s", formatDuration(d))
}

func TestPrepareTestData(t *testing.T) {
	config := &BenchmarkConfig{
		DataSize:   5,
		RandomData: false,
	}

	testData := prepareTestData(config, 1)
	assert.Len(t, testData, 100)

	// Check a few values
	for key, value := range testData {
		assert.Contains(t, key, "test:1:")
		assert.Equal(t, "xxxxx", value) // 5 bytes of 'x'
	}
}

func TestBenchmarkResultCalculation(t *testing.T) {
	result := BenchmarkResult{
		Command:  "PING",
		Requests: 1000,
		Duration: 1 * time.Second,
		Latencies: []time.Duration{
			1 * time.Millisecond,
			2 * time.Millisecond,
			3 * time.Millisecond,
			4 * time.Millisecond,
			5 * time.Millisecond,
		},
		Errors: 0,
	}

	result.Throughput = float64(result.Requests) / result.Duration.Seconds()
	assert.Equal(t, 1000.0, result.Throughput)

	// Test percentile calculation
	if len(result.Latencies) > 0 {
		sort.Slice(result.Latencies, func(i, j int) bool {
			return result.Latencies[i] < result.Latencies[j]
		})
		result.P50Latency = result.Latencies[len(result.Latencies)*50/100]
		result.P95Latency = result.Latencies[len(result.Latencies)*95/100]
		result.P99Latency = result.Latencies[len(result.Latencies)*99/100]
	}

	assert.Equal(t, 3*time.Millisecond, result.P50Latency)
	assert.Equal(t, 5*time.Millisecond, result.P95Latency)
	assert.Equal(t, 5*time.Millisecond, result.P99Latency)
}

func TestLatencyHistogram(t *testing.T) {
	latencies := []time.Duration{
		1 * time.Microsecond,
		10 * time.Microsecond,
		100 * time.Microsecond,
		1 * time.Millisecond,
		10 * time.Millisecond,
	}

	// Capture output
	var buf bytes.Buffer
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printLatencyHistogram(latencies)

	w.Close()
	os.Stdout = oldStdout
	io.Copy(&buf, r)

	output := buf.String()
	assert.Contains(t, output, "Latency histogram:")
	assert.Contains(t, output, "<=1.000 µs: 20.0%")
	assert.Contains(t, output, "<=10.000 µs: 40.0%")
	assert.Contains(t, output, "<=100.000 µs: 60.0%")
	assert.Contains(t, output, "<=1.000 ms: 80.0%")
	assert.Contains(t, output, "<=10.000 ms: 100.0%")
}

func TestCSVOutput(t *testing.T) {
	results := []BenchmarkResult{
		{
			Command:    "PING",
			Requests:   1000,
			Duration:   1 * time.Second,
			Errors:     0,
			Throughput: 1000.0,
			P50Latency: 1 * time.Millisecond,
			P95Latency: 2 * time.Millisecond,
			P99Latency: 3 * time.Millisecond,
		},
	}

	// Capture output
	var buf bytes.Buffer
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printCSVResults(results)

	w.Close()
	os.Stdout = oldStdout
	io.Copy(&buf, r)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	assert.Len(t, lines, 2)
	assert.Contains(t, lines[0], "Command,Requests,Errors,Duration,Throughput,P50,P95,P99")
	assert.Contains(t, lines[1], "PING,1000,0,1.000 s,1000.00,1.000 ms,2.000 ms,3.000 ms")
}

func TestConnectionCreation(t *testing.T) {
	config := &BenchmarkConfig{
		Host:    "127.0.0.1",
		Port:    9999, // Use a port that's definitely not in use
		Timeout: 1 * time.Second,
		TLS:     false,
	}

	// This should fail since no server is running on port 9999
	conn, err := createConnection(config)
	assert.Error(t, err)
	assert.Nil(t, conn)
}

func TestAuthentication(t *testing.T) {
	// Create a mock server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	// Start mock server in goroutine
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read AUTH command
		buffer := make([]byte, 1024)
		n, _ := conn.Read(buffer)
		authCmd := string(buffer[:n])

		// Check if it's an AUTH command
		if strings.Contains(authCmd, "AUTH") {
			// Send OK response
			conn.Write([]byte("+OK\r\n"))
		}
	}()

	// Test authentication
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	err = authenticate(conn, "testpassword")
	assert.NoError(t, err)
}

func TestDatabaseSelection(t *testing.T) {
	// Create a mock server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	// Start mock server in goroutine
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read SELECT command
		buffer := make([]byte, 1024)
		n, _ := conn.Read(buffer)
		selectCmd := string(buffer[:n])

		// Check if it's a SELECT command
		if strings.Contains(selectCmd, "SELECT") {
			// Send OK response
			conn.Write([]byte("+OK\r\n"))
		}
	}()

	// Test database selection
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	err = selectDB(conn, 1)
	assert.NoError(t, err)
}

func TestSummaryCalculation(t *testing.T) {
	results := []BenchmarkResult{
		{
			Command:    "PING",
			Requests:   1000,
			Errors:     10,
			Throughput: 1000.0,
		},
		{
			Command:    "SET",
			Requests:   2000,
			Errors:     20,
			Throughput: 2000.0,
		},
	}

	// Capture output
	var buf bytes.Buffer
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printSummary(results)

	w.Close()
	os.Stdout = oldStdout
	io.Copy(&buf, r)

	output := buf.String()
	assert.Contains(t, output, "Total requests: 3000")
	assert.Contains(t, output, "Total errors: 30")
	assert.Contains(t, output, "Error rate: 1.00%")
	assert.Contains(t, output, "Average throughput: 1500.00 requests/second")
}
