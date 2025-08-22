package cli

import (
	"bufio"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandHistory(t *testing.T) {
	// Test creating new history
	history := NewCommandHistory(5)
	assert.NotNil(t, history)
	assert.Equal(t, 0, history.Len())

	// Test adding commands
	history.Add("PING")
	assert.Equal(t, 1, history.Len())

	history.Add("SET key value")
	assert.Equal(t, 2, history.Len())

	// Test not adding empty commands
	history.Add("")
	assert.Equal(t, 2, history.Len())

	// Test not adding duplicate commands
	history.Add("SET key value")
	assert.Equal(t, 2, history.Len())

	// Test previous command
	prev := history.Previous()
	assert.Equal(t, "SET key value", prev)

	prev = history.Previous()
	assert.Equal(t, "PING", prev)

	// Test next command
	next := history.Next()
	assert.Equal(t, "SET key value", next)

	next = history.Next()
	assert.Equal(t, "", next) // At the end

	// Test max size
	history.Add("GET key")
	history.Add("DEL key")
	history.Add("EXISTS key")
	history.Add("KEYS *")
	assert.Equal(t, 5, history.Len()) // Should be at max size
}

func TestCommandHistoryMaxSize(t *testing.T) {
	history := NewCommandHistory(3)

	// Add more commands than max size
	history.Add("CMD1")
	history.Add("CMD2")
	history.Add("CMD3")
	history.Add("CMD4")
	history.Add("CMD5")

	// Should only keep the last 3
	assert.Equal(t, 3, history.Len())

	// Test that the last commands are preserved
	history.ResetPosition()
	prev := history.Previous()
	assert.Equal(t, "CMD5", prev)

	prev = history.Previous()
	assert.Equal(t, "CMD4", prev)

	prev = history.Previous()
	assert.Equal(t, "CMD3", prev)
}

func TestCommandHistoryNavigation(t *testing.T) {
	history := NewCommandHistory(10)

	// Add some commands
	history.Add("PING")
	history.Add("SET key value")
	history.Add("GET key")

	// Test navigation
	assert.Equal(t, "GET key", history.Previous())
	assert.Equal(t, "SET key value", history.Previous())
	assert.Equal(t, "PING", history.Previous())
	assert.Equal(t, "", history.Previous()) // At the beginning

	// Test going forward
	assert.Equal(t, "SET key value", history.Next())
	assert.Equal(t, "GET key", history.Next())
	assert.Equal(t, "", history.Next()) // At the end

	// Test reset position
	history.ResetPosition()
	assert.Equal(t, "", history.Next()) // Should be at the end
}

func TestArrowKeyInput(t *testing.T) {
	// Test arrow key sequence detection
	reader := bufio.NewReader(strings.NewReader("test\n"))

	history := NewCommandHistory(10)
	history.Add("PING")
	history.Add("SET key value")

	currentInput := ""

	// Test basic input without arrow keys
	input, err := readInputWithHistory(reader, history, &currentInput)
	assert.NoError(t, err)
	assert.Equal(t, "test", input)

	// Test that history is maintained
	assert.Equal(t, 2, len(history.commands))
	assert.Equal(t, "PING", history.commands[0])
	assert.Equal(t, "SET key value", history.commands[1])
}

func TestCtrlCHandling(t *testing.T) {
	// Test Ctrl+C handling
	reader := bufio.NewReader(strings.NewReader("test\x03\n")) // Ctrl+C followed by Enter

	history := NewCommandHistory(10)
	currentInput := ""

	// Test that Ctrl+C is handled properly
	input, err := readInputWithHistory(reader, history, &currentInput)
	assert.NoError(t, err)
	assert.Equal(t, "", input) // Ctrl+C resets the input, so it should be empty
}

func TestParseCommand(t *testing.T) {
	// Test simple command
	cmd := parseCommand("PING")
	assert.Equal(t, "*1\r\n$4\r\nPING\r\n", cmd)

	// Test command with arguments
	cmd = parseCommand("SET key value")
	assert.Equal(t, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", cmd)

	// Test command with multiple arguments
	cmd = parseCommand("MSET key1 value1 key2 value2")
	assert.Equal(t, "*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n", cmd)

	// Test empty command
	cmd = parseCommand("")
	assert.Equal(t, "", cmd)

	// Test command with extra spaces
	cmd = parseCommand("  SET   key   value  ")
	assert.Equal(t, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", cmd)
}

func TestFormatResponse(t *testing.T) {
	// Test simple string
	response := formatResponse("+OK")
	assert.Equal(t, "OK", response)

	// Test error
	response = formatResponse("-ERR unknown command")
	assert.Equal(t, "(error) ERR unknown command", response)

	// Test integer
	response = formatResponse(":42")
	assert.Equal(t, "(integer) 42", response)

	// Test null bulk string
	response = formatResponse("$-1")
	assert.Equal(t, "(nil)", response)

	// Test bulk string
	response = formatResponse("$5\r\nhello")
	assert.Equal(t, "hello", response)

	// Test null array
	response = formatResponse("*-1")
	assert.Equal(t, "(nil)", response)

	// Test array (raw response for now)
	response = formatResponse("*2\r\n$3\r\nfoo\r\n$3\r\nbar")
	assert.Equal(t, "*2\r\n$3\r\nfoo\r\n$3\r\nbar", response)

	// Test unknown format
	response = formatResponse("unknown")
	assert.Equal(t, "unknown", response)
}

func TestReadRESPResponseFromReader(t *testing.T) {
	// Test simple string
	reader := bufio.NewReader(strings.NewReader("+OK\r\n"))
	response, err := readRESPResponseFromReader(reader)
	assert.NoError(t, err)
	assert.Equal(t, "+OK", response)

	// Test error
	reader = bufio.NewReader(strings.NewReader("-ERR unknown command\r\n"))
	response, err = readRESPResponseFromReader(reader)
	assert.NoError(t, err)
	assert.Equal(t, "-ERR unknown command", response)

	// Test integer
	reader = bufio.NewReader(strings.NewReader(":42\r\n"))
	response, err = readRESPResponseFromReader(reader)
	assert.NoError(t, err)
	assert.Equal(t, ":42", response)

	// Test null bulk string
	reader = bufio.NewReader(strings.NewReader("$-1\r\n"))
	response, err = readRESPResponseFromReader(reader)
	assert.NoError(t, err)
	assert.Equal(t, "$-1", response)

	// Test bulk string
	reader = bufio.NewReader(strings.NewReader("$5\r\nhello\r\n"))
	response, err = readRESPResponseFromReader(reader)
	assert.NoError(t, err)
	assert.Equal(t, "$5\r\nhello", response)

	// Test null array
	reader = bufio.NewReader(strings.NewReader("*-1\r\n"))
	response, err = readRESPResponseFromReader(reader)
	assert.NoError(t, err)
	assert.Equal(t, "*-1", response)

	// Test array
	reader = bufio.NewReader(strings.NewReader("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	response, err = readRESPResponseFromReader(reader)
	assert.NoError(t, err)
	assert.Equal(t, "*2\r\n$3\r\nfoo\r\n$3\r\nbar", response)
}

func TestCLIConnectionCreation(t *testing.T) {
	config := &CLIConfig{
		Host:    "127.0.0.1",
		Port:    9999, // Use a port that's definitely not in use
		Timeout: 1 * time.Second,
		TLS:     false,
	}

	// This should fail since no server is running on port 9999
	conn, err := createCLIConnection(config)
	assert.Error(t, err)
	assert.Nil(t, conn)
}

func TestCLIAuthentication(t *testing.T) {
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

	err = authenticateCLI(conn, "testpassword")
	assert.NoError(t, err)
}

func TestCLIDatabaseSelection(t *testing.T) {
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

	err = selectDBCLI(conn, 1)
	assert.NoError(t, err)
}

func TestExecuteCommand(t *testing.T) {
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

		// Read PING command
		buffer := make([]byte, 1024)
		n, _ := conn.Read(buffer)
		pingCmd := string(buffer[:n])

		// Check if it's a PING command
		if strings.Contains(pingCmd, "PING") {
			// Send PONG response
			conn.Write([]byte("+PONG\r\n"))
		}
	}()

	// Test command execution
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	executeCommand(conn, "PING", false)

	w.Close()
	os.Stdout = oldStdout
	var buf strings.Builder
	io.Copy(&buf, r)

	output := buf.String()
	assert.Contains(t, output, "PONG")
}

func TestExecuteFile(t *testing.T) {
	// Create a temporary file with commands
	tmpfile, err := os.CreateTemp("", "test_commands")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// Write test commands to file
	commands := []string{
		"# This is a comment",
		"",
		"PING",
		"SET testkey testvalue",
		"GET testkey",
	}
	for _, cmd := range commands {
		tmpfile.WriteString(cmd + "\n")
	}
	tmpfile.Close()

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

		// Handle multiple commands
		for i := 0; i < 3; i++ {
			buffer := make([]byte, 1024)
			n, _ := conn.Read(buffer)
			cmd := string(buffer[:n])

			if strings.Contains(cmd, "PING") {
				conn.Write([]byte("+PONG\r\n"))
			} else if strings.Contains(cmd, "SET") {
				conn.Write([]byte("+OK\r\n"))
			} else if strings.Contains(cmd, "GET") {
				conn.Write([]byte("$9\r\ntestvalue\r\n"))
			}
		}
	}()

	// Test file execution
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	executeFile(conn, tmpfile.Name(), false)

	w.Close()
	os.Stdout = oldStdout
	var buf strings.Builder
	io.Copy(&buf, r)

	output := buf.String()
	assert.Contains(t, output, "PONG")
	assert.Contains(t, output, "OK")
	assert.Contains(t, output, "testvalue")
}

func TestPrintHelp(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printHelp()

	w.Close()
	os.Stdout = oldStdout
	var buf strings.Builder
	io.Copy(&buf, r)

	output := buf.String()
	assert.Contains(t, output, "GridHouse CLI Commands:")
	assert.Contains(t, output, "help")
	assert.Contains(t, output, "quit")
	assert.Contains(t, output, "Navigation:")
	assert.Contains(t, output, "arrow keys")
	assert.Contains(t, output, "Redis Commands:")
	assert.Contains(t, output, "PING")
	assert.Contains(t, output, "SET")
	assert.Contains(t, output, "GET")
}
