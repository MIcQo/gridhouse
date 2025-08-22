package cli

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"
)

// CLIConfig holds the configuration for the CLI
type CLIConfig struct {
	Host     string
	Port     int
	Password string
	Database int
	Timeout  time.Duration
	TLS      bool
	Raw      bool
	Eval     string
	File     string
	Pipe     bool
}

// CommandHistory manages command history for the CLI
type CommandHistory struct {
	commands []string
	position int
	maxSize  int
}

// NewCommandHistory creates a new command history with specified max size
func NewCommandHistory(maxSize int) *CommandHistory {
	return &CommandHistory{
		commands: make([]string, 0, maxSize),
		position: 0,
		maxSize:  maxSize,
	}
}

func (h *CommandHistory) Len() int {
	return len(h.commands)
}

// Add adds a command to history
func (h *CommandHistory) Add(command string) {
	// Don't add empty commands or duplicates of the last command
	if command == "" || (len(h.commands) > 0 && h.commands[len(h.commands)-1] == command) {
		return
	}

	// Add command to history
	h.commands = append(h.commands, command)

	// Maintain max size
	if len(h.commands) > h.maxSize {
		h.commands = h.commands[1:]
	}

	// Reset position to current (newest)
	h.position = len(h.commands)
}

// Previous returns the previous command in history
func (h *CommandHistory) Previous() string {
	if len(h.commands) == 0 {
		return ""
	}

	// If we're at the end (current input), start from the last command
	if h.position >= len(h.commands) {
		h.position = len(h.commands) - 1
		return h.commands[h.position]
	}

	// Move to previous command
	if h.position > 0 {
		h.position--
		return h.commands[h.position]
	}

	// At the beginning
	return ""
}

// Next returns the next command in history
func (h *CommandHistory) Next() string {
	if len(h.commands) == 0 {
		return ""
	}

	// Move to next command
	if h.position < len(h.commands)-1 {
		h.position++
		return h.commands[h.position]
	}

	// At the end (current input)
	h.position = len(h.commands)
	return ""
}

// ResetPosition resets the position to the end (current input)
func (h *CommandHistory) ResetPosition() {
	h.position = len(h.commands)
}

func createCLIConnection(config *CLIConfig) (net.Conn, error) {
	address := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))

	if config.TLS {
		return tls.DialWithDialer(&net.Dialer{
			Timeout:   config.Timeout,
			KeepAlive: config.Timeout,
		}, "tcp", address, &tls.Config{InsecureSkipVerify: true})
	}

	return net.DialTimeout("tcp", address, config.Timeout)
}

func authenticateCLI(conn net.Conn, password string) error {
	authCmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(password), password)
	_, err := conn.Write([]byte(authCmd))
	if err != nil {
		return err
	}

	response, err := readRESPResponse(conn)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(response, "+OK") {
		return fmt.Errorf("authentication failed: %s", response)
	}

	return nil
}

func selectDBCLI(conn net.Conn, db int) error {
	selectCmd := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n", len(strconv.Itoa(db)), db)
	_, err := conn.Write([]byte(selectCmd))
	if err != nil {
		return err
	}

	response, err := readRESPResponse(conn)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(response, "+OK") {
		return fmt.Errorf("database selection failed: %s", response)
	}

	return nil
}

func executeCommand(conn net.Conn, command string, raw bool) {
	// Parse and format the command
	cmd := parseCommand(command)
	if cmd == "" {
		fmt.Fprintf(os.Stderr, "Invalid command: %s\n", command)
		os.Exit(1)
	}

	// Send command
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error sending command: %v\n", err)
		os.Exit(1)
	}

	// Read response
	response, err := readRESPResponse(conn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
		os.Exit(1)
	}

	// Output response
	if raw {
		fmt.Print(response)
	} else {
		fmt.Println(formatResponse(response))
	}
}

func executeFile(conn net.Conn, filename string, raw bool) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file %s: %v\n", filename, err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse and execute command
		cmd := parseCommand(line)
		if cmd == "" {
			fmt.Fprintf(os.Stderr, "Invalid command at line %d: %s\n", lineNum, line)
			continue
		}

		// Send command
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending command at line %d: %v\n", lineNum, err)
			continue
		}

		// Read response
		response, err := readRESPResponse(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading response at line %d: %v\n", lineNum, err)
			continue
		}

		// Output response
		if raw {
			fmt.Print(response)
		} else {
			fmt.Printf("Line %d: %s\n", lineNum, formatResponse(response))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}
}

func executePipe(conn net.Conn, raw bool) {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if line == "" {
			continue
		}

		// Parse and execute command
		cmd := parseCommand(line)
		if cmd == "" {
			fmt.Fprintf(os.Stderr, "Invalid command: %s\n", line)
			continue
		}

		// Send command
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending command: %v\n", err)
			continue
		}

		// Read response
		response, err := readRESPResponse(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
			continue
		}

		// Output response
		if raw {
			fmt.Print(response)
		} else {
			fmt.Println(formatResponse(response))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
		os.Exit(1)
	}
}

func executeInteractive(conn net.Conn, config *CLIConfig) {
	fmt.Printf("GridHouse CLI v1.0.0\n")
	fmt.Printf("Connected to %s:%d\n", config.Host, config.Port)
	if config.Database != 0 {
		fmt.Printf("Using database %d\n", config.Database)
	}
	fmt.Printf("Type 'help' for commands, 'quit' to exit\n")
	fmt.Printf("Use arrow keys to navigate command history\n\n")

	// Initialize command history
	history := NewCommandHistory(100)

	// Set terminal to raw mode for arrow key detection
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "\r\nWarning: Could not set terminal to raw mode. Arrow key navigation may not work %v.\r\n", err)
		executeInteractiveFallback(conn, config, history)
		return
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	reader := bufio.NewReader(os.Stdin)
	currentInput := ""

	for {
		// Read input character by character
		input, err := readInputWithHistory(reader, history, &currentInput)
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			continue
		}

		// Show prompt
		fmt.Print("gridhouse> ")

		// Trim whitespace
		input = strings.TrimSpace(input)

		// Handle special commands
		if input == "" {
			continue
		}
		if input == "quit" || input == "exit" {
			break
		}
		if input == "help" {
			printHelp()
			continue
		}
		if input == "clear" {
			fmt.Print("\033[H\033[2J")
			continue
		}

		// Add command to history
		history.Add(input)

		// Parse and execute command
		cmd := parseCommand(input)
		if cmd == "" {
			fmt.Fprintf(os.Stderr, "Invalid command: %s\n", input)
			continue
		}

		// Send command
		_, err = conn.Write([]byte(cmd))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending command: %v\n", err)
			continue
		}

		// Read response
		response, err := readRESPResponse(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
			continue
		}

		// Output response
		if config.Raw {
			fmt.Print("\r" + response + "\r")
		} else {
			fmt.Println("\r" + formatResponse(response) + "\r")
		}
	}

	fmt.Print("\rGoodbye!")
}

// executeInteractiveFallback is used when raw mode is not available
func executeInteractiveFallback(conn net.Conn, config *CLIConfig, history *CommandHistory) {
	reader := bufio.NewReader(os.Stdin)

	for {
		// Show prompt
		fmt.Print("gridhouse> ")

		// Read input
		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			continue
		}

		// Trim whitespace
		input = strings.TrimSpace(input)

		// Handle special commands
		if input == "" {
			continue
		}
		if input == "quit" || input == "exit" {
			break
		}
		if input == "help" {
			printHelp()
			continue
		}
		if input == "clear" {
			fmt.Print("\033[H\033[2J")
			continue
		}

		// Add command to history
		history.Add(input)

		// Parse and execute command
		cmd := parseCommand(input)
		if cmd == "" {
			fmt.Fprintf(os.Stderr, "Invalid command: %s\n", input)
			continue
		}

		// Send command
		_, err = conn.Write([]byte(cmd))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending command: %v\n", err)
			continue
		}

		// Read response
		response, err := readRESPResponse(conn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
			continue
		}

		// Output response
		if config.Raw {
			fmt.Print(response)
		} else {
			fmt.Println(formatResponse(response))
		}
	}

	fmt.Println("Goodbye!")
}

// readInputWithHistory reads input with arrow key support for history navigation
func readInputWithHistory(reader *bufio.Reader, history *CommandHistory, currentInput *string) (string, error) {
	var input strings.Builder
	cursorPos := 0

	for {
		char, err := reader.ReadByte()
		if err != nil {
			return "", err
		}

		// Handle special characters
		if char == 27 { // ESC
			nextChar, err := reader.ReadByte()
			if err != nil {
				return "", err
			}

			if nextChar == 91 { // [
				thirdChar, err := reader.ReadByte()
				if err != nil {
					return "", err
				}

				switch thirdChar {
				case 65: // Up arrow
					if history.Len() == 0 || history.position == 0 {
						continue
					}

					// Clear current line
					fmt.Print("\r\033[K")

					// Get previous command from history
					prevCmd := history.Previous()
					if prevCmd != "" {
						*currentInput = prevCmd
						input.Reset()
						input.WriteString(prevCmd)
						cursorPos = len(prevCmd)
						fmt.Print("gridhouse> " + prevCmd)
					}
					continue

				case 66: // Down arrow
					// Clear current line
					fmt.Print("\r\033[K")

					// Get next command from history
					nextCmd := history.Next()
					if nextCmd != "" {
						*currentInput = nextCmd
						input.Reset()
						input.WriteString(nextCmd)
						cursorPos = len(nextCmd)
						fmt.Print("gridhouse> " + nextCmd)
					} else {
						// At the end of history, clear input
						*currentInput = ""
						input.Reset()
						cursorPos = 0
						fmt.Print("gridhouse> ")
					}
					continue

				case 67: // Right arrow
					if cursorPos < input.Len() {
						cursorPos++
						fmt.Print("\033[C")
					}
					continue

				case 68: // Left arrow
					if cursorPos > 0 {
						cursorPos--
						fmt.Print("\033[D")
					}
					continue

				case 72: // Home
					// Move cursor to start of input (after prompt)
					fmt.Print("\rgridhouse> ")
					cursorPos = 0
					continue

				case 70: // End
					// Move cursor to end of input
					fmt.Printf("\033[%dC", input.Len()-cursorPos) // Move to end of input
					cursorPos = input.Len()
					continue

				case 51: // Delete
					deleteChar, err := reader.ReadByte()
					if err != nil {
						return "", err
					}
					if deleteChar == 126 && cursorPos < input.Len() { // ~
						// Delete character at cursor
						current := input.String()
						if cursorPos < len(current) {
							newStr := current[:cursorPos] + current[cursorPos+1:]
							input.Reset()
							input.WriteString(newStr)
							fmt.Print("\033[P") // Delete character
						}
					}
					continue
				}
			}
		}

		// Handle backspace
		if char == 127 { // Backspace
			if cursorPos > 0 {
				current := input.String()
				newStr := current[:cursorPos-1] + current[cursorPos:]
				input.Reset()
				input.WriteString(newStr)
				cursorPos--
				fmt.Print("\b \b") // Move back, clear character, move back again
			}
			continue
		}

		// Handle Ctrl+C (ASCII 3)
		if char == 3 {
			fmt.Print("\r\nUse 'quit' or 'exit' to exit the CLI\n")
			fmt.Print("\rgridhouse> ")
			input.Reset()
			cursorPos = 0
			continue
		}

		// Handle enter
		if char == 10 || char == 13 { // Enter
			fmt.Println()
			return input.String(), nil
		}

		// Handle regular characters
		if char >= 32 && char <= 126 { // Printable ASCII
			current := input.String()
			newStr := current[:cursorPos] + string(char) + current[cursorPos:]
			input.Reset()
			input.WriteString(newStr)
			cursorPos++
			fmt.Print(string(char))
		}
	}
}

func parseCommand(input string) string {
	// Split input into parts
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return ""
	}

	// Build RESP array
	var resp strings.Builder
	resp.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))

	for _, part := range parts {
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(part), part))
	}

	return resp.String()
}

func readRESPResponse(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)
	return readRESPResponseFromReader(reader)
}

func readRESPResponseFromReader(reader *bufio.Reader) (string, error) {
	// Read the first character to determine response type
	firstChar, err := reader.ReadByte()
	if err != nil {
		return "", err
	}

	switch firstChar {
	case '+': // Simple String
		return readSimpleString(reader)
	case '-': // Error
		return readError(reader)
	case ':': // Integer
		return readInteger(reader)
	case '$': // Bulk String
		return readBulkString(reader)
	case '*': // Array
		return readArray(reader)
	default:
		return "", fmt.Errorf("unknown RESP type: %c", firstChar)
	}
}

func readSimpleString(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return "+" + strings.TrimSuffix(line, "\r\n"), nil
}

func readError(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return "-" + strings.TrimSuffix(line, "\r\n"), nil
}

func readInteger(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return ":" + strings.TrimSuffix(line, "\r\n"), nil
}

func readBulkString(reader *bufio.Reader) (string, error) {
	// Read length
	lengthStr, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	lengthStr = strings.TrimSuffix(lengthStr, "\r\n")

	if lengthStr == "-1" {
		return "$-1", nil // Null bulk string
	}

	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %s", lengthStr)
	}

	// Read the string
	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return "", err
	}

	// Read the trailing \r\n
	_, err = reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("$%d\r\n%s", length, string(data)), nil
}

func readArray(reader *bufio.Reader) (string, error) {
	// Read array length
	lengthStr, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	lengthStr = strings.TrimSuffix(lengthStr, "\r\n")

	if lengthStr == "-1" {
		return "*-1", nil // Null array
	}

	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return "", fmt.Errorf("invalid array length: %s", lengthStr)
	}

	// Build array response
	var resp strings.Builder
	resp.WriteString(fmt.Sprintf("*%d\r\n", length))

	// Read each element
	for i := 0; i < length; i++ {
		element, err := readRESPResponseFromReader(reader)
		if err != nil {
			return "", err
		}
		resp.WriteString(element)
		if i < length-1 {
			resp.WriteString("\r\n")
		}
	}

	return resp.String(), nil
}

func formatResponse(response string) string {
	// Remove RESP formatting for display
	if strings.HasPrefix(response, "+") {
		return strings.TrimPrefix(response, "+")
	}
	if strings.HasPrefix(response, "-") {
		return "(error) " + strings.TrimPrefix(response, "-")
	}
	if strings.HasPrefix(response, ":") {
		return "(integer) " + strings.TrimPrefix(response, ":")
	}
	if strings.HasPrefix(response, "$-1") {
		return "(nil)"
	}
	if strings.HasPrefix(response, "$") {
		// Extract bulk string content
		lines := strings.Split(response, "\r\n")
		if len(lines) >= 2 {
			return lines[1]
		}
		return response
	}
	if strings.HasPrefix(response, "*-1") {
		return "(nil)"
	}
	if strings.HasPrefix(response, "*") {
		// For arrays, just return the raw response for now
		return response
	}
	return response
}

func printHelp() {
	fmt.Println("\rGridHouse CLI Commands:\r")
	fmt.Println("\r  help                    - Show this help\r")
	fmt.Println("\r  quit, exit              - Exit the CLI\r")
	fmt.Println("\r  clear                   - Clear the screen\r")
	fmt.Println("\r\r")
	fmt.Println("\rNavigation:\r")
	fmt.Println("\r  arrow keys              - Navigate command history\r")
	fmt.Println("\r  ←/→ arrows              - Move cursor left/right\r")
	fmt.Println("\r  Home/End                - Move to start/end of line\r")
	fmt.Println("\r  Backspace               - Delete character\r")
	fmt.Println("\r\r")
	fmt.Println("\rRedis Commands:\r")
	fmt.Println("\r  PING                    - Test server connectivity\r")
	fmt.Println("\r  SET key value           - Set a key-value pair\r")
	fmt.Println("\r  GET key                 - Get a value by key\r")
	fmt.Println("\r  DEL key [key ...]       - Delete one or more keys\r")
	fmt.Println("\r  EXISTS key [key ...]    - Check if keys exist\r")
	fmt.Println("\r  KEYS pattern            - Find keys matching pattern\r")
	fmt.Println("\r  DBSIZE                  - Get number of keys in database\r")
	fmt.Println("\r  FLUSHDB                 - Remove all keys from database\r")
	fmt.Println("\r  INCR key                - Increment a counter\r")
	fmt.Println("\r  DECR key                - Decrement a counter\r")
	fmt.Println("\r  LPUSH key value [value ...] - Push values to list\r")
	fmt.Println("\r  RPUSH key value [value ...] - Push values to end of list\r")
	fmt.Println("\r  LPOP key                - Pop value from list\r")
	fmt.Println("\r  RPOP key                - Pop value from end of list\r")
	fmt.Println("\r  LRANGE key start stop   - Get range of elements from list\r")
	fmt.Println("\r  SADD key member [member ...] - Add members to set\r")
	fmt.Println("\r  SREM key member [member ...] - Remove members from set\r")
	fmt.Println("\r  SMEMBERS key            - Get all members of set\r")
	fmt.Println("\r  HSET key field value    - Set hash field\r")
	fmt.Println("\r  HGET key field          - Get hash field\r")
	fmt.Println("\r  HGETALL key             - Get all hash fields\r")
	fmt.Println("\r  ZADD key score member   - Add member to sorted set\r")
	fmt.Println("\r  ZRANGE key start stop   - Get range from sorted set\r")
	fmt.Println("\r  MSET key value [key value ...] - Set multiple keys\r")
	fmt.Println("\r  MGET key [key ...]      - Get multiple values\r")
	fmt.Println("\r")
}

func RunCLI(config *CLIConfig, args []string) {

	// Connect to server
	conn, err := createCLIConnection(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to %s:%d: %v\n", config.Host, config.Port, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Authenticate if needed
	if config.Password != "" {
		if err := authenticateCLI(conn, config.Password); err != nil {
			fmt.Fprintf(os.Stderr, "Authentication failed: %v\n", err)
			os.Exit(1)
		}
	}

	// Select database if needed
	if config.Database != 0 {
		if err := selectDBCLI(conn, config.Database); err != nil {
			fmt.Fprintf(os.Stderr, "Database selection failed: %v\n", err)
			os.Exit(1)
		}
	}

	// Handle different modes
	if config.Eval != "" {
		// Single command mode
		executeCommand(conn, config.Eval, config.Raw)
	} else if len(args) > 0 {
		// executing through arguments
		executeCommand(conn, strings.Join(args, " "), config.Raw)
	} else if config.File != "" {
		// File mode
		executeFile(conn, config.File, config.Raw)
	} else if config.Pipe {
		// Pipe mode
		executePipe(conn, config.Raw)
	} else {
		// Interactive mode
		executeInteractive(conn, config)
	}
}
