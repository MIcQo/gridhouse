package repl

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gridhouse/internal/logger"
	rdb "gridhouse/internal/rdb/v2"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
)

// Slave represents a replication slave that can connect to a master
type Slave struct {
	masterAddr string
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	runID      string
	offset     int64
	role       Role
	stopChan   chan struct{}
	db         store.DataStore // Database instance to apply replicated commands
}

// NewSlave creates a new slave instance
func NewSlave(masterAddr string, db store.DataStore) *Slave {
	return &Slave{
		masterAddr: masterAddr,
		role:       RoleSlave,
		stopChan:   make(chan struct{}),
		db:         db,
	}
}

// Connect connects to the master and initiates replication
func (s *Slave) Connect() error {
	logger.Infof("Connecting to master at %s", s.masterAddr)

	var err error
	s.conn, err = net.Dial("tcp", s.masterAddr)
	if err != nil {
		logger.Errorf("Failed to connect to master %s: %v", s.masterAddr, err)
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	s.reader = bufio.NewReader(s.conn)
	s.writer = bufio.NewWriter(s.conn)

	logger.Info("Connected to master, starting replication handshake")
	return s.performHandshake()
}

// performHandshake performs the replication handshake with the master
func (s *Slave) performHandshake() error {
	// Step 1: Send PING
	logger.Debug("Sending PING to master")
	if err := s.sendCommand("PING"); err != nil {
		return fmt.Errorf("failed to send PING: %w", err)
	}

	response, err := s.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read PING response: %w", err)
	}

	if response.Type != resp.SimpleString || response.Str != "PONG" {
		return fmt.Errorf("unexpected PING response: %v", response)
	}

	logger.Debug("Received PONG from master")

	// Step 2: Send REPLCONF listening-port
	logger.Debug("Sending REPLCONF listening-port")
	if err := s.sendCommand("REPLCONF", "listening-port", "6381"); err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port: %w", err)
	}

	response, err = s.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read REPLCONF response: %w", err)
	}

	if response.Type != resp.SimpleString || response.Str != "OK" {
		return fmt.Errorf("unexpected REPLCONF response: %v", response)
	}

	logger.Debug("Received OK for REPLCONF listening-port")

	// Step 3: Send REPLCONF capability
	logger.Debug("Sending REPLCONF capability")
	if err := s.sendCommand("REPLCONF", "capability", "eof"); err != nil {
		return fmt.Errorf("failed to send REPLCONF capability: %w", err)
	}

	response, err = s.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read REPLCONF capability response: %w", err)
	}

	if response.Type != resp.SimpleString || response.Str != "OK" {
		return fmt.Errorf("unexpected REPLCONF capability response: %v", response)
	}

	logger.Debug("Received OK for REPLCONF capability")

	// Step 4: Send PSYNC
	logger.Debug("Sending PSYNC")
	if err := s.sendCommand("PSYNC", "?", "-1"); err != nil {
		return fmt.Errorf("failed to send PSYNC: %w", err)
	}

	response, err = s.readResponse()
	if err != nil {
		return fmt.Errorf("failed to read PSYNC response: %w", err)
	}

	if response.Type != resp.SimpleString {
		return fmt.Errorf("unexpected PSYNC response type: %v", response.Type)
	}

	// Parse PSYNC response: +FULLRESYNC <runid> <offset>
	parts := strings.Fields(response.Str)
	if len(parts) != 3 || parts[0] != "FULLRESYNC" {
		return fmt.Errorf("unexpected PSYNC response format: %s", response.Str)
	}

	s.runID = parts[1]
	offset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset in PSYNC response: %w", err)
	}

	s.offset = offset
	logger.Infof("PSYNC successful: run_id=%s, offset=%d", s.runID, s.offset)

	// Step 5: Read RDB dump
	logger.Info("Starting to receive RDB dump from master")
	return s.receiveRDBDump()
}

// receiveRDBDump receives the RDB dump from the master
func (s *Slave) receiveRDBDump() error {
	// Read the RDB file size line - but it might contain RDB data too
	line, err := s.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB size: %w", err)
	}

	// Parse the bulk string format: $<size>\r\n<data>\r\n
	if !strings.HasPrefix(line, "$") {
		return fmt.Errorf("unexpected RDB size format: %s", line)
	}

	// Extract size from the line (before \r\n)
	parts := strings.Split(line, "\r\n")
	if len(parts) < 1 {
		return fmt.Errorf("malformed RDB size line: %s", line)
	}

	sizeStr := parts[0][1:] // Remove '$'
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid RDB size: %w", err)
	}

	logger.Infof("Receiving RDB dump of size: %d bytes", size)

	// Check if RDB data was included in the same line after \r\n
	var rdbData []byte
	var dataFromLine []byte

	if len(parts) > 1 && len(parts[1]) > 0 {
		// RDB data started in the same line
		dataFromLine = []byte(parts[1])
		logger.Debugf("Found %d bytes of RDB data in size line", len(dataFromLine))
	}

	// Read remaining RDB data
	rdbData = make([]byte, size)
	bytesRead := 0

	// Copy data from the size line first
	if len(dataFromLine) > 0 {
		copy(rdbData, dataFromLine)
		bytesRead = len(dataFromLine)
	}

	// Read remaining bytes if needed
	for bytesRead < int(size) {
		n, err := s.reader.Read(rdbData[bytesRead:])
		if err != nil {
			return fmt.Errorf("failed to read RDB data: %w", err)
		}
		bytesRead += n
	}

	logger.Infof("Received %d bytes of RDB data", bytesRead)

	// Load the RDB data into the local database
	debugLen := 20
	if len(rdbData) < debugLen {
		debugLen = len(rdbData)
	}
	logger.Debugf("About to load RDB data, first %d bytes: %x", debugLen, rdbData[:debugLen])
	if err := s.loadRDBData(rdbData); err != nil {
		logger.Errorf("Failed to load RDB data: %v", err)
		// Continue anyway, as the command stream will sync the data
	} else {
		logger.Info("RDB data loading completed successfully")
		// Check what was loaded
		keys := s.db.Keys()
		logger.Infof("Keys in database after RDB load: %d", len(keys))
		for _, key := range keys {
			if value, exists := s.db.Get(key); exists {
				logger.Debugf("Loaded key: %s = %s", key, value)
			}
		}
	}

	// Read the final \r\n after RDB data (if not already consumed)
	if len(dataFromLine) == 0 || len(dataFromLine) < int(size) {
		_, err = s.reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read final newline: %w", err)
		}
	}

	logger.Info("RDB dump received successfully, starting command stream")
	return s.startCommandStream()
}

// startCommandStream starts receiving commands from the master
func (s *Slave) startCommandStream() error {
	logger.Info("Starting command stream from master")

	for {
		select {
		case <-s.stopChan:
			logger.Info("Stopping command stream")
			return nil
		default:
			// Read command from master
			command, err := s.readCommand()
			if err != nil {
				logger.Errorf("Failed to read command from master: %v", err)
				return err
			}

			logger.Debugf("Received command from master: %s", command)
			s.offset += int64(len(command))
		}
	}
}

// sendCommand sends a command to the master
func (s *Slave) sendCommand(cmd string, args ...string) error {
	// Build RESP array
	array := make([]resp.Value, 1+len(args))
	array[0] = resp.Value{Type: resp.BulkString, Str: cmd}
	for i, arg := range args {
		array[i+1] = resp.Value{Type: resp.BulkString, Str: arg}
	}

	// Encode and send
	var buf strings.Builder
	if err := resp.Encode(&buf, resp.Value{Type: resp.Array, Array: array}); err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	_, err := s.writer.WriteString(buf.String())
	if err != nil {
		return fmt.Errorf("failed to write command: %w", err)
	}

	return s.writer.Flush()
}

// readResponse reads a response from the master
func (s *Slave) readResponse() (resp.Value, error) {
	return resp.Parse(s.reader)
}

// readCommand reads and executes a command from the master
func (s *Slave) readCommand() (string, error) {
	// Parse the command from the master
	command, err := resp.Parse(s.reader)
	if err != nil {
		return "", fmt.Errorf("failed to parse command: %w", err)
	}

	if command.Type != resp.Array {
		return "", fmt.Errorf("expected array command, got %v", command.Type)
	}

	if len(command.Array) == 0 {
		return "", fmt.Errorf("empty command array")
	}

	// Extract command name and arguments
	cmdName := command.Array[0].Str
	cmdNameUpper := strings.ToUpper(cmdName)
	args := command.Array[1:]

	logger.Debugf("Executing replicated command: %s with %d args", cmdName, len(args))

	// Execute the command on the local database
	switch cmdNameUpper {
	case "SET":
		if len(args) >= 2 {
			key := args[0].Str
			value := args[1].Str
			s.db.Set(key, value, time.Time{}) // No expiration for replicated SET
			logger.Debugf("Replicated SET %s = %s", key, value)
		}
	case "DEL":
		if len(args) >= 1 {
			key := args[0].Str
			s.db.Del(key)
			logger.Debugf("Replicated DEL %s", key)
		}
	case "EXPIRE":
		if len(args) >= 2 {
			key := args[0].Str
			ttl, err := strconv.ParseInt(args[1].Str, 10, 64)
			if err == nil {
				s.db.Expire(key, time.Duration(ttl)*time.Second)
				logger.Debugf("Replicated EXPIRE %s = %d", key, ttl)
			}
		}
	case "PEXPIRE":
		if len(args) >= 2 {
			key := args[0].Str
			ttl, err := strconv.ParseInt(args[1].Str, 10, 64)
			if err == nil {
				s.db.Expire(key, time.Duration(ttl)*time.Millisecond)
				logger.Debugf("Replicated PEXPIRE %s = %d", key, ttl)
			}
		}
	case "INCR":
		if len(args) >= 1 {
			key := args[0].Str
			currentValue, exists := s.db.Get(key)

			var newValue int64
			if !exists {
				// Key doesn't exist, start with 0
				newValue = 1
			} else {
				// Parse current value
				if currentInt, err := strconv.ParseInt(currentValue, 10, 64); err == nil {
					newValue = currentInt + 1
				} else {
					// If current value is not a valid integer, start with 1
					newValue = 1
				}
			}

			// Set the new value
			s.db.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
			logger.Debugf("Replicated INCR %s = %d", key, newValue)
		}
	case "DECR":
		if len(args) >= 1 {
			key := args[0].Str
			currentValue, exists := s.db.Get(key)

			var newValue int64
			if !exists {
				// Key doesn't exist, start with 0
				newValue = -1
			} else {
				// Parse current value
				if currentInt, err := strconv.ParseInt(currentValue, 10, 64); err == nil {
					newValue = currentInt - 1
				} else {
					// If current value is not a valid integer, start with -1
					newValue = -1
				}
			}

			// Set the new value
			s.db.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
			logger.Debugf("Replicated DECR %s = %d", key, newValue)
		}
	// List commands
	case "LPUSH":
		if len(args) >= 2 {
			key := args[0].Str
			elements := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				elements[i] = arg.Str
			}
			list := s.db.GetOrCreateList(key)
			list.LPush(elements...)
			logger.Debugf("Replicated LPUSH %s with %d elements", key, len(elements))
		}
	case "RPUSH":
		if len(args) >= 2 {
			key := args[0].Str
			elements := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				elements[i] = arg.Str
			}
			list := s.db.GetOrCreateList(key)
			list.RPush(elements...)
			logger.Debugf("Replicated RPUSH %s with %d elements", key, len(elements))
		}
	case "LPOP":
		if len(args) >= 1 {
			key := args[0].Str
			list := s.db.GetOrCreateList(key)
			list.LPop()
			logger.Debugf("Replicated LPOP %s", key)
		}
	case "RPOP":
		if len(args) >= 1 {
			key := args[0].Str
			list := s.db.GetOrCreateList(key)
			list.RPop()
			logger.Debugf("Replicated RPOP %s", key)
		}
	case "LREM":
		if len(args) >= 3 {
			key := args[0].Str
			count, err := strconv.Atoi(args[1].Str)
			if err == nil {
				value := args[2].Str
				list := s.db.GetOrCreateList(key)
				list.LRem(count, value)
				logger.Debugf("Replicated LREM %s count=%d value=%s", key, count, value)
			}
		}
	case "LTRIM":
		if len(args) >= 3 {
			key := args[0].Str
			start, err1 := strconv.Atoi(args[1].Str)
			stop, err2 := strconv.Atoi(args[2].Str)
			if err1 == nil && err2 == nil {
				list := s.db.GetOrCreateList(key)
				list.LTrim(start, stop)
				logger.Debugf("Replicated LTRIM %s [%d:%d]", key, start, stop)
			}
		}
	// Set commands
	case "SADD":
		if len(args) >= 2 {
			key := args[0].Str
			elements := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				elements[i] = arg.Str
			}
			set := s.db.GetOrCreateSet(key)
			set.SAdd(elements...)
			logger.Debugf("Replicated SADD %s with %d elements", key, len(elements))
		}
	case "SREM":
		if len(args) >= 2 {
			key := args[0].Str
			elements := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				elements[i] = arg.Str
			}
			set := s.db.GetOrCreateSet(key)
			set.SRem(elements...)
			logger.Debugf("Replicated SREM %s with %d elements", key, len(elements))
		}
	case "SPOP":
		if len(args) >= 1 {
			key := args[0].Str
			set := s.db.GetOrCreateSet(key)
			set.SPop()
			logger.Debugf("Replicated SPOP %s", key)
		}
	// Hash commands
	case "HSET":
		if len(args) >= 3 {
			key := args[0].Str
			hash := s.db.GetOrCreateHash(key)
			// Handle multiple field-value pairs
			for i := 1; i < len(args); i += 2 {
				if i+1 < len(args) {
					field := args[i].Str
					value := args[i+1].Str
					hash.HSet(field, value)
				}
			}
			logger.Debugf("Replicated HSET %s with %d field-value pairs", key, (len(args)-1)/2)
		}
	case "HDEL":
		if len(args) >= 2 {
			key := args[0].Str
			fields := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				fields[i] = arg.Str
			}
			hash := s.db.GetOrCreateHash(key)
			hash.HDel(fields...)
			logger.Debugf("Replicated HDEL %s with %d fields", key, len(fields))
		}
	case "HINCRBY":
		if len(args) >= 3 {
			key := args[0].Str
			field := args[1].Str
			increment, err := strconv.ParseInt(args[2].Str, 10, 64)
			if err == nil {
				hash := s.db.GetOrCreateHash(key)
				hash.HIncrBy(field, increment)
				logger.Debugf("Replicated HINCRBY %s %s += %d", key, field, increment)
			}
		}
	case "HINCRBYFLOAT":
		if len(args) >= 3 {
			key := args[0].Str
			field := args[1].Str
			increment, err := strconv.ParseFloat(args[2].Str, 64)
			if err == nil {
				hash := s.db.GetOrCreateHash(key)
				hash.HIncrByFloat(field, increment)
				logger.Debugf("Replicated HINCRBYFLOAT %s %s += %f", key, field, increment)
			}
		}
	// Sorted Set commands
	case "ZADD":
		if len(args) >= 3 {
			key := args[0].Str
			sortedSet := s.db.GetOrCreateSortedSet(key)
			// Handle multiple score-member pairs
			pairs := make(map[string]float64)
			for i := 1; i < len(args); i += 2 {
				if i+1 < len(args) {
					score, err := strconv.ParseFloat(args[i].Str, 64)
					if err == nil {
						member := args[i+1].Str
						pairs[member] = score
					}
				}
			}
			sortedSet.ZAdd(pairs)
			logger.Debugf("Replicated ZADD %s with %d score-member pairs", key, len(pairs))
		}
	case "ZPOPMIN":
		if len(args) >= 1 {
			key := args[0].Str
			count := 1 // Default to 1 if not specified
			if len(args) >= 2 {
				if parsedCount, err := strconv.Atoi(args[1].Str); err == nil {
					count = parsedCount
				}
			}
			sortedSet := s.db.GetOrCreateSortedSet(key)
			sortedSet.ZPopMin(count)
			logger.Debugf("Replicated ZPOPMIN %s count=%d", key, count)
		}
	case "ZREM":
		if len(args) >= 2 {
			key := args[0].Str
			members := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				members[i] = arg.Str
			}
			sortedSet := s.db.GetOrCreateSortedSet(key)
			sortedSet.ZRem(members...)
			logger.Debugf("Replicated ZREM %s with %d members", key, len(members))
		}
	case "FLUSHDB":
		// Clear all keys from the database
		keys := s.db.Keys()
		for _, key := range keys {
			s.db.Del(key)
		}
		logger.Debugf("Replicated FLUSHDB - cleared %d keys", len(keys))
	default:
		logger.Debugf("Replicated command not handled: %s", cmdName)
	}

	// Return the command as string for logging
	var buf strings.Builder
	if err := resp.Encode(&buf, command); err != nil {
		logger.Errorf("Failed to encode command for logging: %v", err)
	}
	return buf.String(), nil
}

// loadRDBData loads RDB data into the local database
func (s *Slave) loadRDBData(rdbData []byte) error {
	if len(rdbData) == 0 {
		logger.Debug("Empty RDB data, nothing to load")
		return nil
	}

	// Create a temporary file to load the RDB data
	tempFile, err := os.CreateTemp("", "gridhouse-rdb-*")
	if err != nil {
		return fmt.Errorf("failed to create temp RDB file: %w", err)
	}
	defer func() {
		if err := os.Remove(tempFile.Name()); err != nil {
			logger.Errorf("Failed to remove temp RDB file: %v", err)
		}
	}()
	defer func() {
		if err := tempFile.Close(); err != nil {
			logger.Errorf("Failed to close temp RDB file: %v", err)
		}
	}()

	// Write RDB data to temp file
	if _, err := tempFile.Write(rdbData); err != nil {
		return fmt.Errorf("failed to write RDB data to temp file: %w", err)
	}

	// Load RDB data using the v2 RDB reader
	reader, err := rdb.NewReader(tempFile.Name())
	if err != nil {
		return fmt.Errorf("failed to create RDB reader: %w", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Errorf("Failed to close RDB reader: %v", err)
		}
	}()

	// Read all data directly into the database
	if err := reader.ReadAll(s.db); err != nil {
		return fmt.Errorf("failed to read RDB data: %w", err)
	}

	logger.Info("RDB data loaded successfully into local database")
	return nil
}

// Stop stops the slave
func (s *Slave) Stop() {
	logger.Info("Stopping slave")
	close(s.stopChan)
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			logger.Errorf("Failed to close slave connection: %v", err)
		}
	}
}

// RunID returns the master's run ID
func (s *Slave) RunID() string {
	return s.runID
}

// Offset returns the current replication offset
func (s *Slave) Offset() int64 {
	return s.offset
}

// Role returns the replication role
func (s *Slave) Role() Role {
	return s.role
}
