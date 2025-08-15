package server

import (
	"bytes"
	"fmt"
	"gridhouse/internal/cmd"
	"gridhouse/internal/logger"
	"gridhouse/internal/persistence"
	"gridhouse/internal/repl"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Response buffer pool for zero-copy - HUGE buffers for pipelines
var responsePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 64*1024) // 64KB initial capacity for large pipelines
		return &buf
	},
}

// Pipeline response buffer pool for massive pipelines
var pipelineResponsePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 2*1024*1024) // 2MB for 10K command responses
		return &buf
	},
}

type Config struct {
	Addr        string
	Persistence *persistence.Config
	Password    string // Optional password for AUTH command
	SlaveOf     string // Master address for replication
	WriteBuffer int
	ReadBuffer  int
}

type Server struct {
	cfg         Config
	ln          net.Listener
	addr        string
	db          store.DataStore
	registry    *cmd.Registry
	persist     *persistence.Manager
	stats       *ServerStats
	slave       *repl.Slave   // Slave instance for replication
	replManager *repl.Manager // Replication manager

	// Connection management
	connSemaphore chan struct{} // Semaphore to limit concurrent connections

	// Memory management
	memoryLimit int64 // Memory limit in bytes
	activeConns int32 // Atomic counter for active connections

	// Transaction management
	tm *cmd.TransactionManager // Transaction manager for ACID compliance
}

func New(cfg Config) *Server {
	// Use ultra-optimized DB for maximum performance
	db := store.NewUltraOptimizedDB()
	registry := cmd.NewRegistry()
	cmd.RegisterOptimizedCommands(registry, db)

	// Initialize transaction manager for ACID compliance
	tm := cmd.NewTransactionManager(db)

	// Extract port from address for stats
	port := 6380 // default
	if cfg.Addr != "" {
		// Parse port from address like ":6381" or "127.0.0.1:6381"
		if strings.Contains(cfg.Addr, ":") {
			parts := strings.Split(cfg.Addr, ":")
			if len(parts) > 1 {
				if parsedPort, err := fmt.Sscanf(parts[len(parts)-1], "%d", &port); err != nil || parsedPort != 1 {
					port = 6380 // fallback to default
				}
			}
		}
	}

	// Initialize server stats
	stats := NewServerStats(port, nil)

	server := &Server{
		cfg:           cfg,
		db:            db,
		registry:      registry,
		stats:         stats,
		tm:            tm,                         // Transaction manager for ACID compliance
		connSemaphore: make(chan struct{}, 50000), // Increased to 50K connections
		// REMOVED: workerPool - eliminated to prevent scaling bottleneck
		memoryLimit: 4 * 1024 * 1024 * 1024, // 4GB memory limit for large test runs
		activeConns: 0,
	}

	// Initialize persistence if configured
	if cfg.Persistence != nil {
		persist, err := persistence.NewManager(cfg.Persistence, db)
		if err == nil {
			server.persist = persist
			// Load existing data
			if err := persist.LoadData(); err != nil {
				// Log error but continue
				// In production, you might want to handle this differently
				_ = err // Suppress unused variable warning
			}
			// Register persistence commands
			cmd.RegisterPersistenceCommands(registry, persist, db)
		}
	}

	// Determine role based on configuration
	role := repl.RoleMaster
	if cfg.SlaveOf != "" {
		role = repl.RoleSlave
	}

	// Initialize replication manager with correct role
	replManager := repl.NewManager(role, 1024*1024) // 1MB backlog
	server.replManager = replManager

	// Register server commands (INFO, AUTH) with dynamic stats
	cmd.RegisterServerCommands(registry, stats, replManager, cfg.Password)
	cmd.RegisterReplicationCommands(registry, replManager)

	// If configured as slave, start replication
	if cfg.SlaveOf != "" {
		logger.Infof("Starting as slave, connecting to master at %s", cfg.SlaveOf)
		server.slave = repl.NewSlave(cfg.SlaveOf, db)
		go func() {
			if err := server.slave.Connect(); err != nil {
				logger.Errorf("Failed to connect to master: %v", err)
			}
		}()
	}

	return server
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		logger.Errorf("Failed to start server on %s: %v", s.cfg.Addr, err)
		return err
	}
	s.ln = ln
	s.addr = ln.Addr().String()
	logger.Infof("Server listening on %s", s.addr)
	go s.serve()
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	return nil
}

func (s *Server) Addr() string { return s.addr }

// shouldRejectConnection checks if we should reject connection
func (s *Server) shouldRejectConnection() bool {
	// Check memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Reject if memory usage exceeds limit
	// if m.Alloc > uint64(s.memoryLimit) {
	// 	logger.Warnf("Memory limit exceeded: %d MB", m.Alloc/(1024*1024))
	// 	return true
	// }

	// Reject if too many active connections
	if atomic.LoadInt32(&s.activeConns) > 8000 {
		logger.Warnf("Too many active connections: %d", atomic.LoadInt32(&s.activeConns))
		return true
	}

	return false
}

func (s *Server) Close() error {
	logger.Info("Closing server...")

	if s.slave != nil {
		logger.Debug("Stopping slave replication")
		s.slave.Stop()
	}

	if s.persist != nil {
		if err := s.persist.Close(); err != nil {
			logger.Errorf("Failed to close persistence manager: %v", err)
		}
	}
	if s.db != nil {
		s.db.Close()
	}
	if s.ln != nil {
		logger.Info("Server closed successfully")
		return s.ln.Close()
	}
	return nil
}

func (s *Server) serve() {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("Panic in serve loop: %v", r)
		}
	}()

	for {
		c, err := s.ln.Accept()
		if err != nil {
			logger.Debugf("Failed to accept connection: %v", err)
			return
		}

		// Check memory usage and circuit breaker
		if s.shouldRejectConnection() {
			logger.Warnf("Memory pressure or high load, rejecting connection from %s", c.RemoteAddr())
			c.Close()
			continue
		}

		// Check connection limit
		select {
		case s.connSemaphore <- struct{}{}:
			// Connection accepted
			atomic.AddInt32(&s.activeConns, 1)
			logger.Debugf("Connection accepted from %s (active: %d)", c.RemoteAddr(), atomic.LoadInt32(&s.activeConns))
		default:
			// Connection limit reached, close connection
			logger.Warnf("Connection limit reached, rejecting connection from %s", c.RemoteAddr())
			c.Close()
			continue
		}

		// Track new connection
		s.stats.IncrementConnectedClients()
		s.stats.IncrementConnectionsReceived()

		logger.Debugf("New connection accepted from %s", c.RemoteAddr())

		// Wrap connection with tracking and unique ID
		trackedConn := newConnectionTracker(c, s.stats)

		// Direct goroutine handling - eliminates worker pool bottleneck
		go func(conn net.Conn) {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("Panic in connection handler: %v", r)
				}
				atomic.AddInt32(&s.activeConns, -1)
				<-s.connSemaphore // Release connection slot only
			}()
			s.handle(conn)
		}(trackedConn)
	}
}

// handle is the main connection handler using Client struct
func (s *Server) handle(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("Panic in connection handler: %v", r)
			debug.PrintStack()
		}
		logger.Debugf("Connection closed from %s", conn.RemoteAddr())
		conn.Close()
	}()

	// Get connection ID for transaction management
	var connID string
	if tracker, ok := conn.(*connectionTracker); ok {
		connID = tracker.ID()
	} else {
		// Fallback for non-tracked connections
		connID = conn.RemoteAddr().String()
	}

	// Set TCP options for performance
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true) // Disable Nagle's algorithm for low latency
		tcpConn.SetKeepAlive(true)
		tcpConn.SetReadBuffer(s.cfg.ReadBuffer)   // Increase read buffer
		tcpConn.SetWriteBuffer(s.cfg.WriteBuffer) // Increase write buffer
	}

	// Create client instance
	client := newClient(conn, s, connID)

	defer func() {
		// Return response buffer to pool when client connection ends
		if client.responseBuf != nil {
			responsePool.Put(client.responseBuf)
		}
	}()

	for {
		// Check if in transaction mode
		if client.txMode {
			// Read next command
			command, args, err := client.readCommand()
			if err != nil {
				logger.Debugf("Parse error in transaction: %v", err)
				break
			}

			// Handle transaction commands
			switch command {
			case "EXEC", "exec":
				if len(args) != 0 {
					if err := client.writeAndFlushError("ERR wrong number of arguments for 'EXEC' command"); err != nil {
						break
					}
				} else {
					if err := client.execTransaction(); err != nil {
						if err := client.writeAndFlushError("ERR " + err.Error()); err != nil {
							break
						}
					}
				}
			case "DISCARD", "discard":
				if len(args) != 0 {
					if err := client.writeAndFlushError("ERR wrong number of arguments for 'DISCARD' command"); err != nil {
						break
					}
				} else {
					if err := client.discardTransaction(); err != nil {
						if err := client.writeAndFlushError("ERR " + err.Error()); err != nil {
							break
						}
					} else {
						if err := client.writeAndFlushOK(); err != nil {
							break
						}
					}
				}
			default:
				// Queue the command and respond with QUEUED
				client.queueCommand(command, args)
				if err := client.writeResponse(resp.Value{Type: resp.SimpleString, Str: "QUEUED"}); err != nil {
					break
				}
				// Must flush QUEUED response immediately
				logger.Debugf("Flushing QUEUED response for command: %s", command)
				if err := client.flush(); err != nil {
					logger.Debugf("Failed to flush QUEUED response: %v", err)
					break
				}
			}
			continue
		}

		// Not in transaction mode - read first command
		command, args, err := client.readCommand()
		if err != nil {
			// logger.Errorf("Parse error: %v", err)
			logger.Debugf("Parse error: %v", err)
			break
		}

		// AUTH handling and NOAUTH enforcement
		if client.server.cfg.Password != "" {
			// Only allow AUTH until authenticated
			if strings.EqualFold(command, "AUTH") {
				// AUTH arity check and validation
				if len(args) < 1 {
					if err := client.writeAndFlushError("ERR wrong number of arguments for 'AUTH' command"); err != nil {
						break
					}
					continue
				}

				var pass = args[len(args)-1]

				// If no password configured, accept any (for completeness)
				if client.server.cfg.Password == "" || pass == client.server.cfg.Password {
					client.authed = true
					if err := client.writeAndFlushOK(); err != nil {
						break
					}
				} else {
					if err := client.writeAndFlushError("ERR invalid password"); err != nil {
						break
					}
				}
				continue
			}
			if !client.authed {
				// Reject any other command until AUTH succeeds
				if err := client.writeAndFlushError("NOAUTH Authentication required."); err != nil {
					break
				}
				continue
			}
		}

		// Check for transaction and replication commands
		switch command {
		case "PSYNC", "psync":
			if err := s.handlePSyncCommand(client, args); err != nil {
				logger.Errorf("PSYNC command failed: %v", err)
				if err := client.writeAndFlushError("ERR " + err.Error()); err != nil {
					break
				}
			}
			continue
		case "MULTI", "multi":
			if len(args) != 0 {
				if err := client.writeAndFlushError("ERR wrong number of arguments for 'MULTI' command"); err != nil {
					break
				}
			} else {
				if err := client.beginTransaction(); err != nil {
					if err := client.writeAndFlushError("ERR " + err.Error()); err != nil {
						break
					}
				} else {
					if err := client.writeAndFlushOK(); err != nil {
						break
					}
				}
			}
			continue
		case "EXEC", "exec":
			if err := client.writeAndFlushError("ERR EXEC without MULTI"); err != nil {
				break
			}
			continue
		case "DISCARD", "discard":
			if err := client.writeAndFlushError("ERR DISCARD without MULTI"); err != nil {
				break
			}
			continue
		}

		// Check if this is a pipeline (more commands pending)
		if client.isPipelineCommand() {
			// Pipeline mode: read all commands first, then execute
			commands := []struct {
				command string
				args    []string
			}{{command, args}}

			// Get dedicated pipeline response buffer
			pb := pipelineResponsePool.Get().(*[]byte)
			*pb = (*pb)[:0] // Reset
			defer pipelineResponsePool.Put(pb)
			var pipelineBuf = *pb

			// var pipelineBuf = make([]byte, 0, 2*1024*1024)

			// Track error count for debugging
			var errorResponses = 0

			// Read remaining commands in pipeline
			// Handle parse errors properly
			for {
				if !client.isPipelineCommand() {
					logger.Debugf("No more commands in pipeline, proceeding with execution")
					break
				}
				cmd, cmdArgs, err := client.readCommand()
				if err != nil {
					if err == io.EOF {
						logger.Debugf("EOF reached while reading pipeline commands")
						break
					}
					logger.Debugf("Pipeline parse error: %v, generating error response", err)
					// Generate error response for malformed command and append to buffer
					errorResponseBytes := []byte("-" + err.Error() + "\r\n")
					pipelineBuf = append(pipelineBuf, errorResponseBytes...)
					errorResponses++

					// Try to clean up any remaining malformed data in the buffer
					// This prevents the reader from being left in a bad state
					if client.reader.Buffered() > 0 {
						// Read up to 1024 bytes or until newline to clear malformed data
						discarded := make([]byte, min(client.reader.Buffered(), 1024))
						client.reader.Read(discarded)
						logger.Debugf("Discarded %d bytes of malformed data", len(discarded))
					}
					break
				}
				commands = append(commands, struct {
					command string
					args    []string
				}{cmd, cmdArgs})
				logger.Debugf("Added command to pipeline: %s (total: %d)", cmd, len(commands))
			}

			// Execute all commands and write responses directly to buffer
			for _, cmd := range commands {
				// Execute command using ultra-fast path when possible
				var responseBytes []byte
				var err error
				isWriteCommand := false

				// Ultra-fast response generation for common commands
				switch cmd.command {
				case "SET", "set":
					if len(cmd.args) >= 2 {
						key, value := cmd.args[0], cmd.args[1]
						expiration := time.Time{}
						// Handle TTL options quickly
						if len(cmd.args) >= 4 {
							switch cmd.args[2] {
							case "EX", "ex":
								if seconds, parseErr := strconv.ParseInt(cmd.args[3], 10, 64); parseErr == nil {
									expiration = time.Now().Add(time.Duration(seconds) * time.Second)
								}
							case "PX", "px":
								if ms, parseErr := strconv.ParseInt(cmd.args[3], 10, 64); parseErr == nil {
									expiration = time.Now().Add(time.Duration(ms) * time.Millisecond)
								}
							}
						}
						client.server.db.Set(key, value, expiration)
						responseBytes = resp.OkResponse
						isWriteCommand = true
					} else {
						responseBytes = []byte("-ERR wrong number of arguments for 'SET' command\r\n")
						errorResponses++
					}
				case "GET", "get":
					if len(cmd.args) == 1 {
						value, exists := client.server.db.Get(cmd.args[0])
						if exists {
							// Build bulk string response directly
							responseBytes = append(responseBytes, '$')
							responseBytes = strconv.AppendInt(responseBytes, int64(len(value)), 10)
							responseBytes = append(responseBytes, '\r', '\n')
							responseBytes = append(responseBytes, value...)
							responseBytes = append(responseBytes, '\r', '\n')
						} else {
							responseBytes = []byte("$-1\r\n")
						}
					} else {
						responseBytes = []byte("-ERR wrong number of arguments for 'GET' command\r\n")
						errorResponses++
					}
				case "PING", "ping":
					if len(cmd.args) == 0 {
						responseBytes = []byte("+PONG\r\n")
					} else {
						responseBytes = append(responseBytes, '+')
						responseBytes = append(responseBytes, cmd.args[0]...)
						responseBytes = append(responseBytes, '\r', '\n')
					}
				default:
					respArgs := make([]resp.Value, len(cmd.args))
					for i, arg := range cmd.args {
						respArgs[i] = resp.Value{Type: resp.BulkString, Str: arg}
					}

					// Fallback to generic execution
					result, execErr := s.registry.Execute(cmd.command, respArgs)
					err = execErr
					if err != nil {
						responseBytes = []byte("-ERR " + err.Error() + "\r\n")
						errorResponses++
					} else {
						// Convert resp.Value to bytes efficiently
						var respBuf bytes.Buffer
						resp.UltraEncode(&respBuf, result)
						responseBytes = respBuf.Bytes()

						// Check if write command for AOF
						if cmdInfo, exists := s.registry.Get(cmd.command); exists && !cmdInfo.ReadOnly {
							isWriteCommand = true
						}
					}
				}

				// Append response directly to pipeline buffer
				pipelineBuf = append(pipelineBuf, responseBytes...)

				// Async AOF logging for write commands
				if isWriteCommand && s.persist != nil {
					if err := s.persist.AppendMultiCommands(cmd.command, cmd.args); err != nil {
						logger.Error(err)
					}
				}

				// Forward write commands to replicas for ongoing replication (pipeline)
				if isWriteCommand && s.replManager.Count() > 0 && s.replManager.Role() == repl.RoleMaster {
					logger.Debugf("Forwarding pipeline write command %s to replicas", cmd.command)
					// Encode command as RESP array for replication
					respArray := make([]resp.Value, 1+len(cmd.args))
					respArray[0] = resp.Value{Type: resp.BulkString, Str: cmd.command}
					for i, arg := range cmd.args {
						respArray[i+1] = resp.Value{Type: resp.BulkString, Str: arg}
					}

					var buf strings.Builder
					if err := resp.Encode(&buf, resp.Value{Type: resp.Array, Array: respArray}); err == nil {
						// Forward to all connected replicas
						s.replManager.AppendCommand([]byte(buf.String()))
						logger.Debugf("Successfully forwarded pipeline write command %s to replicas", cmd.command)
					} else {
						logger.Errorf("Failed to encode pipeline command for replication: %v", err)
					}
				}
			}

			// Single write of entire pipeline response buffer
			logger.Debugf("Flushing pipeline with %d commands (%d errors), total response: %d bytes", len(commands), errorResponses, len(pipelineBuf))

			// Write entire pipeline response with guaranteed full write and minimal lock scope
			if len(pipelineBuf) > 0 {
				if s.persist != nil {
					s.persist.FlushMultiCommand()
				}

				if err := client.writeFullBuffer(pipelineBuf); err != nil {
					logger.Warnf("Pipeline write error: %v", err)
					break
				}
			}

			// Single protected flush for entire pipeline
			if err := client.flushProtected(); err != nil {
				logger.Warnf("Pipeline flush error: %v", err)
				break
			}
		} else {
			// Single command mode: execute and flush immediately
			logger.Debugf("Executing single command via registry: %s", command)

			// Ultra-fast path for most common commands
			var fastPathErr error
			isWriteCommand := false

			switch command {
			case "PING", "ping":
				fastPathErr = client.executePingCommandFast(args)
			case "GET", "get":
				fastPathErr = client.executeGetCommandFast(args)
			case "SET", "set":
				fastPathErr = client.executeSetCommandFast(args)
				isWriteCommand = true
			default:
				respArgs := make([]resp.Value, len(args))
				for i, arg := range args {
					respArgs[i] = resp.Value{Type: resp.BulkString, Str: arg}
				}
				// Fallback to generic path
				result, err := s.registry.Execute(command, respArgs)
				if err != nil {
					logger.Debugf("Single command error, flushing error response")
					fastPathErr = client.writeAndFlushError("ERR " + err.Error())
				} else {
					logger.Debugf("Single command success, flushing result")
					fastPathErr = client.writeAndFlush(result)

					// Check if it's a write command for AOF
					if cmdInfo, exists := s.registry.Get(command); exists && !cmdInfo.ReadOnly {
						isWriteCommand = true
						logger.Debugf("Detected write command: %s (ReadOnly: %v)", command, cmdInfo.ReadOnly)
					} else {
						logger.Debugf("Command %s is read-only or not found (exists: %v)", command, exists)
					}
				}
			}

			if fastPathErr != nil {
				break
			}

			// Log to AOF if it's a write command that succeeded (ASYNC)
			if isWriteCommand && s.persist != nil {
				// PERFORMANCE FIX: Make AOF logging asynchronous to avoid blocking
				go func(cmd string, args []string) {
					if err := s.persist.AppendCommand(cmd, args); err != nil {
						logger.Error(err)
					}
				}(command, args)
			}

			// Forward write commands to replicas for ongoing replication
			logger.Debugf("Checking replication forwarding: isWriteCommand=%v, role=%v", isWriteCommand, s.replManager.Role())
			if isWriteCommand && s.replManager.Count() > 0 && s.replManager.Role() == repl.RoleMaster {
				logger.Debugf("Forwarding write command %s to replicas", command)
				// Encode command as RESP array for replication
				respArray := make([]resp.Value, 1+len(args))
				respArray[0] = resp.Value{Type: resp.BulkString, Str: command}
				for i, arg := range args {
					respArray[i+1] = resp.Value{Type: resp.BulkString, Str: arg}
				}

				var buf strings.Builder
				if err := resp.Encode(&buf, resp.Value{Type: resp.Array, Array: respArray}); err == nil {
					// Forward to all connected replicas
					s.replManager.AppendCommand([]byte(buf.String()))
					logger.Debugf("Successfully forwarded write command %s to replicas", command)
				} else {
					logger.Errorf("Failed to encode command for replication: %v", err)
				}
			}
		}
	}
}

// handlePSyncCommand handles PSYNC command specially to send RDB data
func (s *Server) handlePSyncCommand(client *Client, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("wrong number of arguments for 'PSYNC' command")
	}

	// Convert args to resp.Value for replication manager
	respArgs := make([]resp.Value, len(args))
	for i, arg := range args {
		respArgs[i] = resp.Value{Type: resp.BulkString, Str: arg}
	}

	// Use the special PSYNC handler that sends RDB data
	if s.persist != nil {
		psyncHandler := cmd.PSyncHandlerWithRDB(s.replManager, s.persist, s.replManager)
		return psyncHandler(respArgs, client.conn, client.writer)
	} else {
		// Fallback to regular PSYNC if no persistence
		result, err := s.registry.Execute("PSYNC", respArgs)
		if err != nil {
			return err
		}
		return client.writeAndFlush(result)
	}
}
