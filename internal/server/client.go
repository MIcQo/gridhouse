package server

import (
	"bufio"
	"fmt"
	"gridhouse/internal/resp"
	"net"
	"strconv"
	"sync"
)

// QueuedCommand represents a command queued during a transaction
type QueuedCommand struct {
	Command string
	Args    []string
}

// Client represents a client connection with buffered I/O and transaction state
type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	server *Server
	connID string

	// Auth state
	authed bool

	// Transaction state
	txMode     bool
	queuedCmds []QueuedCommand

	// Performance: Reusable response buffer
	responseBuf *[]byte

	// Per-client writer mutex to minimize lock contention scope
	writerMu sync.Mutex
}

// newClient creates a new client instance
func newClient(conn net.Conn, server *Server, connID string) *Client {
	responseBuf := responsePool.Get().(*[]byte)
	*responseBuf = (*responseBuf)[:0] // Reset buffer

	return &Client{
		conn:        conn,
		reader:      bufio.NewReaderSize(conn, server.cfg.ReadBuffer),
		writer:      bufio.NewWriterSize(conn, server.cfg.WriteBuffer),
		server:      server,
		connID:      connID,
		authed:      server.cfg.Password == "",
		txMode:      false,
		queuedCmds:  make([]QueuedCommand, 0),
		responseBuf: responseBuf,
	}
}

// readCommand reads and parses a single command from the client
func (c *Client) readCommand() (string, []string, error) {
	command, args, err := resp.UltraParseCommand(c.reader, 10000)
	if err != nil {
		return "", nil, err
	}
	return command, args, nil
}

// writeResponse writes a response without flushing
func (c *Client) writeResponse(response resp.Value) error {
	return resp.UltraEncode(c.writer, response)
}

// writeResponseOK writes an OK response without flushing
func (c *Client) writeResponseOK() error {
	return resp.UltraEncodeOK(c.writer)
}

// writeResponseError writes an error response without flushing
func (c *Client) writeResponseError(errMsg string) error {
	return resp.UltraEncodeError(c.writer, errMsg)
}

// flush flushes the write buffer
func (c *Client) flush() error {
	return c.writer.Flush()
}

// writeFullBuffer ensures all bytes written with minimal lock scope
func (c *Client) writeFullBuffer(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// Lock only around writes, not command processing
	c.writerMu.Lock()
	defer c.writerMu.Unlock()

	// Guarantee full write with retry loop
	written := 0
	for written < len(data) {
		n, err := c.writer.Write(data[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

// flushProtected flushes with mutex protection
func (c *Client) flushProtected() error {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	return c.writer.Flush()
}

// writeAndFlush writes a response and flushes immediately
func (c *Client) writeAndFlush(response resp.Value) error {
	if err := c.writeResponse(response); err != nil {
		return err
	}
	return c.flush()
}

// writeAndFlushOK writes an OK response and flushes immediately
func (c *Client) writeAndFlushOK() error {
	if err := c.writeResponseOK(); err != nil {
		return err
	}
	return c.flush()
}

// writeAndFlushError writes an error response and flushes immediately
func (c *Client) writeAndFlushError(errMsg string) error {
	if err := c.writeResponseError(errMsg); err != nil {
		return err
	}
	return c.flush()
}

func (c *Client) writeRawAndFlush(response []byte) error {
	if _, err := c.writer.Write(response); err != nil {
		return err
	}
	return c.flush()
}

// buildBulkStringResponse builds bulk string response in buffer
func (c *Client) buildBulkStringResponse(value string, isNull bool) []byte {
	buf := *c.responseBuf
	buf = buf[:0] // Reset

	if isNull {
		buf = append(buf, "$-1\r\n"...)
	} else {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(value)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, value...)
		buf = append(buf, '\r', '\n')
	}

	*c.responseBuf = buf
	return buf
}

// buildSimpleStringResponse builds simple string response in buffer
func (c *Client) buildSimpleStringResponse(value string) []byte {
	buf := *c.responseBuf
	buf = buf[:0] // Reset

	buf = append(buf, '+')
	buf = append(buf, value...)
	buf = append(buf, '\r', '\n')

	*c.responseBuf = buf
	return buf
}

// beginTransaction starts a transaction
func (c *Client) beginTransaction() error {
	if c.txMode {
		return fmt.Errorf("MULTI calls can not be nested")
	}
	c.txMode = true
	c.queuedCmds = c.queuedCmds[:0] // Clear any existing commands
	return nil
}

// queueCommand queues a command during a transaction
func (c *Client) queueCommand(command string, args []string) {
	c.queuedCmds = append(c.queuedCmds, QueuedCommand{
		Command: command,
		Args:    args,
	})
}

// execTransaction executes all queued commands and exits transaction mode
func (c *Client) execTransaction() error {
	if !c.txMode {
		return fmt.Errorf("EXEC without MULTI")
	}

	// Execute all queued commands
	results := make([]resp.Value, len(c.queuedCmds))
	for i, queuedCmd := range c.queuedCmds {
		result, err := c.executeCommand(queuedCmd.Command, queuedCmd.Args)
		if err != nil {
			results[i] = resp.Value{Type: resp.Error, Str: err.Error()}
		} else {
			results[i] = result
		}
	}

	// Write all responses
	response := resp.Value{Type: resp.Array, Array: results}
	if err := c.writeResponse(response); err != nil {
		return err
	}

	// Exit transaction mode
	c.txMode = false
	c.queuedCmds = c.queuedCmds[:0]

	// Use protected flush for transactions
	return c.flushProtected()
}

// discardTransaction discards all queued commands and exits transaction mode
func (c *Client) discardTransaction() error {
	if !c.txMode {
		return fmt.Errorf("DISCARD without MULTI")
	}
	c.txMode = false
	c.queuedCmds = c.queuedCmds[:0]
	return nil
}

// isPipelineCommand checks if more commands are pending in the buffer
func (c *Client) isPipelineCommand() bool {
	// If there's already buffered data, this is definitely a pipeline
	if c.reader.Buffered() > 0 {
		return true
	}
	// No buffered data means no more commands in pipeline
	return false
}

// executeCommand executes a single command and returns the result
func (c *Client) executeCommand(command string, args []string) (resp.Value, error) {
	// Convert args to resp.Value format for registry
	respArgs := make([]resp.Value, len(args))
	for i, arg := range args {
		respArgs[i] = resp.Value{Type: resp.BulkString, Str: arg}
	}

	// Try ultra-fast path for common commands first
	switch command {
	default:
		// Use registry for other commands
		response, err := c.server.registry.Execute(command, respArgs)
		if err != nil {
			return resp.Value{}, err
		}
		return response, nil
	}
}
