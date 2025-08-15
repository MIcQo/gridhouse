package cmd

import (
	"bufio"
	"fmt"
	"gridhouse/internal/logger"
	"gridhouse/internal/repl"
	"gridhouse/internal/resp"
	"net"
	"strconv"
	"strings"
)

// ReplicationManager interface for replication commands
type ReplicationManager interface {
	Role() repl.Role
	RunID() string
	Offset() int64
	Stats() map[string]interface{}
	// Full replication protocol methods
	HandlePSync(replID string, offset int64) (string, int64, error)
	HandleReplConf(args []string) (string, error)
	HandleSync() error
	SetReplicaInfo(addr string, info map[string]string)
	GetReplicaInfo(addr string) map[string]string
}

// RDBGenerator interface for RDB generation
type RDBGenerator interface {
	GenerateRDBData() ([]byte, error)
}

// RoleHandler handles the ROLE command
func RoleHandler(manager ReplicationManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ROLE' command")
		}

		role := manager.Role()
		var roleStr string
		switch role {
		case repl.RoleMaster:
			roleStr = "master"
		case repl.RoleSlave:
			roleStr = "slave"
		default:
			roleStr = "unknown"
		}

		// Return array: [role, run_id, offset]
		return resp.Value{
			Type: resp.Array,
			Array: []resp.Value{
				{Type: resp.BulkString, Str: roleStr},
				{Type: resp.BulkString, Str: manager.RunID()},
				{Type: resp.Integer, Int: manager.Offset()},
			},
		}, nil
	}
}

// InfoReplicationHandler handles INFO replication
func InfoReplicationHandler(manager ReplicationManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// Only handle if exactly one argument and it's "replication"
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'INFO' command")
		}

		section := strings.ToLower(args[0].Str)
		if section != "replication" {
			return resp.Value{}, fmt.Errorf("ERR unsupported INFO section")
		}

		stats := manager.Stats()
		var lines []string

		// Add role information
		role := stats["role"].(repl.Role)
		switch role {
		case repl.RoleMaster:
			lines = append(lines, "role:master")
		case repl.RoleSlave:
			lines = append(lines, "role:slave")
		}

		// Add replication statistics
		lines = append(lines, fmt.Sprintf("run_id:%s", stats["run_id"]))
		lines = append(lines, fmt.Sprintf("offset:%d", stats["offset"]))
		lines = append(lines, fmt.Sprintf("backlog_size:%d", stats["backlog_size"]))
		lines = append(lines, fmt.Sprintf("backlog_capacity:%d", stats["backlog_capacity"]))
		lines = append(lines, fmt.Sprintf("replica_count:%d", stats["replica_count"]))
		lines = append(lines, fmt.Sprintf("connected_replicas:%d", stats["connected_replicas"]))

		// Join lines with \r\n
		info := strings.Join(lines, "\r\n") + "\r\n"
		return resp.Value{Type: resp.BulkString, Str: info}, nil
	}
}

// PSyncHandler handles the PSYNC command
func PSyncHandler(manager ReplicationManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 || len(args) > 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'PSYNC' command")
		}

		replID := args[0].Str
		offset := int64(0)
		if len(args) == 2 {
			var err error
			offset, err = strconv.ParseInt(args[1].Str, 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR invalid offset")
			}
		}

		logger.Infof("PSYNC request: replID=%s, offset=%d", replID, offset)
		runID, newOffset, err := manager.HandlePSync(replID, offset)
		if err != nil {
			return resp.Value{}, err
		}

		// Return: +FULLRESYNC <runid> <offset>
		response := fmt.Sprintf("FULLRESYNC %s %d", runID, newOffset)
		return resp.Value{Type: resp.SimpleString, Str: response}, nil
	}
}

// ReplicaRegistrar interface for registering replica connections
type ReplicaRegistrar interface {
	RegisterReplica(conn net.Conn) error
}

// PSyncHandlerWithRDB handles the PSYNC command and sends RDB data
func PSyncHandlerWithRDB(manager ReplicationManager, rdbGen RDBGenerator, registrar ReplicaRegistrar) func(args []resp.Value, conn net.Conn, writer *bufio.Writer) error {
	return func(args []resp.Value, conn net.Conn, writer *bufio.Writer) error {
		if len(args) < 1 || len(args) > 2 {
			return fmt.Errorf("ERR wrong number of arguments for 'PSYNC' command")
		}

		replID := args[0].Str
		offset := int64(0)
		if len(args) == 2 {
			var err error
			offset, err = strconv.ParseInt(args[1].Str, 10, 64)
			if err != nil {
				return fmt.Errorf("ERR invalid offset")
			}
		}

		logger.Infof("PSYNC request: replID=%s, offset=%d", replID, offset)
		runID, newOffset, err := manager.HandlePSync(replID, offset)
		if err != nil {
			return err
		}

		// Send FULLRESYNC response
		response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", runID, newOffset)
		if _, err := writer.WriteString(response); err != nil {
			return fmt.Errorf("failed to write FULLRESYNC response: %w", err)
		}
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush FULLRESYNC response: %w", err)
		}

		logger.Info("Sent FULLRESYNC response, generating RDB data...")

		// Generate RDB data
		rdbData, err := rdbGen.GenerateRDBData()
		if err != nil {
			logger.Errorf("Failed to generate RDB data: %v", err)
			// Send empty RDB as fallback
			rdbData = []byte{}
		}

		// Send RDB data as bulk string: $<size>\r\n<data>\r\n
		rdbSize := len(rdbData)
		logger.Infof("Sending RDB data of size: %d bytes", rdbSize)

		if _, err := fmt.Fprintf(writer, "$%d\r\n", rdbSize); err != nil {
			return fmt.Errorf("failed to write RDB size: %w", err)
		}

		if rdbSize > 0 {
			if _, err := writer.Write(rdbData); err != nil {
				return fmt.Errorf("failed to write RDB data: %w", err)
			}
		}

		if _, err := writer.WriteString("\r\n"); err != nil {
			return fmt.Errorf("failed to write RDB terminator: %w", err)
		}

		if err := writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush RDB data: %w", err)
		}

		logger.Info("RDB data sent successfully to replica")

		// Register this connection for ongoing command replication
		if err := registrar.RegisterReplica(conn); err != nil {
			logger.Errorf("Failed to register replica for ongoing replication: %v", err)
			// Don't return error as RDB sync was successful
		} else {
			logger.Infof("Registered replica %s for ongoing command replication", conn.RemoteAddr())
		}

		return nil
	}
}

// ReplConfHandler handles the REPLCONF command
func ReplConfHandler(manager ReplicationManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'REPLCONF' command")
		}

		// Convert args to string slice
		argStrings := make([]string, len(args))
		for i, arg := range args {
			argStrings[i] = arg.Str
		}

		result, err := manager.HandleReplConf(argStrings)
		if err != nil {
			return resp.Value{}, err
		}

		return resp.Value{Type: resp.SimpleString, Str: result}, nil
	}
}

// SyncHandler handles the SYNC command (legacy)
func SyncHandler(manager ReplicationManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SYNC' command")
		}

		err := manager.HandleSync()
		if err != nil {
			return resp.Value{}, err
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// RegisterReplicationCommands registers replication-related commands
func RegisterReplicationCommands(registry *Registry, manager ReplicationManager) {
	registry.Register(&Command{
		Name:     "ROLE",
		Arity:    0,
		Handler:  RoleHandler(manager),
		ReadOnly: true,
	})

	// Full replication protocol commands
	registry.Register(&Command{
		Name:     "PSYNC",
		Arity:    -1,
		Handler:  PSyncHandler(manager),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "REPLCONF",
		Arity:    -1,
		Handler:  ReplConfHandler(manager),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "SYNC",
		Arity:    0,
		Handler:  SyncHandler(manager),
		ReadOnly: true,
	})
}
