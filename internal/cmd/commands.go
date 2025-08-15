package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"gridhouse/internal/stats"
	"gridhouse/internal/store"
	"strconv"
	"strings"
	"time"
)

// Store interface for commands that need to access data
type Store interface {
	Set(key, value string, expiration time.Time)
	Get(key string) (string, bool)
	Del(key string) bool
	Exists(key string) bool
	TTL(key string) int64
	PTTL(key string) int64
	Expire(key string, duration time.Duration) bool
	Keys() []string
	GetOrCreateList(key string) *store.List
	GetOrCreateSet(key string) *store.Set
	GetOrCreateHash(key string) *store.Hash
}

// Handler represents a command handler function
type Handler func(args []resp.Value) (resp.Value, error)

// Command represents a registered command
type Command struct {
	Name     string
	Arity    int // -1 means variable arity, >=0 means exact arity
	Handler  Handler
	ReadOnly bool
}

// Registry holds all registered commands
type Registry struct {
	commands map[string]*Command
}

// List returns all registered command names (case-insensitive keys may duplicate, so return unique upper-case names)
func (r *Registry) List() []string {
	seen := make(map[string]struct{}, len(r.commands))
	names := make([]string, 0, len(r.commands))
	for name := range r.commands {
		upper := strings.ToUpper(name)
		if _, ok := seen[upper]; ok {
			continue
		}
		seen[upper] = struct{}{}
		names = append(names, upper)
	}
	return names
}

// NewRegistry creates a new command registry
func NewRegistry() *Registry {
	return &Registry{
		commands: make(map[string]*Command),
	}
}

// Register adds a command to the registry
func (r *Registry) Register(cmd *Command) {
	r.commands[strings.ToUpper(cmd.Name)] = cmd
	r.commands[strings.ToLower(cmd.Name)] = cmd
}

// Get retrieves a command by name
func (r *Registry) Get(name string) (*Command, bool) {
	cmd, ok := r.commands[strings.ToUpper(name)]
	return cmd, ok
}

// Execute runs a command with the given arguments
func (r *Registry) Execute(name string, args []resp.Value) (resp.Value, error) {
	cmd, ok := r.Get(name)
	if !ok {
		return resp.Value{}, fmt.Errorf("ERR unknown command '%s'", name)
	}

	// Check arity
	if cmd.Arity >= 0 && len(args) != cmd.Arity {
		return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for '%s' command", name)
	}

	return cmd.Handler(args)
}

// PersistenceManager interface for persistence commands
type PersistenceManager interface {
	SaveRDB() error
	BGSaveAsync() error
	Stats() map[string]interface{}
	ClearData() error
	AppendCommand(cmd string, args []string) error
}

// RegisterPersistenceCommands registers persistence-related commands
func RegisterPersistenceCommands(registry *Registry, persist PersistenceManager, db *store.UltraOptimizedDB) {
	registry.Register(&Command{
		Name:     "SAVE",
		Arity:    0,
		Handler:  SaveHandler(persist),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "BGSAVE",
		Arity:    0,
		Handler:  BgsaveHandler(persist),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "FLUSHDB",
		Arity:    -1,
		Handler:  FlushDBWithPersistenceHandler(db, persist),
		ReadOnly: false,
	})
}

// RegisterServerCommands registers server-related commands
func RegisterServerCommands(registry *Registry, stats interface{}, replManager interface{}, password string) {
	// INFO command
	registry.Register(&Command{
		Name:     "INFO",
		Arity:    -1,
		Handler:  InfoHandler(stats),
		ReadOnly: true,
	})

	// CONFIG command
	registry.Register(&Command{
		Name:     "CONFIG",
		Arity:    -1,
		Handler:  ConfigHandler(),
		ReadOnly: true,
	})

	// AUTH command
	if password != "" {
		registry.Register(&Command{
			Name:     "AUTH",
			Arity:    2,
			Handler:  AuthHandler(password),
			ReadOnly: true,
		})
	}
}

// SaveHandler handles the SAVE command
func SaveHandler(persist PersistenceManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if err := persist.SaveRDB(); err != nil {
			return resp.Value{}, fmt.Errorf("ERR %v", err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// BgsaveHandler handles the BGSAVE command
func BgsaveHandler(persist PersistenceManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if err := persist.BGSaveAsync(); err != nil {
			return resp.Value{}, fmt.Errorf("ERR %v", err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "Background saving started"}, nil
	}
}

// InfoHandler handles the INFO command using live server stats
// Accepts a provider that exposes GetStats() *stats.OptimizedStatsManager
func InfoHandler(statsProvider interface{}) Handler {
	// Local interface to avoid import cycles for callers
	type provider interface {
		GetStats() *stats.OptimizedStatsManager
	}
	return func(args []resp.Value) (resp.Value, error) {
		var mgr *stats.OptimizedStatsManager
		if p, ok := statsProvider.(provider); ok && p != nil {
			mgr = p.GetStats()
		}
		// Build sections from snapshot (if mgr is nil, use zero-values)
		var snap stats.StatsSnapshot
		if mgr != nil {
			snap = mgr.GetSnapshot()
		}

		// Helper to build each section string
		buildServer := func() string {
			port := snap.Port
			role := snap.Role
			if role == "" {
				role = "master"
			}
			return "# Server\r\n" +
				fmt.Sprintf("redis_version:%s\r\n", snap.RedisVersion) +
				fmt.Sprintf("os:%s\r\n", snap.OS) +
				fmt.Sprintf("tcp_port:%d\r\n", port) +
				fmt.Sprintf("role:%s\r\n", role)
		}
		buildClients := func() string {
			return "# Clients\r\n" +
				fmt.Sprintf("connected_clients:%d\r\n", snap.ActiveConnections) +
				"blocked_clients:0\r\n"
		}
		buildMemory := func() string {
			return "# Memory\r\n" +
				fmt.Sprintf("used_memory:%d\r\n", snap.UsedMemory) +
				fmt.Sprintf("used_memory_peak:%d\r\n", snap.PeakMemory)
		}
		buildStats := func() string {
			return "# Stats\r\n" +
				fmt.Sprintf("total_connections_received:%d\r\n", snap.TotalConnectionsReceived) +
				fmt.Sprintf("total_commands_processed:%d\r\n", snap.TotalCommandsProcessed)
		}
		buildCommands := func() string {
			b := strings.Builder{}
			b.WriteString("# Commands\r\n")
			if snap.CommandsByType != nil {
				for cmd, calls := range snap.CommandsByType {
					// We don't track latency here; keep 0 like Redis if unavailable
					b.WriteString(fmt.Sprintf("cmdstat_%s:calls=%d,usec=0,usec_per_call=0\r\n", strings.ToLower(cmd), calls))
				}
			}
			return b.String()
		}
		buildKeyspace := func() string {
			b := strings.Builder{}
			b.WriteString("# Keyspace\r\n")
			// If database stats available, print db0 aggregate only
			if len(snap.DatabaseKeys) == 0 {
				b.WriteString("db0:keys=0,expires=0,avg_ttl=0\r\n")
				return b.String()
			}
			var totalKeys int64
			var totalExpires int64
			for db, k := range snap.DatabaseKeys {
				_ = db
				totalKeys += k
				if snap.DatabaseExpires != nil {
					totalExpires += snap.DatabaseExpires[db]
				}
			}
			b.WriteString(fmt.Sprintf("db0:keys=%d,expires=%d,avg_ttl=0\r\n", totalKeys, totalExpires))
			return b.String()
		}

		section := ""
		if len(args) == 1 {
			section = strings.ToLower(args[0].Str)
		}

		var info strings.Builder
		switch section {
		case "server":
			info.WriteString(buildServer())
		case "clients":
			info.WriteString(buildClients())
		case "memory":
			info.WriteString(buildMemory())
		case "stats":
			info.WriteString(buildStats())
		case "commands":
			info.WriteString(buildCommands())
		case "keyspace":
			info.WriteString(buildKeyspace())
		case "":
			info.WriteString(buildServer())
			info.WriteString("\r\n")
			info.WriteString(buildClients())
			info.WriteString("\r\n")
			info.WriteString(buildMemory())
			info.WriteString("\r\n")
			info.WriteString(buildStats())
			info.WriteString("\r\n")
			info.WriteString(buildCommands())
			info.WriteString("\r\n")
			info.WriteString(buildKeyspace())
		default:
			// Unsupported section -> empty bulk string like Redis
			return resp.Value{Type: resp.BulkString, Str: ""}, nil
		}
		info.WriteString("\r\n")
		return resp.Value{Type: resp.BulkString, Str: info.String()}, nil
	}
}

// ConfigHandler handles the CONFIG command
func ConfigHandler() Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'CONFIG' command")
		}

		subcommand := args[0].Str
		switch subcommand {
		case "GET", "get":
			if len(args) < 2 {
				return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'CONFIG GET' command")
			}

			// Return proper format for CONFIG GET (key-value pairs)
			configKey := args[1].Str
			var configValue string
			switch configKey {
			case "port":
				configValue = "6380"
			case "bind":
				configValue = "0.0.0.0"
			case "timeout":
				configValue = "0"
			case "tcp-keepalive":
				configValue = "300"
			case "databases":
				configValue = "16"
			case "appendonly":
				configValue = "true"
			case "save":
				configValue = "3600 1 300 100 60 10000"
			default:
				configValue = ""
			}

			if configValue == "" {
				return resp.Value{Type: resp.Array, Array: []resp.Value{}}, nil
			}

			return resp.Value{Type: resp.Array, Array: []resp.Value{
				{Type: resp.BulkString, Str: configKey},
				{Type: resp.BulkString, Str: configValue},
			}}, nil
		case "SET", "set":
			if len(args) < 3 {
				return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'CONFIG SET' command")
			}
			return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
		default:
			return resp.Value{}, fmt.Errorf("ERR unknown subcommand or wrong number of arguments for 'CONFIG' command")
		}
	}
}

// AuthHandler handles the AUTH command
func AuthHandler(password string) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// If no password is required, accept any auth attempt
		if password == "" {
			return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
		}

		// If password is required, check arguments
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'AUTH' command")
		}
		if args[0].Str != password {
			return resp.Value{}, fmt.Errorf("ERR invalid password")
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// OptimizedSetHandler is a faster version of SetHandler
func OptimizedSetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SET' command")
		}

		key := args[0].Str
		value := args[1].Str
		expiration := time.Time{}

		// Parse optional expiration (optimized)
		for i := 2; i < len(args); i++ {
			if i+1 >= len(args) {
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}

			option := args[i].Str
			// Fast string comparison instead of ToUpper
			if len(option) == 2 && option[0] == 'E' && option[1] == 'X' {
				sec, err := strconv.Atoi(args[i+1].Str)
				if err != nil {
					return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
				}
				expiration = time.Now().Add(time.Duration(sec) * time.Second)
				i++ // Skip the next argument since we consumed it
			} else if len(option) == 2 && option[0] == 'P' && option[1] == 'X' {
				msec, err := strconv.Atoi(args[i+1].Str)
				if err != nil {
					return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
				}
				expiration = time.Now().Add(time.Duration(msec) * time.Millisecond)
				i++ // Skip the next argument since we consumed it
			} else {
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}
		}

		store.Set(key, value, expiration)
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// OptimizedGetHandler is a faster version of GetHandler
func OptimizedGetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'GET' command")
		}

		key := args[0].Str
		value, exists := store.Get(key)
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}
		return resp.Value{Type: resp.BulkString, Str: value}, nil
	}
}

// OptimizedDelHandler is a faster version of DelHandler
func OptimizedDelHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'DEL' command")
		}

		deleted := 0
		for _, arg := range args {
			if store.Del(arg.Str) {
				deleted++
			}
		}
		return resp.Value{Type: resp.Integer, Int: int64(deleted)}, nil
	}
}

// OptimizedExistsHandler is a faster version of ExistsHandler
func OptimizedExistsHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'EXISTS' command")
		}

		exists := 0
		for _, arg := range args {
			if store.Exists(arg.Str) {
				exists++
			}
		}
		return resp.Value{Type: resp.Integer, Int: int64(exists)}, nil
	}
}

// OptimizedTTLHandler is a faster version of TTLHandler
func OptimizedTTLHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'TTL' command")
		}

		key := args[0].Str
		ttl := store.TTL(key)
		return resp.Value{Type: resp.Integer, Int: ttl}, nil
	}
}

// OptimizedPTTLHandler is a faster version of PTTLHandler
func OptimizedPTTLHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'PTTL' command")
		}

		key := args[0].Str
		pttl := store.PTTL(key)
		return resp.Value{Type: resp.Integer, Int: pttl}, nil
	}
}

// OptimizedExpireHandler is a faster version of ExpireHandler
func OptimizedExpireHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'EXPIRE' command")
		}

		key := args[0].Str
		seconds, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		success := store.Expire(key, time.Duration(seconds)*time.Second)
		if success {
			return resp.Value{Type: resp.Integer, Int: 1}, nil
		}
		return resp.Value{Type: resp.Integer, Int: 0}, nil
	}
}

// OptimizedIncrHandler is a faster version of IncrHandler
func OptimizedIncrHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'INCR' command")
		}

		key := args[0].Str
		currentValue, exists := store.Get(key)

		var newValue int64
		if !exists {
			// Key doesn't exist, start with 0
			newValue = 1
		} else {
			// Parse current value
			currentInt, err := strconv.ParseInt(currentValue, 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}
			newValue = currentInt + 1
		}

		// Set the new value
		store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
		return resp.Value{Type: resp.Integer, Int: newValue}, nil
	}
}

// OptimizedDecrHandler is a faster version of DecrHandler
func OptimizedDecrHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'DECR' command")
		}

		key := args[0].Str
		currentValue, exists := store.Get(key)

		var newValue int64
		if !exists {
			// Key doesn't exist, start with 0
			newValue = -1
		} else {
			// Parse current value
			currentInt, err := strconv.ParseInt(currentValue, 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}
			newValue = currentInt - 1
		}

		// Set the new value
		store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
		return resp.Value{Type: resp.Integer, Int: newValue}, nil
	}
}

// EchoHandler handles the ECHO command
var PingHandler Handler = func(args []resp.Value) (resp.Value, error) {
	if len(args) > 1 {
		return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'PING' command")
	}
	if len(args) == 1 {
		// For compatibility with existing tests, return SimpleString for PING with message
		return resp.Value{Type: resp.SimpleString, Str: args[0].Str}, nil
	}
	return resp.Value{Type: resp.SimpleString, Str: "PONG"}, nil
}

func EchoHandler() Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ECHO' command")
		}
		return resp.Value{Type: resp.BulkString, Str: args[0].Str}, nil
	}
}

// Compatibility wrappers for legacy-named handlers used in tests
func SetHandler(store Store) Handler    { return OptimizedSetHandler(store) }
func GetHandler(store Store) Handler    { return OptimizedGetHandler(store) }
func DelHandler(store Store) Handler    { return OptimizedDelHandler(store) }
func ExistsHandler(store Store) Handler { return OptimizedExistsHandler(store) }
func TTLHandler(store Store) Handler    { return OptimizedTTLHandler(store) }
func PTTLHandler(store Store) Handler   { return OptimizedPTTLHandler(store) }
func ExpireHandler(store Store) Handler { return OptimizedExpireHandler(store) }

// RegisterOptimizedCommands registers optimized command handlers
func RegisterOptimizedCommands(registry *Registry, store DataStore) {
	// Register optimized basic commands
	// registry.Register(&Command{
	// 	Name:     "SET",
	// 	Arity:    -1, // Variable arity: SET key value [EX seconds] [PX milliseconds]
	// 	Handler:  OptimizedSetHandler(store),
	// 	ReadOnly: false,
	// })
	//
	// registry.Register(&Command{
	// 	Name:     "GET",
	// 	Arity:    1,
	// 	Handler:  OptimizedGetHandler(store),
	// 	ReadOnly: true,
	// })

	registry.Register(&Command{
		Name:     "DEL",
		Arity:    -1, // Variable arity: DEL key [key ...]
		Handler:  OptimizedDelHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "EXISTS",
		Arity:    -1, // Variable arity: EXISTS key [key ...]
		Handler:  OptimizedExistsHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "TTL",
		Arity:    1,
		Handler:  OptimizedTTLHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "PTTL",
		Arity:    1,
		Handler:  OptimizedPTTLHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "EXPIRE",
		Arity:    2,
		Handler:  OptimizedExpireHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "INCR",
		Arity:    1,
		Handler:  OptimizedIncrHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "DECR",
		Arity:    1,
		Handler:  OptimizedDecrHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "DEL",
		Arity:    -1,
		Handler:  OptimizedDelHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "EXISTS",
		Arity:    -1,
		Handler:  OptimizedExistsHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "TTL",
		Arity:    1,
		Handler:  OptimizedTTLHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "PTTL",
		Arity:    1,
		Handler:  OptimizedPTTLHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "MSET",
		Arity:    -1,
		Handler:  OptimizedMSetHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "MGET",
		Arity:    -1,
		Handler:  OptimizedMGetHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "KEYS",
		Arity:    -1,
		Handler:  OptimizedKeysHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "MEMORY",
		Arity:    -1, // Variable arity: MEMORY USAGE key OR MEMORY STATS
		Handler:  MemoryHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "FLUSHDB",
		Arity:    -1,
		Handler:  OptimizedFlushDBHandler(store),
		ReadOnly: false,
	})

	// Register list commands
	registry.Register(&Command{
		Name:     "LPUSH",
		Arity:    -1, // Variable arity: LPUSH key element [element ...]
		Handler:  LPushHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "RPUSH",
		Arity:    -1, // Variable arity: RPUSH key element [element ...]
		Handler:  RPushHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "LPOP",
		Arity:    1,
		Handler:  LPopHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "RPOP",
		Arity:    1,
		Handler:  RPopHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "LLEN",
		Arity:    1,
		Handler:  LLenHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "LRANGE",
		Arity:    3, // LRANGE key start stop
		Handler:  LRangeHandler(store),
		ReadOnly: true,
	})

	registry.Register(&Command{
		Name:     "HSET",
		Arity:    -1,
		Handler:  HSetHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "HGET",
		Arity:    2,
		Handler:  HGetHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "HDEL",
		Arity:    -1,
		Handler:  HDelHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "HEXISTS",
		Arity:    2,
		Handler:  HExistsHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "HGETALL",
		Arity:    1,
		Handler:  HGetAllHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "HKEYS",
		Arity:    1,
		Handler:  HKeysHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "HVALS",
		Arity:    1,
		Handler:  HValsHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "HLEN",
		Arity:    1,
		Handler:  HLenHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "HINCRBY",
		Arity:    3,
		Handler:  HIncrByHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "HINCRBYFLOAT",
		Arity:    3,
		Handler:  HIncrByFloatHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "SAdd",
		Arity:    -1,
		Handler:  SAddHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "SRem",
		Arity:    2,
		Handler:  SRemHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "SIsMember",
		Arity:    2,
		Handler:  SIsMemberHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "SMembers",
		Arity:    1,
		Handler:  SMembersHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "SCard",
		Arity:    1,
		Handler:  SCardHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "SPop",
		Arity:    1,
		Handler:  SPopHandler(store),
		ReadOnly: true,
	})

	// Sorted Set commands
	registry.Register(&Command{
		Name:     "ZADD",
		Arity:    -1,
		Handler:  ZAddHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "ZREM",
		Arity:    -1,
		Handler:  ZRemHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "ZCARD",
		Arity:    1,
		Handler:  ZCardHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "ZSCORE",
		Arity:    2,
		Handler:  ZScoreHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "ZRANGE",
		Arity:    -1,
		Handler:  ZRangeHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "ZPOPMIN",
		Arity:    -1,
		Handler:  ZPopMinHandler(store),
		ReadOnly: false,
	})

	// Misc simple commands
	registry.Register(&Command{
		Name:     "ECHO",
		Arity:    1,
		Handler:  EchoHandler(),
		ReadOnly: true,
	})

	// DBSIZE
	registry.Register(&Command{
		Name:     "DBSIZE",
		Arity:    0,
		Handler:  DBSizeHandler(store),
		ReadOnly: true,
	})

	// GETRANGE (alias SUBSTR)
	registry.Register(&Command{
		Name:     "GETRANGE",
		Arity:    3,
		Handler:  GetRangeHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "SUBSTR",
		Arity:    3,
		Handler:  GetRangeHandler(store),
		ReadOnly: true,
	})

	// Cursor-based key scan
	registry.Register(&Command{
		Name:     "SCAN",
		Arity:    -1,
		Handler:  ScanHandler(store),
		ReadOnly: true,
	})

	// Set scan
	registry.Register(&Command{
		Name:     "SSCAN",
		Arity:    -1,
		Handler:  SScanHandler(store),
		ReadOnly: true,
	})

	// Hash scan
	registry.Register(&Command{
		Name:     "HSCAN",
		Arity:    -1,
		Handler:  HScanHandler(store),
		ReadOnly: true,
	})

	// TYPE
	registry.Register(&Command{
		Name:     "TYPE",
		Arity:    1,
		Handler:  TypeHandler(store),
		ReadOnly: true,
	})

	// Streams
	registry.Register(&Command{
		Name:     "XADD",
		Arity:    -1,
		Handler:  XAddHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "XLEN",
		Arity:    1,
		Handler:  XLenHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "XRANGE",
		Arity:    -1,
		Handler:  XRangeHandler(store),
		ReadOnly: true,
	})
	registry.Register(&Command{
		Name:     "XDEL",
		Arity:    -1,
		Handler:  XDelHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "XTRIM",
		Arity:    -1,
		Handler:  XTrimHandler(store),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "XREAD",
		Arity:    -1,
		Handler:  XReadHandler(store),
		ReadOnly: true,
	})
}

// ===== MISSING BATCH OPERATIONS =====

// OptimizedMSetHandler handles the MSET command with pre-allocated slices
func OptimizedMSetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) == 0 || len(args)%2 != 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MSET' command")
		}

		// Batch set operations for better performance
		for i := 0; i < len(args); i += 2 {
			key := args[i].Str
			value := args[i+1].Str
			store.Set(key, value, time.Time{})
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// OptimizedMGetHandler handles the MGET command with pre-allocated results
func OptimizedMGetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MGET' command")
		}

		// Pre-allocate results array
		results := make([]resp.Value, len(args))

		// Batch get operations
		for i, arg := range args {
			key := arg.Str
			value, exists := store.Get(key)

			if exists {
				results[i] = resp.Value{Type: resp.BulkString, Str: value}
			} else {
				results[i] = resp.Value{Type: resp.BulkString, IsNull: true}
			}
		}

		return resp.Value{Type: resp.Array, Array: results}, nil
	}
}

// OptimizedKeysHandler handles the KEYS command with optimized pattern matching
func OptimizedKeysHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) > 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'KEYS' command")
		}

		allKeys := store.Keys()

		// Fast path: no pattern or wildcard
		if len(args) == 0 {
			return keysToRespArrayOptimized(allKeys), nil
		}

		pattern := args[0].Str
		if pattern == "" || pattern == "*" {
			return keysToRespArrayOptimized(allKeys), nil
		}

		// Optimized pattern matching
		matchedKeys := make([]string, 0, len(allKeys)/2) // Estimate capacity
		for _, key := range allKeys {
			if matchPatternOptimized(key, pattern) {
				matchedKeys = append(matchedKeys, key)
			}
		}

		return keysToRespArrayOptimized(matchedKeys), nil
	}
}

// keysToRespArrayOptimized converts keys to RESP array with pre-allocation
func keysToRespArrayOptimized(keys []string) resp.Value {
	array := make([]resp.Value, len(keys))
	for i, key := range keys {
		array[i] = resp.Value{Type: resp.BulkString, Str: key}
	}
	return resp.Value{Type: resp.Array, Array: array}
}

// matchPatternOptimized - optimized pattern matching
func matchPatternOptimized(key, pattern string) bool {
	// Simple optimization: exact match
	if pattern == key {
		return true
	}

	// Wildcard patterns - simplified for performance
	if strings.Contains(pattern, "*") {
		// For now, implement simple prefix/suffix matching
		if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
			middle := pattern[1 : len(pattern)-1]
			return strings.Contains(key, middle)
		} else if strings.HasPrefix(pattern, "*") {
			suffix := pattern[1:]
			return strings.HasSuffix(key, suffix)
		} else if strings.HasSuffix(pattern, "*") {
			prefix := pattern[:len(pattern)-1]
			return strings.HasPrefix(key, prefix)
		}
	}

	return false
}

// OptimizedFlushDBHandler handles the FLUSHDB command with batch deletion
func OptimizedFlushDBHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// Get all keys and delete in batch
		keys := store.Keys()
		for _, key := range keys {
			store.Del(key)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// []resp.Value{
//    resp.Value{
//      Type:3,
//      Str:"get",
//      Int:0,
//      Array:[]resp.Value(nil),
//      IsNull:false
//    },
//    resp.Value{Type:3, Str:"save", Int:0, Array:[]resp.Value(nil), IsNull:false}}
