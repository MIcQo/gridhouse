package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
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
	GetOrCreateStream(key string) *store.Stream
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
func RegisterServerCommands(registry *Registry, stats interface{}) {
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

	// PING command
	registry.Register(&Command{
		Name:     "PING",
		Arity:    -1,
		Handler:  PingHandler(),
		ReadOnly: true,
	})
}

// RegisterOptimizedCommands registers optimized command handlers
func RegisterOptimizedCommands(registry *Registry, store DataStore) {
	registry.Register(&Command{
		Name:     "SET",
		Arity:    -1,
		Handler:  OptimizedSetHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "GET",
		Arity:    1,
		Handler:  OptimizedGetHandler(store),
		ReadOnly: true,
	})

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
		Handler:  MSetHandler(store),
		ReadOnly: false,
	})

	registry.Register(&Command{
		Name:     "MGET",
		Arity:    -1,
		Handler:  MGetHandler(store),
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
		Handler:  FlushDBHandler(store),
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
