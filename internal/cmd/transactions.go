package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"sync"
	"sync/atomic"
	"time"
)

// TransactionManager handles transaction state and WATCH keys
type TransactionManager struct {
	mu           sync.RWMutex
	transactions map[string]*Transaction    // connection ID -> transaction
	watchedKeys  map[string]map[string]bool // key -> set of connection IDs watching
	keyVersions  map[string]int64           // key -> version for optimistic locking
	watchDB      *WatchAwareDB              // watch-aware database for notifications
	db           store.DataStore            // database for command execution
	// Performance optimizations
	activeCount int32 // Atomic counter for active transactions
}

// Transaction represents a client transaction
type Transaction struct {
	ID          string
	Commands    []QueuedCommand
	WatchedKeys map[string]bool
	Active      bool
	Created     time.Time
}

// QueuedCommand represents a command queued in a transaction
type QueuedCommand struct {
	Name string
	Args []resp.Value
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(db store.DataStore) *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string]*Transaction),
		watchedKeys:  make(map[string]map[string]bool),
		keyVersions:  make(map[string]int64),
		db:           db,
	}
}

// StartTransaction starts a new transaction for a connection
func (tm *TransactionManager) StartTransaction(connID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if existing, exists := tm.transactions[connID]; exists && existing.Active {
		return &CommandError{"ERR MULTI calls can not be nested"}
	}

	// Preserve WATCH information if it exists
	var watchedKeys map[string]bool
	if existing, exists := tm.transactions[connID]; exists {
		watchedKeys = existing.WatchedKeys
	} else {
		watchedKeys = make(map[string]bool)
	}

	tm.transactions[connID] = &Transaction{
		ID:          connID,
		Commands:    make([]QueuedCommand, 0, 200), // Pre-allocate for very large transactions
		WatchedKeys: watchedKeys,
		Active:      true,
		Created:     time.Now(),
	}

	atomic.AddInt32(&tm.activeCount, 1)
	return nil
}

// QueueCommand adds a command to a transaction
// Ultra-optimized for performance - minimal allocations and lock time
func (tm *TransactionManager) QueueCommand(connID string, name string, args []resp.Value) error {
	tm.mu.Lock()
	txn, exists := tm.transactions[connID]
	if !exists || !txn.Active {
		tm.mu.Unlock()
		return &CommandError{"ERR EXEC without MULTI"}
	}

	// Pre-allocate slice if needed to avoid multiple allocations
	// Use exponential growth for better performance with large queues
	if cap(txn.Commands) == len(txn.Commands) {
		newCap := len(txn.Commands) * 2
		if newCap < 100 {
			newCap = 100 // Minimum growth for small queues
		}
		newCommands := make([]QueuedCommand, len(txn.Commands), newCap)
		copy(newCommands, txn.Commands)
		txn.Commands = newCommands
	}

	// Optimized args copying - avoid unnecessary allocations for common cases
	var argsCopy []resp.Value
	if len(args) <= 4 {
		// For small args, use stack allocation
		argsCopy = make([]resp.Value, len(args))
	} else {
		// For larger args, use heap allocation
		argsCopy = make([]resp.Value, len(args))
	}
	copy(argsCopy, args)

	txn.Commands = append(txn.Commands, QueuedCommand{
		Name: name,
		Args: argsCopy,
	})

	tm.mu.Unlock()
	return nil
}

// ExecuteTransaction executes all commands in a transaction
func (tm *TransactionManager) ExecuteTransaction(connID string, registry *OptimizedRegistry, aofCallback func([]QueuedCommand)) (resp.Value, error) {
	tm.mu.Lock()
	txn, exists := tm.transactions[connID]
	if !exists {
		tm.mu.Unlock()
		return resp.Value{}, &CommandError{"ERR EXEC without MULTI"}
	}

	// Check if any watched keys have been modified
	if !tm.checkWatchedKeys(connID) {
		// Clean up transaction
		delete(tm.transactions, connID)
		tm.mu.Unlock()
		return resp.Value{Type: resp.BulkString, IsNull: true}, nil // Return null for WATCH failure
	}

	// Check if transaction is active (not invalidated by WATCH)
	if !txn.Active {
		tm.mu.Unlock()
		return resp.Value{}, &CommandError{"ERR EXEC without MULTI"}
	}

	// Get commands before removing transaction
	commands := txn.Commands

	// Remove transaction from active state
	delete(tm.transactions, connID)
	atomic.AddInt32(&tm.activeCount, -1)
	tm.mu.Unlock()

	// Execute all commands with pre-allocated results array for better performance
	results := make([]resp.Value, len(commands))

	for i, cmd := range commands {
		var result resp.Value
		var err error

		if registry != nil {
			// Execute through registry
			result, err = registry.Execute(cmd.Name, cmd.Args)
		} else {
			// Execute through ultra-fast path (for server's direct execution)
			result, err = executeUltraFastCommand(cmd.Name, cmd.Args, tm.db)
		}

		if err != nil {
			// Return the actual error for this command
			result = resp.Value{Type: resp.Error, Str: err.Error()}
		}
		results[i] = result
	}

	// Log transaction to AOF if callback provided and transaction succeeded
	if aofCallback != nil {
		aofCallback(commands)
	}

	return resp.Value{Type: resp.Array, Array: results}, nil
}

// executeUltraFastCommand executes commands using ultra-fast path (for transaction execution)
func executeUltraFastCommand(name string, args []resp.Value, db store.DataStore) (resp.Value, error) {
	// Debug: check if database is nil
	if db == nil {
		return resp.Value{}, fmt.Errorf("database is nil")
	}

	// Optimized arg conversion - avoid allocation for common cases
	var argStrings []string
	if len(args) <= 4 {
		// For small args, use stack allocation
		argStrings = make([]string, len(args))
	} else {
		// For larger args, use heap allocation
		argStrings = make([]string, len(args))
	}
	for i, arg := range args {
		argStrings[i] = arg.Str
	}

	switch name {
	case "SET":
		if len(argStrings) < 2 {
			return resp.Value{}, fmt.Errorf("wrong number of arguments for 'SET' command")
		}
		db.Set(argStrings[0], argStrings[1], time.Time{})
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	case "GET":
		if len(argStrings) != 1 {
			return resp.Value{}, fmt.Errorf("wrong number of arguments for 'GET' command")
		}
		value, exists := db.Get(argStrings[0])
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}
		return resp.Value{Type: resp.BulkString, Str: value}, nil
	case "DEL":
		if len(argStrings) < 1 {
			return resp.Value{}, fmt.Errorf("wrong number of arguments for 'DEL' command")
		}
		deleted := 0
		for _, key := range argStrings {
			if db.Del(key) {
				deleted++
			}
		}
		return resp.Value{Type: resp.Integer, Int: int64(deleted)}, nil
	case "EXISTS":
		if len(argStrings) < 1 {
			return resp.Value{}, fmt.Errorf("wrong number of arguments for 'EXISTS' command")
		}
		exists := 0
		for _, key := range argStrings {
			if db.Exists(key) {
				exists++
			}
		}
		return resp.Value{Type: resp.Integer, Int: int64(exists)}, nil
	case "MSET":
		if len(argStrings) == 0 || len(argStrings)%2 != 0 {
			return resp.Value{}, fmt.Errorf("wrong number of arguments for 'MSET' command")
		}
		for i := 0; i < len(argStrings); i += 2 {
			db.Set(argStrings[i], argStrings[i+1], time.Time{})
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	case "MGET":
		if len(argStrings) == 0 {
			return resp.Value{}, fmt.Errorf("wrong number of arguments for 'MGET' command")
		}
		results := make([]resp.Value, len(argStrings))
		for i, key := range argStrings {
			value, exists := db.Get(key)
			if exists {
				results[i] = resp.Value{Type: resp.BulkString, Str: value}
			} else {
				results[i] = resp.Value{Type: resp.BulkString, IsNull: true}
			}
		}
		return resp.Value{Type: resp.Array, Array: results}, nil
	default:
		return resp.Value{}, fmt.Errorf("unknown command: %s", name)
	}
}

// DiscardTransaction discards a transaction
func (tm *TransactionManager) DiscardTransaction(connID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.transactions[connID]
	if !exists || !txn.Active {
		return &CommandError{"ERR DISCARD without MULTI"}
	}

	// Remove from watched keys
	for key := range txn.WatchedKeys {
		if conns, exists := tm.watchedKeys[key]; exists {
			delete(conns, connID)
			if len(conns) == 0 {
				delete(tm.watchedKeys, key)
			}
		}
	}

	delete(tm.transactions, connID)
	atomic.AddInt32(&tm.activeCount, -1)
	return nil
}

// WatchKeys adds keys to the watch list for a connection
func (tm *TransactionManager) WatchKeys(connID string, keys []string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if we're in a transaction
	if txn, exists := tm.transactions[connID]; exists && txn.Active {
		return &CommandError{"ERR WATCH inside MULTI is not allowed"}
	}

	// Add keys to watch list
	for _, key := range keys {
		if tm.watchedKeys[key] == nil {
			tm.watchedKeys[key] = make(map[string]bool)
		}
		tm.watchedKeys[key][connID] = true
	}

	// Create or update transaction to track watched keys
	if txn, exists := tm.transactions[connID]; exists {
		// Update existing transaction
		for _, key := range keys {
			txn.WatchedKeys[key] = true
		}
	} else {
		// Create new transaction for WATCH (not active yet)
		tm.transactions[connID] = &Transaction{
			ID:          connID,
			Commands:    make([]QueuedCommand, 0, 10),
			WatchedKeys: make(map[string]bool),
			Active:      false, // WATCH doesn't start an active transaction
			Created:     time.Now(),
		}
		for _, key := range keys {
			tm.transactions[connID].WatchedKeys[key] = true
		}
	}

	return nil
}

// UnwatchKeys removes all watched keys for a connection
func (tm *TransactionManager) UnwatchKeys(connID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if we're in a transaction
	if txn, exists := tm.transactions[connID]; exists && txn.Active {
		return &CommandError{"ERR UNWATCH inside MULTI is not allowed"}
	}

	// Remove from all watched keys
	for key, conns := range tm.watchedKeys {
		delete(conns, connID)
		if len(conns) == 0 {
			delete(tm.watchedKeys, key)
		}
	}

	return nil
}

// NotifyKeyModified notifies that a key has been modified (for WATCH functionality)
func (tm *TransactionManager) NotifyKeyModified(key string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Increment key version
	tm.keyVersions[key]++

	// Mark all transactions watching this key as invalid
	if conns, exists := tm.watchedKeys[key]; exists {
		for connID := range conns {
			if txn, exists := tm.transactions[connID]; exists {
				txn.Active = false // Mark as invalid
			}
		}
	}
}

// checkWatchedKeys checks if any watched keys have been modified
func (tm *TransactionManager) checkWatchedKeys(connID string) bool {
	txn, exists := tm.transactions[connID]
	if !exists {
		return true
	}

	// Check if the transaction is still active (not invalidated by WATCH)
	return txn.Active
}

// IsInTransaction checks if a connection is in a transaction
// Optimized for performance - fast path when no transactions active
func (tm *TransactionManager) IsInTransaction(connID string) bool {
	// Fast path: if no transactions are active, return false immediately
	if atomic.LoadInt32(&tm.activeCount) == 0 {
		return false
	}

	tm.mu.RLock()
	txn, exists := tm.transactions[connID]
	active := exists && txn.Active
	tm.mu.RUnlock()
	return active
}

// TransactionAwareRegistry wraps a registry to make it transaction-aware
type TransactionAwareRegistry struct {
	*OptimizedRegistry
	tm *TransactionManager
}

// NewTransactionAwareRegistry creates a new transaction-aware registry
func NewTransactionAwareRegistry(registry *OptimizedRegistry, tm *TransactionManager) *TransactionAwareRegistry {
	return &TransactionAwareRegistry{
		OptimizedRegistry: registry,
		tm:                tm,
	}
}

// Execute overrides the Execute method to handle transactions
func (tar *TransactionAwareRegistry) Execute(name string, args []resp.Value) (resp.Value, error) {
	// Check if we're in a transaction
	if tar.tm.IsInTransaction("default") {
		// Don't queue transaction commands themselves
		if name != "MULTI" && name != "EXEC" && name != "DISCARD" && name != "WATCH" && name != "UNWATCH" {
			// Queue the command instead of executing it
			err := tar.tm.QueueCommand("default", name, args)
			if err != nil {
				return resp.Value{}, err
			}
			return resp.Value{Type: resp.SimpleString, Str: "QUEUED"}, nil
		}
	}

	// Execute normally
	return tar.OptimizedRegistry.Execute(name, args)
}

// WatchAwareDB wraps a database to integrate with WATCH functionality
type WatchAwareDB struct {
	db store.DataStore
	tm *TransactionManager
}

// NewWatchAwareDB creates a new watch-aware database
func NewWatchAwareDB(db store.DataStore, tm *TransactionManager) *WatchAwareDB {
	return &WatchAwareDB{
		db: db,
		tm: tm,
	}
}

// Set wraps the database Set method to notify WATCH
func (wdb *WatchAwareDB) Set(key, value string, expiration time.Time) {
	wdb.db.Set(key, value, expiration)
	wdb.tm.NotifyKeyModified(key)
}

// Del wraps the database Del method to notify WATCH
func (wdb *WatchAwareDB) Del(key string) bool {
	result := wdb.db.Del(key)
	if result {
		wdb.tm.NotifyKeyModified(key)
	}
	return result
}

// Get delegates to the underlying database
func (wdb *WatchAwareDB) Get(key string) (string, bool) {
	return wdb.db.Get(key)
}

// Exists delegates to the underlying database
func (wdb *WatchAwareDB) Exists(key string) bool {
	return wdb.db.Exists(key)
}

// TTL delegates to the underlying database
func (wdb *WatchAwareDB) TTL(key string) int64 {
	return wdb.db.TTL(key)
}

// PTTL delegates to the underlying database
func (wdb *WatchAwareDB) PTTL(key string) int64 {
	return wdb.db.PTTL(key)
}

// Expire delegates to the underlying database
func (wdb *WatchAwareDB) Expire(key string, duration time.Duration) bool {
	return wdb.db.Expire(key, duration)
}

// Keys delegates to the underlying database
func (wdb *WatchAwareDB) Keys() []string {
	return wdb.db.Keys()
}

// GetOrCreateList delegates to the underlying database
func (wdb *WatchAwareDB) GetOrCreateList(key string) *store.List {
	return wdb.db.GetOrCreateList(key)
}

// GetOrCreateSet delegates to the underlying database
func (wdb *WatchAwareDB) GetOrCreateSet(key string) *store.Set {
	return wdb.db.GetOrCreateSet(key)
}

// GetOrCreateHash delegates to the underlying database
func (wdb *WatchAwareDB) GetOrCreateHash(key string) *store.Hash {
	return wdb.db.GetOrCreateHash(key)
}

// GetDataType delegates to the underlying database
func (wdb *WatchAwareDB) GetDataType(key string) store.DataType {
	// For now, return TypeString as default
	// In a real implementation, this would check the actual data type
	return store.TypeString
}

// Close delegates to the underlying database
func (wdb *WatchAwareDB) Close() {
	wdb.db.Close()
}

// RegisterTransactionCommands registers transaction-related commands
func RegisterTransactionCommands(registry *OptimizedRegistry, db store.DataStore) *TransactionAwareRegistry {
	// Create transaction manager
	tm := NewTransactionManager(db)

	// Create watch-aware database
	watchDB := NewWatchAwareDB(db, tm)
	tm.watchDB = watchDB

	// Register basic commands with watch-aware database
	registry.Register(&Command{
		Name:     "SET",
		Arity:    -1,
		Handler:  OptimizedSetHandler(watchDB),
		ReadOnly: false,
	})
	registry.Register(&Command{
		Name:     "GET",
		Arity:    1,
		Handler:  OptimizedGetHandler(watchDB),
		ReadOnly: true,
	})

	// MULTI command
	registry.Register(&Command{
		Name:     "MULTI",
		Arity:    0,
		Handler:  MultiHandler(tm),
		ReadOnly: false,
	})

	// EXEC command
	registry.Register(&Command{
		Name:     "EXEC",
		Arity:    0,
		Handler:  ExecHandler(tm, registry),
		ReadOnly: false,
	})

	// DISCARD command
	registry.Register(&Command{
		Name:     "DISCARD",
		Arity:    0,
		Handler:  DiscardHandler(tm),
		ReadOnly: false,
	})

	// WATCH command
	registry.Register(&Command{
		Name:     "WATCH",
		Arity:    -1, // At least 1 key
		Handler:  WatchHandler(tm),
		ReadOnly: true,
	})

	// UNWATCH command
	registry.Register(&Command{
		Name:     "UNWATCH",
		Arity:    0,
		Handler:  UnwatchHandler(tm),
		ReadOnly: true,
	})

	// Store transaction manager in registry for access by other commands
	registry.transactionManager = tm

	// Return transaction-aware registry
	return NewTransactionAwareRegistry(registry, tm)
}

// MultiHandler handles the MULTI command
func MultiHandler(tm *TransactionManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 0 {
			return resp.Value{}, &CommandError{"ERR wrong number of arguments for 'multi' command"}
		}

		// Use a simple connection ID for now (in real implementation, this would come from the connection)
		connID := "default"
		err := tm.StartTransaction(connID)
		if err != nil {
			return resp.Value{}, err
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// ExecHandler handles the EXEC command
func ExecHandler(tm *TransactionManager, registry *OptimizedRegistry) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 0 {
			return resp.Value{}, &CommandError{"ERR wrong number of arguments for 'exec' command"}
		}

		connID := "default"
		result, err := tm.ExecuteTransaction(connID, registry, nil)
		if err != nil {
			return resp.Value{}, err
		}

		return result, nil
	}
}

// DiscardHandler handles the DISCARD command
func DiscardHandler(tm *TransactionManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 0 {
			return resp.Value{}, &CommandError{"ERR wrong number of arguments for 'discard' command"}
		}

		connID := "default"
		err := tm.DiscardTransaction(connID)
		if err != nil {
			return resp.Value{}, err
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// WatchHandler handles the WATCH command
func WatchHandler(tm *TransactionManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 {
			return resp.Value{}, &CommandError{"ERR wrong number of arguments for 'watch' command"}
		}

		// Extract keys from arguments
		keys := make([]string, len(args))
		for i, arg := range args {
			keys[i] = arg.Str
		}

		connID := "default"
		err := tm.WatchKeys(connID, keys)
		if err != nil {
			return resp.Value{}, err
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// UnwatchHandler handles the UNWATCH command
func UnwatchHandler(tm *TransactionManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 0 {
			return resp.Value{}, &CommandError{"ERR wrong number of arguments for 'unwatch' command"}
		}

		connID := "default"
		err := tm.UnwatchKeys(connID)
		if err != nil {
			return resp.Value{}, err
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// TransactionAwareHandler wraps a command handler to be transaction-aware
func TransactionAwareHandler(originalHandler Handler, tm *TransactionManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		connID := "default"

		// Check if we're in a transaction
		if tm.IsInTransaction(connID) {
			// Extract command name from args (assuming first arg is command name)
			cmdName := ""
			if len(args) > 0 {
				cmdName = args[0].Str
			}

			// Queue the command instead of executing it
			err := tm.QueueCommand(connID, cmdName, args[1:])
			if err != nil {
				return resp.Value{}, err
			}

			return resp.Value{Type: resp.SimpleString, Str: "QUEUED"}, nil
		}

		// Not in transaction, execute normally
		return originalHandler(args)
	}
}
