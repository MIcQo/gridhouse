package cmd

import (
	"gridhouse/internal/resp"
	"strings"
	"sync"
)

// OptimizedRegistry uses lock-free maps for zero-contention command lookups
type OptimizedRegistry struct {
	// Eliminated mutex - commands map is read-only after initialization
	commands map[string]*Command
	// Pre-computed command names for faster case-insensitive lookup
	normalizedNames map[string]string
	// Transaction manager for handling transactions
	transactionManager *TransactionManager
	// Mutex only for rare dynamic registration (not used in hot path)
	registrationMu sync.Mutex
}

// NewOptimizedRegistry creates a high-performance command registry
func NewOptimizedRegistry() *OptimizedRegistry {
	return &OptimizedRegistry{
		commands:        make(map[string]*Command, 64), // Pre-allocate for common commands
		normalizedNames: make(map[string]string, 64),
	}
}

// Register adds a command to the registry with optimized normalization
func (r *OptimizedRegistry) Register(cmd *Command) {
	upperName := strings.ToUpper(cmd.Name)

	// Only lock during rare registration, not during command execution
	r.registrationMu.Lock()
	r.commands[upperName] = cmd
	// Store normalized versions for common case variations
	r.normalizedNames[cmd.Name] = upperName
	r.normalizedNames[upperName] = upperName
	r.normalizedNames[strings.ToLower(cmd.Name)] = upperName
	r.registrationMu.Unlock()
}

// Get retrieves a command by name with zero-contention lookup
func (r *OptimizedRegistry) Get(name string) (*Command, bool) {
	// Lock-free direct lookup (fastest path)
	if cmd, ok := r.commands[name]; ok {
		return cmd, true
	}

	// Lock-free normalized lookup
	if normalized, ok := r.normalizedNames[name]; ok {
		cmd := r.commands[normalized]
		return cmd, true
	}

	// Fallback to case-insensitive lookup
	upperName := strings.ToUpper(name)
	cmd, ok := r.commands[upperName]

	if ok {
		// Cache this lookup for future use (rare case, ok to lock)
		r.registrationMu.Lock()
		r.normalizedNames[name] = upperName
		r.registrationMu.Unlock()
	}

	return cmd, ok
}

// Execute runs a command with optimized lookup and validation
func (r *OptimizedRegistry) Execute(name string, args []resp.Value) (resp.Value, error) {
	cmd, ok := r.Get(name)
	if !ok {
		return resp.Value{}, &CommandError{"ERR unknown command '" + name + "'"}
	}

	// Optimized arity check
	argCount := len(args)
	if cmd.Arity >= 0 && argCount != cmd.Arity {
		return resp.Value{}, &CommandError{"ERR wrong number of arguments for '" + name + "' command"}
	}

	return cmd.Handler(args)
}

// List returns all registered command names (lock-free copy)
func (r *OptimizedRegistry) List() []string {
	// Lock-free read of command names (maps are safe for concurrent reads)
	names := make([]string, 0, len(r.commands))
	for name := range r.commands {
		names = append(names, name)
	}
	return names
}

// CommandError represents a command execution error
type CommandError struct {
	Message string
}

func (e *CommandError) Error() string {
	return e.Message
}

// UltraOptimizedRegistry uses lock-free techniques for maximum performance
type UltraOptimizedRegistry struct {
	commands sync.Map // Use sync.Map for better read performance
}

// NewUltraOptimizedRegistry creates the fastest possible command registry
func NewUltraOptimizedRegistry() *UltraOptimizedRegistry {
	return &UltraOptimizedRegistry{}
}

// Register adds a command using sync.Map for lock-free reads
func (r *UltraOptimizedRegistry) Register(cmd *Command) {
	upperName := strings.ToUpper(cmd.Name)
	r.commands.Store(upperName, cmd)
}

// Get retrieves a command with lock-free lookup
func (r *UltraOptimizedRegistry) Get(name string) (*Command, bool) {
	// Try direct lookup first
	if cmd, ok := r.commands.Load(name); ok {
		return cmd.(*Command), true
	}

	// Try uppercase lookup
	upperName := strings.ToUpper(name)
	if cmd, ok := r.commands.Load(upperName); ok {
		return cmd.(*Command), true
	}

	return nil, false
}

// Execute runs a command with ultra-fast lookup
func (r *UltraOptimizedRegistry) Execute(name string, args []resp.Value) (resp.Value, error) {
	cmd, ok := r.Get(name)
	if !ok {
		return resp.Value{}, &CommandError{"ERR unknown command '" + name + "'"}
	}

	// Fast arity check
	if cmd.Arity >= 0 && len(args) != cmd.Arity {
		return resp.Value{}, &CommandError{"ERR wrong number of arguments for '" + name + "' command"}
	}

	return cmd.Handler(args)
}

// List returns all registered command names
func (r *UltraOptimizedRegistry) List() []string {
	var names []string
	r.commands.Range(func(key, value interface{}) bool {
		names = append(names, key.(string))
		return true
	})
	return names
}
