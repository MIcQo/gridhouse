package cmd

import (
	"fmt"
	"gridhouse/internal/logger"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

// MemoryUsageCalculator interface for calculating memory usage of different data types
type MemoryUsageCalculator interface {
	CalculateUsage(key string) int64
}

// memoryUsageCalculator implements memory usage calculation for the store
type memoryUsageCalculator struct {
	store Store
}

func newMemoryUsageCalculator(store Store) *memoryUsageCalculator {
	return &memoryUsageCalculator{store: store}
}

// CalculateUsage calculates the approximate memory usage of a key in bytes
func (m *memoryUsageCalculator) CalculateUsage(key string) int64 {
	if !m.store.Exists(key) {
		return 0
	}

	// Base overhead for the key itself
	keySize := int64(len(key)) + int64(unsafe.Sizeof(key))

	// Try to get as string first
	if value, ok := m.store.Get(key); ok {
		// String value
		valueSize := int64(len(value)) + int64(unsafe.Sizeof(value))
		return keySize + valueSize + 64 // Additional overhead for storage structures
	}

	// Check if it's a list
	if list := m.tryGetList(key); list != nil {
		return keySize + m.calculateListUsage(list)
	}

	// Check if it's a hash
	if hash := m.tryGetHash(key); hash != nil {
		return keySize + m.calculateHashUsage(hash)
	}

	// Check if it's a set
	if set := m.tryGetSet(key); set != nil {
		return keySize + m.calculateSetUsage(set)
	}

	// Default fallback
	return keySize + 64
}

func (m *memoryUsageCalculator) tryGetList(key string) *store.List {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	// For now, return nil - in a real implementation we'd need type information
	// This is a simplified approach for the MEMORY command demo
	return nil
}

func (m *memoryUsageCalculator) tryGetHash(key string) *store.Hash {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	// For now, return nil - in a real implementation we'd need type information
	// This is a simplified approach for the MEMORY command demo
	return nil
}

func (m *memoryUsageCalculator) tryGetSet(key string) *store.Set {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	// For now, return nil - in a real implementation we'd need type information
	// This is a simplified approach for the MEMORY command demo
	return nil
}

// Helper methods to check if data structures exist (simplified approach)
func (m *memoryUsageCalculator) listExists(key string) bool { //nolint:unused
	// This is a simplified check - in a real implementation,
	// we'd need type information from the store
	return false
}

func (m *memoryUsageCalculator) hashExists(key string) bool { //nolint:unused
	return false
}

func (m *memoryUsageCalculator) setExists(key string) bool { //nolint:unused
	return false
}

func (m *memoryUsageCalculator) calculateListUsage(list *store.List) int64 {
	if list == nil {
		return 0
	}

	// Base list structure overhead
	usage := int64(unsafe.Sizeof(*list)) + 64

	// Estimate usage based on length (simplified)
	length := list.LLen()
	if length > 0 {
		// Assume average string length of 20 bytes per element
		usage += int64(length) * 20
		// Add pointer overhead
		usage += int64(length) * int64(unsafe.Sizeof(uintptr(0)))
	}

	return usage
}

func (m *memoryUsageCalculator) calculateHashUsage(hash *store.Hash) int64 {
	if hash == nil {
		return 0
	}

	// Base hash structure overhead
	usage := int64(unsafe.Sizeof(*hash)) + 64

	// Estimate usage based on field count
	fieldCount := hash.HLen()
	if fieldCount > 0 {
		// Assume average field name + value of 40 bytes
		usage += int64(fieldCount) * 40
		// Add map overhead
		usage += int64(fieldCount) * int64(unsafe.Sizeof(uintptr(0))*2)
	}

	return usage
}

func (m *memoryUsageCalculator) calculateSetUsage(set *store.Set) int64 {
	if set == nil {
		return 0
	}

	// Base set structure overhead
	usage := int64(unsafe.Sizeof(*set)) + 64

	// Estimate usage based on member count
	memberCount := set.SCard()
	if memberCount > 0 {
		// Assume average member length of 15 bytes
		usage += int64(memberCount) * 15
		// Add set overhead
		usage += int64(memberCount) * int64(unsafe.Sizeof(uintptr(0)))
	}

	return usage
}

// MemoryHandler handles the MEMORY command with various subcommands
func MemoryHandler(store Store) Handler {
	calculator := newMemoryUsageCalculator(store)

	return func(args []resp.Value) (resp.Value, error) {
		if len(args) == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MEMORY' command")
		}

		subcommand := strings.ToUpper(args[0].Str)

		switch subcommand {
		case "USAGE":
			return handleMemoryUsage(args, calculator)
		case "STATS":
			return handleMemoryStats(args)
		default:
			return resp.Value{}, fmt.Errorf("ERR unknown subcommand '%s' for 'MEMORY' command", args[0].Str)
		}
	}
}

func handleMemoryUsage(args []resp.Value, calculator MemoryUsageCalculator) (resp.Value, error) {
	// Supported forms:
	// MEMORY USAGE key
	// MEMORY USAGE key SAMPLES count
	if len(args) < 2 {
		return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MEMORY USAGE' command")
	}

	key := args[1].Str

	// Parse optional arguments
	if len(args) > 2 {
		// Expect exactly: SAMPLES <count>
		if len(args) != 4 {
			return resp.Value{}, fmt.Errorf("ERR syntax error")
		}
		if strings.ToUpper(args[2].Str) != "SAMPLES" {
			return resp.Value{}, fmt.Errorf("ERR syntax error")
		}
		// Validate count
		if _, err := strconv.Atoi(args[3].Str); err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		// For now we ignore the samples hint as calculation is approximate.
	}

	usage := calculator.CalculateUsage(key)
	if usage == 0 {
		return resp.Value{Type: resp.BulkString, IsNull: true}, nil
	}
	return resp.Value{Type: resp.Integer, Int: usage}, nil
}

func handleMemoryStats(args []resp.Value) (resp.Value, error) {
	if len(args) != 1 {
		return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MEMORY STATS' command")
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Return memory statistics as an array of key-value pairs
	stats := []resp.Value{
		{Type: resp.BulkString, Str: "peak.allocated"},
		{Type: resp.Integer, Int: int64(m.TotalAlloc)},
		{Type: resp.BulkString, Str: "total.allocated"},
		{Type: resp.Integer, Int: int64(m.TotalAlloc)},
		{Type: resp.BulkString, Str: "startup.allocated"},
		{Type: resp.Integer, Int: int64(m.Alloc)},
		{Type: resp.BulkString, Str: "replication.backlog"},
		{Type: resp.Integer, Int: 0},
		{Type: resp.BulkString, Str: "clients.slaves"},
		{Type: resp.Integer, Int: 0},
		{Type: resp.BulkString, Str: "clients.normal"},
		{Type: resp.Integer, Int: int64(m.Mallocs - m.Frees)},
		{Type: resp.BulkString, Str: "aof.buffer"},
		{Type: resp.Integer, Int: 0},
		{Type: resp.BulkString, Str: "lua.caches"},
		{Type: resp.Integer, Int: 0},
		{Type: resp.BulkString, Str: "overhead.total"},
		{Type: resp.Integer, Int: int64(m.Sys - m.Alloc)},
		{Type: resp.BulkString, Str: "keys.count"},
		{Type: resp.Integer, Int: 0}, // Would need store interface extension to get actual count
		{Type: resp.BulkString, Str: "keys.bytes-per-key"},
		{Type: resp.Integer, Int: 0},
		{Type: resp.BulkString, Str: "dataset.bytes"},
		{Type: resp.Integer, Int: int64(m.Alloc)},
		{Type: resp.BulkString, Str: "dataset.percentage"},
		{Type: resp.BulkString, Str: "100.00"},
		{Type: resp.BulkString, Str: "peak.percentage"},
		{Type: resp.BulkString, Str: "100.00"},
		{Type: resp.BulkString, Str: "fragmentation"},
		{Type: resp.BulkString, Str: "1.00"},
	}

	return resp.Value{Type: resp.Array, Array: stats}, nil
}
