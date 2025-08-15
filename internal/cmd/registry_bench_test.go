package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"strconv"
	"testing"
	"time"
)

// Mock store for testing
type mockStore struct{}

func (m *mockStore) Set(key, value string, expiration time.Time)      {}
func (m *mockStore) Get(key string) (string, bool)                    { return "value", true }
func (m *mockStore) Del(key string) bool                              { return true }
func (m *mockStore) Exists(key string) bool                           { return true }
func (m *mockStore) TTL(key string) int64                             { return -1 }
func (m *mockStore) PTTL(key string) int64                            { return -1 }
func (m *mockStore) Expire(key string, duration time.Duration) bool   { return true }
func (m *mockStore) Keys() []string                                   { return []string{"key1", "key2"} }
func (m *mockStore) GetOrCreateList(key string) *store.List           { return store.NewList() }
func (m *mockStore) GetOrCreateSet(key string) *store.Set             { return store.NewSet() }
func (m *mockStore) GetOrCreateHash(key string) *store.Hash           { return store.NewHash() }
func (m *mockStore) GetOrCreateSortedSet(key string) *store.SortedSet { return store.NewSortedSet() }
func (m *mockStore) GetOrCreateStream(key string) *store.Stream       { return store.NewStream() }
func (m *mockStore) GetDataType(key string) store.DataType            { return store.TypeString }

// BenchmarkRegistryGet measures command lookup performance
func BenchmarkRegistryGet(b *testing.B) {
	registry := NewRegistry()

	// Register some commands
	commands := []string{"SET", "GET", "DEL", "EXISTS", "PING", "ECHO", "INCR", "DECR"}
	for _, cmdName := range commands {
		registry.Register(&Command{
			Name:    cmdName,
			Arity:   -1,
			Handler: func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil },
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Get(commands[i%len(commands)])
	}
}

// BenchmarkRegistryExecute measures command execution performance
func BenchmarkRegistryExecute(b *testing.B) {
	registry := NewRegistry()
	store := &mockStore{}

	// Register optimized commands
	RegisterOptimizedCommands(registry, store)

	args := []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "value1"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Execute("SET", args)
	}
}

// BenchmarkRegistryExecuteGet measures GET command execution
func BenchmarkRegistryExecuteGet(b *testing.B) {
	registry := NewRegistry()
	store := &mockStore{}

	// Register optimized commands
	RegisterOptimizedCommands(registry, store)

	args := []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Execute("GET", args)
	}
}

// BenchmarkRegistryMultipleCommands measures mixed command execution
func BenchmarkRegistryMultipleCommands(b *testing.B) {
	registry := NewRegistry()
	store := &mockStore{}

	// Register optimized commands
	RegisterOptimizedCommands(registry, store)

	commands := []string{"SET", "GET", "DEL", "EXISTS"}
	argSets := [][]resp.Value{
		{{Type: resp.BulkString, Str: "key1"}, {Type: resp.BulkString, Str: "value1"}}, // SET
		{{Type: resp.BulkString, Str: "key1"}},                                         // GET
		{{Type: resp.BulkString, Str: "key1"}},                                         // DEL
		{{Type: resp.BulkString, Str: "key1"}},                                         // EXISTS
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % len(commands)
		registry.Execute(commands[idx], argSets[idx])
	}
}

// BenchmarkRegistryCommandLookupDifferentCases measures case-insensitive lookup
func BenchmarkRegistryCommandLookupDifferentCases(b *testing.B) {
	registry := NewRegistry()

	// Register command in uppercase
	registry.Register(&Command{
		Name:    "GET",
		Arity:   1,
		Handler: func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil },
	})

	cases := []string{"GET", "get", "Get", "gET"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Get(cases[i%len(cases)])
	}
}

// BenchmarkRegistryLargeNumberOfCommands measures performance with many commands
func BenchmarkRegistryLargeNumberOfCommands(b *testing.B) {
	registry := NewRegistry()

	// Register 1000 commands
	for i := 0; i < 1000; i++ {
		registry.Register(&Command{
			Name:    "CMD" + strconv.Itoa(i),
			Arity:   -1,
			Handler: func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil },
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Get("CMD" + strconv.Itoa(i%1000))
	}
}

// BenchmarkRegistryConcurrentAccess measures concurrent command execution
func BenchmarkRegistryConcurrentAccess(b *testing.B) {
	registry := NewRegistry()
	store := &mockStore{}

	// Register optimized commands
	RegisterOptimizedCommands(registry, store)

	args := []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "value1"},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				registry.Execute("SET", args)
			} else {
				registry.Execute("GET", args[:1])
			}
			i++
		}
	})
}

// BenchmarkRegistryArityCheck measures arity validation performance
func BenchmarkRegistryArityCheck(b *testing.B) {
	registry := NewRegistry()

	// Register command with specific arity
	registry.Register(&Command{
		Name:    "TEST",
		Arity:   2,
		Handler: func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil },
	})

	args := []resp.Value{
		{Type: resp.BulkString, Str: "arg1"},
		{Type: resp.BulkString, Str: "arg2"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Execute("TEST", args)
	}
}

// BenchmarkRegistryMemoryUsage measures memory allocation during command registration
func BenchmarkRegistryMemoryUsage(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry := NewRegistry()
		for j := 0; j < 100; j++ {
			registry.Register(&Command{
				Name:    "CMD" + strconv.Itoa(j),
				Arity:   -1,
				Handler: func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil },
			})
		}
	}
}
