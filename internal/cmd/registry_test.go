package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MockStore implements Store interface for testing
type MockStore struct {
	data map[string]string
	ttl  map[string]time.Time
}

func NewMockStore() *MockStore {
	return &MockStore{
		data: make(map[string]string),
		ttl:  make(map[string]time.Time),
	}
}

func (m *MockStore) Set(key, value string, expiration time.Time) {
	m.data[key] = value
	if !expiration.IsZero() {
		m.ttl[key] = expiration
	} else {
		delete(m.ttl, key)
	}
}

func (m *MockStore) Get(key string) (string, bool) {
	value, exists := m.data[key]
	if !exists {
		return "", false
	}

	// Check TTL
	if exp, hasTTL := m.ttl[key]; hasTTL && time.Now().After(exp) {
		delete(m.data, key)
		delete(m.ttl, key)
		return "", false
	}

	return value, true
}

func (m *MockStore) Del(key string) bool {
	if _, exists := m.data[key]; exists {
		delete(m.data, key)
		delete(m.ttl, key)
		return true
	}
	return false
}

func (m *MockStore) Exists(key string) bool {
	_, exists := m.Get(key)
	return exists
}

func (m *MockStore) TTL(key string) int64 {
	if _, exists := m.data[key]; !exists {
		return -2 // Key doesn't exist
	}

	if exp, hasTTL := m.ttl[key]; hasTTL {
		remaining := time.Until(exp)
		if remaining <= 0 {
			return -2 // Expired
		}
		return int64(remaining.Seconds())
	}

	return -1 // No expiration
}

func (m *MockStore) PTTL(key string) int64 {
	if _, exists := m.data[key]; !exists {
		return -2 // Key doesn't exist
	}

	if exp, hasTTL := m.ttl[key]; hasTTL {
		remaining := time.Until(exp)
		if remaining <= 0 {
			return -2 // Expired
		}
		return int64(remaining.Milliseconds())
	}

	return -1 // No expiration
}

func (m *MockStore) Expire(key string, duration time.Duration) bool {
	if _, exists := m.data[key]; !exists {
		return false
	}

	m.ttl[key] = time.Now().Add(duration)
	return true
}

func (m *MockStore) Keys() []string {
	var keys []string
	for key := range m.data {
		keys = append(keys, key)
	}
	return keys
}

func (m *MockStore) GetOrCreateList(key string) *store.List {
	return store.NewList()
}

func (m *MockStore) GetOrCreateSet(key string) *store.Set {
	return store.NewSet()
}

func (m *MockStore) GetOrCreateHash(key string) *store.Hash {
	return store.NewHash()
}

func (m *MockStore) GetOrCreateSortedSet(key string) *store.SortedSet {
	return store.NewSortedSet()
}
func (m *MockStore) GetOrCreateStream(key string) *store.Stream {
	return store.NewStream()
}

func (m *MockStore) GetDataType(key string) store.DataType {
	return store.TypeString
}

func TestRegistryRegistration(t *testing.T) {
	registry := NewRegistry()

	cmd := &Command{
		Name:     "TEST",
		Arity:    1,
		Handler:  func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil },
		ReadOnly: true,
	}

	registry.Register(cmd)

	// Test case-insensitive lookup
	retrieved, exists := registry.Get("test")
	require.True(t, exists)
	require.Equal(t, cmd, retrieved)

	retrieved, exists = registry.Get("TEST")
	require.True(t, exists)
	require.Equal(t, cmd, retrieved)
}

func TestRegistryExecuteUnknownCommand(t *testing.T) {
	registry := NewRegistry()

	_, err := registry.Execute("UNKNOWN", []resp.Value{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown command")
}

func TestRegistryExecuteArityMismatch(t *testing.T) {
	registry := NewRegistry()

	cmd := &Command{
		Name:     "TEST",
		Arity:    2,
		Handler:  func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil },
		ReadOnly: true,
	}
	registry.Register(cmd)

	_, err := registry.Execute("TEST", []resp.Value{{Type: resp.BulkString, Str: "arg1"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong number of arguments")
}

func TestEchoCommand(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&Command{
		Name:     "ECHO",
		Arity:    1,
		Handler:  EchoHandler(),
		ReadOnly: true,
	})

	// ECHO with argument
	result, err := registry.Execute("ECHO", []resp.Value{{Type: resp.BulkString, Str: "Hello World"}})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.BulkString, Str: "Hello World"}, result)

	// ECHO without arguments (should fail)
	_, err = registry.Execute("ECHO", []resp.Value{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong number of arguments")
}

func TestSetGetCommands(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	// Register commands
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

	// SET command
	result, err := registry.Execute("SET", []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "value1"},
	})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.SimpleString, Str: "OK"}, result)

	// GET command
	result, err = registry.Execute("GET", []resp.Value{{Type: resp.BulkString, Str: "key1"}})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.BulkString, Str: "value1"}, result)

	// GET non-existent key
	result, err = registry.Execute("GET", []resp.Value{{Type: resp.BulkString, Str: "nonexistent"}})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.BulkString, IsNull: true}, result)
}

func TestSetWithExpiration(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	registry.Register(&Command{
		Name:     "SET",
		Arity:    -1,
		Handler:  OptimizedSetHandler(store),
		ReadOnly: false,
	})

	// SET with EX
	result, err := registry.Execute("SET", []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "value1"},
		{Type: resp.BulkString, Str: "EX"},
		{Type: resp.BulkString, Str: "1"},
	})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.SimpleString, Str: "OK"}, result)

	// Verify key exists
	value, exists := store.Get("key1")
	require.True(t, exists)
	require.Equal(t, "value1", value)

	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)

	// Verify key expired
	_, exists = store.Get("key1")
	require.False(t, exists)
}

func TestSetInvalidExpiration(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	registry.Register(&Command{
		Name:     "SET",
		Arity:    -1,
		Handler:  OptimizedSetHandler(store),
		ReadOnly: false,
	})

	// SET with invalid EX value
	_, err := registry.Execute("SET", []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "value1"},
		{Type: resp.BulkString, Str: "EX"},
		{Type: resp.BulkString, Str: "invalid"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not an integer")
}

func TestRegistryList(t *testing.T) {
	registry := NewRegistry()

	registry.Register(&Command{Name: "CMD1", Arity: 0, Handler: func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil }})
	registry.Register(&Command{Name: "CMD2", Arity: 0, Handler: func(args []resp.Value) (resp.Value, error) { return resp.Value{}, nil }})

	commands := registry.List()
	require.Len(t, commands, 2)
	require.Contains(t, commands, "CMD1")
	require.Contains(t, commands, "CMD2")
}

func TestDelCommand(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	registry.Register(&Command{
		Name:     "DEL",
		Arity:    -1,
		Handler:  OptimizedDelHandler(store),
		ReadOnly: false,
	})

	// Set some keys
	store.Set("key1", "value1", time.Time{})
	store.Set("key2", "value2", time.Time{})
	store.Set("key3", "value3", time.Time{})

	// Delete single key
	result, err := registry.Execute("DEL", []resp.Value{{Type: resp.BulkString, Str: "key1"}})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: 1}, result)
	require.False(t, store.Exists("key1"))

	// Delete multiple keys
	result, err = registry.Execute("DEL", []resp.Value{
		{Type: resp.BulkString, Str: "key2"},
		{Type: resp.BulkString, Str: "key3"},
		{Type: resp.BulkString, Str: "nonexistent"},
	})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: 2}, result)
	require.False(t, store.Exists("key2"))
	require.False(t, store.Exists("key3"))
}

func TestExistsCommand(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	registry.Register(&Command{
		Name:     "EXISTS",
		Arity:    -1,
		Handler:  OptimizedExistsHandler(store),
		ReadOnly: true,
	})

	// Set some keys
	store.Set("key1", "value1", time.Time{})
	store.Set("key2", "value2", time.Time{})

	// Check single key
	result, err := registry.Execute("EXISTS", []resp.Value{{Type: resp.BulkString, Str: "key1"}})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: 1}, result)

	// Check multiple keys
	result, err = registry.Execute("EXISTS", []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "key2"},
		{Type: resp.BulkString, Str: "nonexistent"},
	})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: 2}, result)
}

func TestTTLCommand(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	registry.Register(&Command{
		Name:     "TTL",
		Arity:    1,
		Handler:  OptimizedTTLHandler(store),
		ReadOnly: true,
	})

	// Test non-existent key
	result, err := registry.Execute("TTL", []resp.Value{{Type: resp.BulkString, Str: "nonexistent"}})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: -2}, result)

	// Test key without TTL
	store.Set("key1", "value1", time.Time{})
	result, err = registry.Execute("TTL", []resp.Value{{Type: resp.BulkString, Str: "key1"}})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: -1}, result)

	// Test key with TTL
	store.Set("key2", "value2", time.Now().Add(2*time.Second))
	result, err = registry.Execute("TTL", []resp.Value{{Type: resp.BulkString, Str: "key2"}})
	require.NoError(t, err)
	require.Greater(t, result.Int, int64(0))
	require.LessOrEqual(t, result.Int, int64(2))
}

func TestPTTLCommand(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	registry.Register(&Command{
		Name:     "PTTL",
		Arity:    1,
		Handler:  OptimizedPTTLHandler(store),
		ReadOnly: true,
	})

	// Test key with TTL in milliseconds
	store.Set("key1", "value1", time.Now().Add(1*time.Second))
	result, err := registry.Execute("PTTL", []resp.Value{{Type: resp.BulkString, Str: "key1"}})
	require.NoError(t, err)
	require.Greater(t, result.Int, int64(0))
	require.LessOrEqual(t, result.Int, int64(1000))
}

func TestExpireCommand(t *testing.T) {
	store := NewMockStore()
	registry := NewRegistry()

	registry.Register(&Command{
		Name:     "EXPIRE",
		Arity:    2,
		Handler:  OptimizedExpireHandler(store),
		ReadOnly: false,
	})

	// Set a key without expiration
	store.Set("key1", "value1", time.Time{})
	require.Equal(t, int64(-1), store.TTL("key1"))

	// Set expiration
	result, err := registry.Execute("EXPIRE", []resp.Value{
		{Type: resp.BulkString, Str: "key1"},
		{Type: resp.BulkString, Str: "2"},
	})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: 1}, result)

	// Check TTL was set
	ttl := store.TTL("key1")
	require.Greater(t, ttl, int64(0))
	require.LessOrEqual(t, ttl, int64(2))

	// Try to expire non-existent key
	result, err = registry.Execute("EXPIRE", []resp.Value{
		{Type: resp.BulkString, Str: "nonexistent"},
		{Type: resp.BulkString, Str: "1"},
	})
	require.NoError(t, err)
	require.Equal(t, resp.Value{Type: resp.Integer, Int: 0}, result)
}
