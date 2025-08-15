package store

import "time"

// DataStore represents the interface that all database implementations must support
type DataStore interface {
	// Basic key-value operations
	Set(key, value string, expiration time.Time)
	Get(key string) (string, bool)
	Del(key string) bool
	Exists(key string) bool

	// Expiration operations
	TTL(key string) int64
	PTTL(key string) int64
	Expire(key string, duration time.Duration) bool

	// Key enumeration
	Keys() []string

	// Data structure operations
	GetOrCreateList(key string) *List
	GetOrCreateSet(key string) *Set
	GetOrCreateHash(key string) *Hash
	GetOrCreateSortedSet(key string) *SortedSet
	GetOrCreateStream(key string) *Stream

	// Introspection
	GetDataType(key string) DataType

	// Lifecycle
	Close()
}

// Ensure both database implementations satisfy the interface
var _ DataStore = (*UltraOptimizedDB)(nil)
