package cmd

import (
	"gridhouse/internal/store"
)

// Extended Store interface for data structures
type DataStore interface {
	Store
	GetDataType(key string) store.DataType
	// List operations
	GetOrCreateList(key string) *store.List
	// Set operations
	GetOrCreateSet(key string) *store.Set
	// Hash operations
	GetOrCreateHash(key string) *store.Hash
	// Sorted Set operations
	GetOrCreateSortedSet(key string) *store.SortedSet
	// Stream operations
	GetOrCreateStream(key string) *store.Stream
}
