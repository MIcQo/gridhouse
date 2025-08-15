package cmd

import (
	"gridhouse/internal/store"
)

// Mock DataStore for testing
type mockDataStore struct {
	*store.UltraOptimizedDB
}

func newMockDataStore() *mockDataStore {
	return &mockDataStore{
		UltraOptimizedDB: store.NewUltraOptimizedDB(),
	}
}
