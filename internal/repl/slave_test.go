package repl

import (
	"testing"

	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
)

func TestNewSlave(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	slave := NewSlave("localhost:6379", db)

	assert.Equal(t, "localhost:6379", slave.masterAddr)
	assert.Equal(t, RoleSlave, slave.role)
	assert.NotNil(t, slave.stopChan)
	assert.Equal(t, db, slave.db)
}

func TestSlaveRole(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	slave := NewSlave("localhost:6379", db)

	assert.Equal(t, RoleSlave, slave.Role())
}

func TestSlaveRunID(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	slave := NewSlave("localhost:6379", db)

	// Initially empty
	assert.Equal(t, "", slave.RunID())

	// Set run ID
	slave.runID = "test-run-id"
	assert.Equal(t, "test-run-id", slave.RunID())
}

func TestSlaveOffset(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	slave := NewSlave("localhost:6379", db)

	// Initially zero
	assert.Equal(t, int64(0), slave.Offset())

	// Set offset
	slave.offset = 12345
	assert.Equal(t, int64(12345), slave.Offset())
}

func TestSlaveStop(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	slave := NewSlave("localhost:6379", db)

	// Should not panic
	slave.Stop()

	// stopChan should be closed
	select {
	case <-slave.stopChan:
		// Expected
	default:
		t.Error("stopChan should be closed")
	}
}
