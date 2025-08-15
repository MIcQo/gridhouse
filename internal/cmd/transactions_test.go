package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"testing"
	"time"
)

func TestTransactionCommands(t *testing.T) {
	tests := []struct {
		name     string
		commands []struct {
			cmd  string
			args []string
		}
		expected    []resp.Value
		expectError bool
	}{
		{
			name: "MULTI without arguments",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
			},
		},
		{
			name: "MULTI with arguments should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{"extra"}},
			},
			expectError: true,
		},
		{
			name: "EXEC without MULTI should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"EXEC", []string{}},
			},
			expectError: true,
		},
		{
			name: "DISCARD without MULTI should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"DISCARD", []string{}},
			},
			expectError: true,
		},
		{
			name: "Simple transaction with SET and GET",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"SET", []string{"key1", "value1"}},
				{"GET", []string{"key1"}},
				{"EXEC", []string{}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
				{Type: resp.SimpleString, Str: "QUEUED"},
				{Type: resp.SimpleString, Str: "QUEUED"},
				{Type: resp.Array, Array: []resp.Value{
					{Type: resp.SimpleString, Str: "OK"},
					{Type: resp.BulkString, Str: "value1"},
				}},
			},
		},
		{
			name: "Transaction with DISCARD",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"SET", []string{"key2", "value2"}},
				{"DISCARD", []string{}},
				{"GET", []string{"key2"}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
				{Type: resp.SimpleString, Str: "QUEUED"},
				{Type: resp.SimpleString, Str: "OK"},
				{Type: resp.BulkString, Str: ""}, // Key should not exist
			},
		},
		{
			name: "Transaction with error command",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"SET", []string{"key3", "value3"}},
				{"INVALID", []string{"command"}},
				{"GET", []string{"key3"}},
				{"EXEC", []string{}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
				{Type: resp.SimpleString, Str: "QUEUED"},
				{Type: resp.SimpleString, Str: "QUEUED"},
				{Type: resp.SimpleString, Str: "QUEUED"},
				{Type: resp.Array, Array: []resp.Value{
					{Type: resp.SimpleString, Str: "OK"},
					{Type: resp.Error, Str: "ERR unknown command 'INVALID'"},
					{Type: resp.BulkString, Str: "value3"},
				}},
			},
		},
		{
			name: "Nested MULTI should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"MULTI", []string{}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
				{Type: resp.Error, Str: "ERR MULTI calls can not be nested"},
			},
		},
		{
			name: "WATCH without arguments should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"WATCH", []string{}},
			},
			expectError: true,
		},
		{
			name: "WATCH with single key",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"WATCH", []string{"key1"}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
			},
		},
		{
			name: "WATCH with multiple keys",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"WATCH", []string{"key1", "key2", "key3"}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
			},
		},
		{
			name: "UNWATCH without arguments",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"UNWATCH", []string{}},
			},
			expected: []resp.Value{
				{Type: resp.SimpleString, Str: "OK"},
			},
		},
		{
			name: "UNWATCH with arguments should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"UNWATCH", []string{"key1"}},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := store.NewUltraOptimizedDB()
			registry := NewOptimizedRegistry()

			// Create transaction-aware registry (includes basic commands)
			txRegistry := RegisterTransactionCommands(registry, db)

			var responses []resp.Value

			for i, cmd := range tt.commands {
				// Convert string args to resp.Value format
				respArgs := make([]resp.Value, len(cmd.args))
				for j, arg := range cmd.args {
					respArgs[j] = resp.Value{Type: resp.BulkString, Str: arg}
				}

				response, err := txRegistry.Execute(cmd.cmd, respArgs)

				if tt.expectError && i == 0 {
					if err == nil {
						t.Errorf("Expected error for command %s, but got none", cmd.cmd)
					}
					return
				}

				if err != nil {
					response = resp.Value{Type: resp.Error, Str: err.Error()}
				}

				responses = append(responses, response)

			}

			// Verify responses
			if len(responses) != len(tt.expected) {
				t.Errorf("Expected %d responses, got %d", len(tt.expected), len(responses))
				return
			}

			for i, expected := range tt.expected {
				if i >= len(responses) {
					t.Errorf("Missing response at index %d", i)
					continue
				}

				actual := responses[i]
				if !compareValues(actual, expected) {
					t.Errorf("Response %d mismatch:\nExpected: %+v\nGot: %+v", i, expected, actual)
				}
			}
		})
	}
}

func TestTransactionConcurrency(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	registry := NewOptimizedRegistry()

	// Create transaction-aware registry (includes basic commands)
	txRegistry := RegisterTransactionCommands(registry, db)

	// Get the watch-aware database from the transaction manager
	watchDB := txRegistry.tm.watchDB

	// Set initial value
	txRegistry.Execute("SET", []resp.Value{
		{Type: resp.BulkString, Str: "counter"},
		{Type: resp.BulkString, Str: "0"},
	})

	// Test WATCH with concurrent modification
	t.Run("WATCH with concurrent modification", func(t *testing.T) {
		// Start transaction 1
		txRegistry.Execute("WATCH", []resp.Value{{Type: resp.BulkString, Str: "counter"}})
		txRegistry.Execute("MULTI", []resp.Value{})
		txRegistry.Execute("GET", []resp.Value{{Type: resp.BulkString, Str: "counter"}})
		txRegistry.Execute("SET", []resp.Value{
			{Type: resp.BulkString, Str: "counter"},
			{Type: resp.BulkString, Str: "1"},
		})

		// Modify the key from another "connection" (simulate concurrent access)
		// We need to use a different approach since we're using the same connection ID
		// Let's directly modify the database to simulate concurrent access
		watchDB.Set("counter", "modified", time.Time{})

		// Try to execute transaction 1 - should fail
		response, _ := txRegistry.Execute("EXEC", []resp.Value{})
		if response.Type != resp.BulkString || !response.IsNull {
			t.Errorf("Expected EXEC to return null due to WATCH failure, got: %+v", response)
		}
	})

	t.Run("WATCH without concurrent modification", func(t *testing.T) {
		// Reset counter
		txRegistry.Execute("SET", []resp.Value{
			{Type: resp.BulkString, Str: "counter"},
			{Type: resp.BulkString, Str: "0"},
		})

		// Start transaction with WATCH
		txRegistry.Execute("WATCH", []resp.Value{{Type: resp.BulkString, Str: "counter"}})
		txRegistry.Execute("MULTI", []resp.Value{})
		txRegistry.Execute("GET", []resp.Value{{Type: resp.BulkString, Str: "counter"}})
		txRegistry.Execute("SET", []resp.Value{
			{Type: resp.BulkString, Str: "counter"},
			{Type: resp.BulkString, Str: "1"},
		})

		// Execute transaction - should succeed
		response, _ := txRegistry.Execute("EXEC", []resp.Value{})
		if response.Type != resp.Array {
			t.Errorf("Expected EXEC to return array, got: %+v", response)
		}
		if len(response.Array) != 2 {
			t.Errorf("Expected 2 responses in transaction, got %d", len(response.Array))
		}
	})
}

func TestTransactionPerformance(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	registry := NewOptimizedRegistry()

	// Create transaction-aware registry (includes basic commands)
	txRegistry := RegisterTransactionCommands(registry, db)

	// Performance test: large transaction
	t.Run("Large transaction performance", func(t *testing.T) {
		start := time.Now()

		// Start transaction
		txRegistry.Execute("MULTI", []resp.Value{})

		// Queue many commands
		for i := 0; i < 1000; i++ {
			key := "key" + string(rune(i))
			value := "value" + string(rune(i))
			txRegistry.Execute("SET", []resp.Value{
				{Type: resp.BulkString, Str: key},
				{Type: resp.BulkString, Str: value},
			})
		}

		// Execute transaction
		response, _ := txRegistry.Execute("EXEC", []resp.Value{})
		duration := time.Since(start)

		if response.Type != resp.Array {
			t.Errorf("Expected EXEC to return array, got: %+v", response)
		}
		if len(response.Array) != 1000 {
			t.Errorf("Expected 1000 responses in transaction, got %d", len(response.Array))
		}

		// Performance assertion: should complete within reasonable time
		if duration > 100*time.Millisecond {
			t.Errorf("Transaction took too long: %v", duration)
		}

		t.Logf("Large transaction completed in %v", duration)
	})
}

func TestTransactionEdgeCases(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	registry := NewOptimizedRegistry()

	// Create transaction-aware registry (includes basic commands)
	txRegistry := RegisterTransactionCommands(registry, db)

	tests := []struct {
		name     string
		commands []struct {
			cmd  string
			args []string
		}
		expectedError bool
	}{
		{
			name: "EXEC with no queued commands",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"EXEC", []string{}},
			},
		},
		{
			name: "DISCARD with no queued commands",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"DISCARD", []string{}},
			},
		},
		{
			name: "WATCH during transaction should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"WATCH", []string{"key1"}},
			},
			expectedError: true,
		},
		{
			name: "UNWATCH during transaction should error",
			commands: []struct {
				cmd  string
				args []string
			}{
				{"MULTI", []string{}},
				{"UNWATCH", []string{}},
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, cmd := range tt.commands {
				respArgs := make([]resp.Value, len(cmd.args))
				for j, arg := range cmd.args {
					respArgs[j] = resp.Value{Type: resp.BulkString, Str: arg}
				}

				_, err := txRegistry.Execute(cmd.cmd, respArgs)

				if tt.expectedError && i == 1 { // Second command should error
					if err == nil {
						t.Errorf("Expected error for command %s, but got none", cmd.cmd)
					}
					return
				}

				if err != nil && !tt.expectedError {
					t.Errorf("Unexpected error for command %s: %v", cmd.cmd, err)
				}
			}
		})
	}
}

func TestWatchMultiFix(t *testing.T) {
	db := store.NewUltraOptimizedDB()
	registry := NewOptimizedRegistry()

	// Create transaction-aware registry (includes basic commands)
	txRegistry := RegisterTransactionCommands(registry, db)

	// Test WATCH then MULTI sequence
	t.Logf("Initial transaction state: %v", txRegistry.tm.IsInTransaction("default"))

	// WATCH command
	response, _ := txRegistry.Execute("WATCH", []resp.Value{{Type: resp.BulkString, Str: "counter"}})
	t.Logf("WATCH response: %+v", response)
	t.Logf("After WATCH - transaction state: %v", txRegistry.tm.IsInTransaction("default"))

	// MULTI command should now work
	response, _ = txRegistry.Execute("MULTI", []resp.Value{})
	t.Logf("MULTI response: %+v", response)
	t.Logf("After MULTI - transaction state: %v", txRegistry.tm.IsInTransaction("default"))

	// Queue a command
	response, _ = txRegistry.Execute("GET", []resp.Value{{Type: resp.BulkString, Str: "counter"}})
	t.Logf("GET response: %+v", response)

	// EXEC command
	response, _ = txRegistry.Execute("EXEC", []resp.Value{})
	t.Logf("EXEC response: %+v", response)
}

// Helper function to compare resp.Value objects
func compareValues(a, b resp.Value) bool {
	if a.Type != b.Type {
		return false
	}

	switch a.Type {
	case resp.SimpleString, resp.BulkString, resp.Error:
		return a.Str == b.Str
	case resp.Integer:
		return a.Int == b.Int
	case resp.Array:
		if len(a.Array) != len(b.Array) {
			return false
		}
		for i, val := range a.Array {
			if !compareValues(val, b.Array[i]) {
				return false
			}
		}
		return true
	default:
		// For null values, check IsNull field
		return a.IsNull == b.IsNull
	}
}
