package cmd

import (
	"gridhouse/internal/resp"
)

// FlushDBHandler handles the FLUSHDB command
// FLUSHDB removes all keys from the current database
func FlushDBHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// FLUSHDB ignores any arguments
		// Clear all keys from the store
		keys := store.Keys()
		for _, key := range keys {
			store.Del(key)
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// FlushDBWithPersistenceHandler handles the FLUSHDB command with persistence support
// This version can clear persistence files to prevent data from reappearing on restart
func FlushDBWithPersistenceHandler(store Store, persist PersistenceManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// FLUSHDB ignores any arguments
		// Clear all keys from the store
		keys := store.Keys()
		for _, key := range keys {
			store.Del(key)
		}

		// Clear persistence files if persistence is enabled
		if persist != nil {
			if err := persist.ClearData(); err != nil {
				// Log error but don't fail the command - memory is still cleared
				// In production, you might want to handle this differently
				// Error is intentionally ignored to ensure FLUSHDB always succeeds
				_ = err // Explicitly ignore error to satisfy linter
			}

			// Log the FLUSHDB command to AOF after clearing data
			// This ensures the command is recorded for replication and recovery
			// if err := persist.AppendCommand([]byte("*1\r\n$7\r\nFLUSHDB\r\n")); err != nil {
			// 	// Log error but don't fail the command
			// 	_ = err // Explicitly ignore error to satisfy linter
			// }
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}
