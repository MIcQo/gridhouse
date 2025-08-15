package cmd

import (
	"fmt"
	"time"

	"gridhouse/internal/resp"
)

// MSetHandler handles the MSET command
// MSET key value [key value ...]
func MSetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// MSET requires an even number of arguments (key-value pairs)
		if len(args) == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MSET' command")
		}

		// Check if we have an even number of arguments
		if len(args)%2 != 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MSET' command")
		}

		// Process key-value pairs
		for i := 0; i < len(args); i += 2 {
			key := args[i].Str
			value := args[i+1].Str

			// Set the key-value pair (no expiration for MSET)
			store.Set(key, value, time.Time{})
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// MGetHandler handles the MGET command
// MGET key [key ...]
func MGetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// MGET requires at least one key
		if len(args) == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'MGET' command")
		}

		// Create array to hold results
		results := make([]resp.Value, len(args))

		// Get values for each key
		for i, arg := range args {
			key := arg.Str
			value, exists := store.Get(key)

			if exists {
				// Key exists, return the value
				results[i] = resp.Value{Type: resp.BulkString, Str: value}
			} else {
				// Key doesn't exist, return null
				results[i] = resp.Value{Type: resp.BulkString, IsNull: true}
			}
		}

		return resp.Value{Type: resp.Array, Array: results}, nil
	}
}
