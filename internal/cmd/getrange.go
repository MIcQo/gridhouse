package cmd

import (
	"fmt"
	"strconv"

	"gridhouse/internal/resp"
)

// GetRangeHandler implements GETRANGE key start end (inclusive end)
// Negative indices are supported like Redis: -1 is last character, etc.
// If the key does not exist, it is treated as empty string and returns empty bulk string.
func GetRangeHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'GETRANGE' command")
		}
		key := args[0].Str
		start64, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		end64, err := strconv.ParseInt(args[2].Str, 10, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		value, exists := store.Get(key)
		if !exists {
			// Missing key behaves like empty string
			return resp.Value{Type: resp.BulkString, Str: ""}, nil
		}

		// Compute slicing with inclusive end semantics
		n := int64(len(value))
		if n == 0 {
			return resp.Value{Type: resp.BulkString, Str: ""}, nil
		}

		start := start64
		end := end64

		// Handle negative indices
		if start < 0 {
			start = n + start
		}
		if end < 0 {
			end = n + end
		}

		// Clamp to bounds
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}

		// Empty range conditions
		if start > end || start >= n {
			return resp.Value{Type: resp.BulkString, Str: ""}, nil
		}

		// Convert inclusive end to Go slice exclusive end
		res := value[start : end+1]
		return resp.Value{Type: resp.BulkString, Str: res}, nil
	}
}
