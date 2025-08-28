package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
)

// StrlenHandler handles the STRLEN command: STRLEN key
func StrlenHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'STRLEN' command")
		}

		key := args[0].Str
		value, exists := store.Get(key)

		if !exists {
			// Return 0 for non-existent keys
			return resp.Value{Type: resp.Integer, Int: 0}, nil
		}

		// Return the length of the string
		return resp.Value{Type: resp.Integer, Int: int64(len(value))}, nil
	}
}
