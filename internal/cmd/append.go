package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"time"
)

// AppendHandler handles the APPEND command: APPEND key value
func AppendHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'APPEND' command")
		}

		key := args[0].Str
		valueToAppend := args[1].Str

		currentValue, exists := store.Get(key)

		if !exists {
			// Key doesn't exist, create new string
			store.Set(key, valueToAppend, time.Time{})
			return resp.Value{Type: resp.Integer, Int: int64(len(valueToAppend))}, nil
		}

		// Key exists, append to existing value
		newValue := currentValue + valueToAppend
		store.Set(key, newValue, time.Time{})
		return resp.Value{Type: resp.Integer, Int: int64(len(newValue))}, nil
	}
}
