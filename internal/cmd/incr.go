package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"strconv"
	"time"
)

// OptimizedIncrHandler is a faster version of IncrHandler
func OptimizedIncrHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'INCR' command")
		}

		key := args[0].Str
		currentValue, exists := store.Get(key)

		var newValue int64
		if !exists {
			// Key doesn't exist, start with 0
			newValue = 1
		} else {
			// Parse current value
			currentInt, err := strconv.ParseInt(currentValue, 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}
			newValue = currentInt + 1
		}

		// Set the new value
		store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
		return resp.Value{Type: resp.Integer, Int: newValue}, nil
	}
}

// OptimizedDecrHandler is a faster version of DecrHandler
func OptimizedDecrHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'DECR' command")
		}

		key := args[0].Str
		currentValue, exists := store.Get(key)

		var newValue int64
		if !exists {
			// Key doesn't exist, start with 0
			newValue = -1
		} else {
			// Parse current value
			currentInt, err := strconv.ParseInt(currentValue, 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}
			newValue = currentInt - 1
		}

		// Set the new value
		store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
		return resp.Value{Type: resp.Integer, Int: newValue}, nil
	}
}
