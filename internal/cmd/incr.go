package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"math"
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

// IncrByHandler handles the INCRBY command: INCRBY key increment
func IncrByHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'INCRBY' command")
		}

		key := args[0].Str
		incrementStr := args[1].Str

		// Parse the increment value
		increment, err := strconv.ParseInt(incrementStr, 10, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		currentValue, exists := store.Get(key)

		var newValue int64
		if !exists {
			// Key doesn't exist, start with 0
			newValue = increment
		} else {
			// Parse current value
			currentInt, err := strconv.ParseInt(currentValue, 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}

			newValue, err = inc(currentInt, increment)
			if err != nil {
				return resp.Value{}, err
			}

			newValue = currentInt + increment
		}

		// Set the new value
		store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
		return resp.Value{Type: resp.Integer, Int: newValue}, nil
	}
}

// DecrByHandler handles the DECRBY command: DECRBY key decrement
func DecrByHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'DECRBY' command")
		}

		key := args[0].Str
		decrementStr := args[1].Str

		// Parse the decrement value
		decrement, err := strconv.ParseInt(decrementStr, 10, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		currentValue, exists := store.Get(key)

		var newValue int64
		if !exists {
			// Key doesn't exist, start with 0
			newValue = -decrement
		} else {
			// Parse current value
			currentInt, err := strconv.ParseInt(currentValue, 10, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}

			newValue, err = decr(currentInt, decrement)
			if err != nil {
				return resp.Value{}, err
			}
		}

		// Set the new value
		store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
		return resp.Value{Type: resp.Integer, Int: newValue}, nil
	}
}

func inc[T int64](left, right T) (T, error) {
	if right > 0 {
		if left > math.MaxInt64-right {
			return 0, fmt.Errorf("ERR increment or decrement would overflow")
		}
	} else {
		if left < math.MinInt64-right {
			return 0, fmt.Errorf("ERR increment or decrement would overflow")
		}
	}
	return left + right, nil
}

func decr[T int64](left, right T) (T, error) {
	if right > 0 {
		if left < math.MinInt64+right {
			return 0, fmt.Errorf("ERR increment or decrement would overflow")
		}
	} else {
		if left < math.MinInt64-right {
			return 0, fmt.Errorf("ERR increment or decrement would overflow")
		}
	}
	return left - right, nil
}
