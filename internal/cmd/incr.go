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

		if !exists {
			// Key doesn't exist, start with 1
			store.Set(key, "1", time.Time{})
			return resp.Value{Type: resp.Integer, Int: 1}, nil
		}

		// Try to parse as integer first
		if currentInt, err := strconv.ParseInt(currentValue, 10, 64); err == nil {
			newValue := currentInt + 1
			store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
			return resp.Value{Type: resp.Integer, Int: newValue}, nil
		}

		// Try to parse as float
		if currentFloat, err := strconv.ParseFloat(currentValue, 64); err == nil {
			newValue := currentFloat + 1.0
			// Format the result - remove trailing .0 if it's an integer
			var resultStr string
			if newValue == float64(int64(newValue)) {
				resultStr = fmt.Sprintf("%d", int64(newValue))
			} else {
				resultStr = fmt.Sprintf("%.10g", newValue)
			}
			store.Set(key, resultStr, time.Time{})
			return resp.Value{Type: resp.BulkString, Str: resultStr}, nil
		}

		return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
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

		if !exists {
			// Key doesn't exist, start with -1
			store.Set(key, "-1", time.Time{})
			return resp.Value{Type: resp.Integer, Int: -1}, nil
		}

		// Try to parse as integer first
		if currentInt, err := strconv.ParseInt(currentValue, 10, 64); err == nil {
			newValue := currentInt - 1
			store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
			return resp.Value{Type: resp.Integer, Int: newValue}, nil
		}

		// Try to parse as float
		if currentFloat, err := strconv.ParseFloat(currentValue, 64); err == nil {
			newValue := currentFloat - 1.0
			// Format the result - remove trailing .0 if it's an integer
			var resultStr string
			if newValue == float64(int64(newValue)) {
				resultStr = fmt.Sprintf("%d", int64(newValue))
			} else {
				resultStr = fmt.Sprintf("%.10g", newValue)
			}
			store.Set(key, resultStr, time.Time{})
			return resp.Value{Type: resp.BulkString, Str: resultStr}, nil
		}

		return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
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

		// Parse the increment value as integer only
		increment, err := strconv.ParseInt(incrementStr, 10, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		currentValue, exists := store.Get(key)

		if !exists {
			// Key doesn't exist, start with increment
			store.Set(key, fmt.Sprintf("%d", increment), time.Time{})
			return resp.Value{Type: resp.Integer, Int: increment}, nil
		}

		// Try to parse current value as integer
		if currentInt, err := strconv.ParseInt(currentValue, 10, 64); err == nil {
			newValue, err := inc(currentInt, increment)
			if err != nil {
				return resp.Value{}, err
			}
			store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
			return resp.Value{Type: resp.Integer, Int: newValue}, nil
		}

		// Try to parse current value as float
		if currentFloat, err := strconv.ParseFloat(currentValue, 64); err == nil {
			newValue := currentFloat + float64(increment)
			// Format the result - remove trailing .0 if it's an integer
			var resultStr string
			if newValue == float64(int64(newValue)) {
				resultStr = fmt.Sprintf("%d", int64(newValue))
			} else {
				resultStr = fmt.Sprintf("%.10g", newValue)
			}
			store.Set(key, resultStr, time.Time{})
			return resp.Value{Type: resp.BulkString, Str: resultStr}, nil
		}

		return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
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

		// Parse the decrement value as integer only
		decrement, err := strconv.ParseInt(decrementStr, 10, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		currentValue, exists := store.Get(key)

		if !exists {
			// Key doesn't exist, start with -decrement
			store.Set(key, fmt.Sprintf("%d", -decrement), time.Time{})
			return resp.Value{Type: resp.Integer, Int: -decrement}, nil
		}

		// Try to parse current value as integer
		if currentInt, err := strconv.ParseInt(currentValue, 10, 64); err == nil {
			newValue, err := decr(currentInt, decrement)
			if err != nil {
				return resp.Value{}, err
			}
			store.Set(key, fmt.Sprintf("%d", newValue), time.Time{})
			return resp.Value{Type: resp.Integer, Int: newValue}, nil
		}

		// Try to parse current value as float
		if currentFloat, err := strconv.ParseFloat(currentValue, 64); err == nil {
			newValue := currentFloat - float64(decrement)
			// Format the result - remove trailing .0 if it's an integer
			var resultStr string
			if newValue == float64(int64(newValue)) {
				resultStr = fmt.Sprintf("%d", int64(newValue))
			} else {
				resultStr = fmt.Sprintf("%.10g", newValue)
			}
			store.Set(key, resultStr, time.Time{})
			return resp.Value{Type: resp.BulkString, Str: resultStr}, nil
		}

		return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
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

// IncrByFloatHandler handles the INCRBYFLOAT command: INCRBYFLOAT key increment
func IncrByFloatHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'INCRBYFLOAT' command")
		}

		key := args[0].Str
		incrementStr := args[1].Str

		// Parse the increment value
		increment, err := strconv.ParseFloat(incrementStr, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not a valid float")
		}

		currentValue, exists := store.Get(key)

		var newValue float64
		if !exists {
			// Key doesn't exist, start with 0
			newValue = increment
		} else {
			// Parse current value
			currentFloat, err := strconv.ParseFloat(currentValue, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not a valid float")
			}

			// Check for overflow
			if (increment > 0 && currentFloat > math.MaxFloat64-increment) ||
				(increment < 0 && currentFloat < -math.MaxFloat64-increment) {
				return resp.Value{}, fmt.Errorf("ERR value is not a valid float")
			}

			newValue = currentFloat + increment
		}

		// Format the result - use %f for better precision control
		var resultStr string
		if newValue == float64(int64(newValue)) {
			resultStr = fmt.Sprintf("%d", int64(newValue))
		} else {
			// Use %f to avoid scientific notation for large numbers
			resultStr = fmt.Sprintf("%.10g", newValue)
		}

		// Set the new value
		store.Set(key, resultStr, time.Time{})
		return resp.Value{Type: resp.BulkString, Str: resultStr}, nil
	}
}
