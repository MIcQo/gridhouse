package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"strconv"
	"strings"
	"time"
)

func EchoHandler() Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ECHO' command")
		}
		return resp.Value{Type: resp.BulkString, Str: args[0].Str}, nil
	}
}

func PingHandler() Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) > 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'PING' command")
		}
		if len(args) == 1 {
			// For compatibility with existing tests, return SimpleString for PING with message
			return resp.Value{Type: resp.SimpleString, Str: args[0].Str}, nil
		}
		return resp.Value{Type: resp.SimpleString, Str: "PONG"}, nil
	}
}

// OptimizedSetHandler  not used besides tests, because of performance reasons
func OptimizedSetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SET' command")
		}

		key := args[0].Str
		value := args[1].Str
		expiration := time.Time{}

		// Parse optional expiration (optimized)
		for i := 2; i < len(args); i++ {
			if i+1 >= len(args) {
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}

			option := args[i].Str
			// Fast string comparison instead of ToUpper
			if len(option) == 2 && option[0] == 'E' && option[1] == 'X' {
				sec, err := strconv.Atoi(args[i+1].Str)
				if err != nil {
					return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
				}
				expiration = time.Now().Add(time.Duration(sec) * time.Second)
				i++ // Skip the next argument since we consumed it
			} else if len(option) == 2 && option[0] == 'P' && option[1] == 'X' {
				msec, err := strconv.Atoi(args[i+1].Str)
				if err != nil {
					return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
				}
				expiration = time.Now().Add(time.Duration(msec) * time.Millisecond)
				i++ // Skip the next argument since we consumed it
			} else {
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}
		}

		store.Set(key, value, expiration)
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// OptimizedGetHandler  not used besides tests, because of performance reasons
func OptimizedGetHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'GET' command")
		}

		key := args[0].Str
		value, exists := store.Get(key)
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}
		return resp.Value{Type: resp.BulkString, Str: value}, nil
	}
}

// OptimizedDelHandler is a faster version of DelHandler
func OptimizedDelHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'DEL' command")
		}

		deleted := 0
		for _, arg := range args {
			if store.Del(arg.Str) {
				deleted++
			}
		}
		return resp.Value{Type: resp.Integer, Int: int64(deleted)}, nil
	}
}

// OptimizedKeysHandler handles the KEYS command with optimized pattern matching
func OptimizedKeysHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) > 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'KEYS' command")
		}

		allKeys := store.Keys()

		// Fast path: no pattern or wildcard
		if len(args) == 0 {
			return keysToRespArrayOptimized(allKeys), nil
		}

		pattern := args[0].Str
		if pattern == "" || pattern == "*" {
			return keysToRespArrayOptimized(allKeys), nil
		}

		// Optimized pattern matching
		matchedKeys := make([]string, 0, len(allKeys)/2) // Estimate capacity
		for _, key := range allKeys {
			if matchPatternOptimized(key, pattern) {
				matchedKeys = append(matchedKeys, key)
			}
		}

		return keysToRespArrayOptimized(matchedKeys), nil
	}
}

// OptimizedExistsHandler is a faster version of ExistsHandler
func OptimizedExistsHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'EXISTS' command")
		}

		exists := 0
		for _, arg := range args {
			if store.Exists(arg.Str) {
				exists++
			}
		}
		return resp.Value{Type: resp.Integer, Int: int64(exists)}, nil
	}
}

// OptimizedTTLHandler is a faster version of TTLHandler
func OptimizedTTLHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'TTL' command")
		}

		key := args[0].Str
		ttl := store.TTL(key)
		return resp.Value{Type: resp.Integer, Int: ttl}, nil
	}
}

// OptimizedPTTLHandler is a faster version of PTTLHandler
func OptimizedPTTLHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'PTTL' command")
		}

		key := args[0].Str
		pttl := store.PTTL(key)
		return resp.Value{Type: resp.Integer, Int: pttl}, nil
	}
}

// OptimizedExpireHandler is a faster version of ExpireHandler
func OptimizedExpireHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'EXPIRE' command")
		}

		key := args[0].Str
		seconds, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		success := store.Expire(key, time.Duration(seconds)*time.Second)
		if success {
			return resp.Value{Type: resp.Integer, Int: 1}, nil
		}
		return resp.Value{Type: resp.Integer, Int: 0}, nil
	}
}

// keysToRespArrayOptimized converts keys to RESP array with pre-allocation
func keysToRespArrayOptimized(keys []string) resp.Value {
	array := make([]resp.Value, len(keys))
	for i, key := range keys {
		array[i] = resp.Value{Type: resp.BulkString, Str: key}
	}
	return resp.Value{Type: resp.Array, Array: array}
}

// matchPatternOptimized - optimized pattern matching
func matchPatternOptimized(key, pattern string) bool {
	// Simple optimization: exact match
	if pattern == key {
		return true
	}

	// Wildcard patterns - simplified for performance
	if strings.Contains(pattern, "*") {
		// For now, implement simple prefix/suffix matching
		if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
			middle := pattern[1 : len(pattern)-1]
			return strings.Contains(key, middle)
		} else if strings.HasPrefix(pattern, "*") {
			suffix := pattern[1:]
			return strings.HasSuffix(key, suffix)
		} else if strings.HasSuffix(pattern, "*") {
			prefix := pattern[:len(pattern)-1]
			return strings.HasPrefix(key, prefix)
		}
	}

	return false
}
