package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"strings"
)

// KeysHandler handles the KEYS command
func KeysHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// KEYS command can take 0 or 1 arguments
		if len(args) > 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'KEYS' command")
		}

		// Get all keys from the store
		allKeys := store.Keys()

		// If no pattern provided, return all keys
		if len(args) == 0 {
			return keysToRespArray(allKeys), nil
		}

		// Get the pattern
		pattern := args[0].Str

		// If pattern is empty or "*", return all keys
		if pattern == "" || pattern == "*" {
			return keysToRespArray(allKeys), nil
		}

		// Filter keys based on pattern
		matchedKeys := filterKeysByPattern(allKeys, pattern)

		return keysToRespArray(matchedKeys), nil
	}
}

// keysToRespArray converts a slice of strings to a RESP array
func keysToRespArray(keys []string) resp.Value {
	array := make([]resp.Value, len(keys))
	for i, key := range keys {
		array[i] = resp.Value{Type: resp.BulkString, Str: key}
	}
	return resp.Value{Type: resp.Array, Array: array}
}

// filterKeysByPattern filters keys based on a pattern
// This implements a simple glob-like pattern matching
func filterKeysByPattern(keys []string, pattern string) []string {
	var matchedKeys []string

	for _, key := range keys {
		if matchPattern(key, pattern) {
			matchedKeys = append(matchedKeys, key)
		}
	}

	return matchedKeys
}

// matchPattern checks if a key matches a pattern
// Supports basic glob patterns: * matches any sequence of characters
func matchPattern(key, pattern string) bool {
	// Convert pattern to regex-like matching
	// For now, we'll implement a simple glob matching
	// This can be enhanced later to support more complex patterns

	// Handle exact match
	if pattern == key {
		return true
	}

	// Handle wildcard patterns
	if strings.Contains(pattern, "*") {
		return matchGlobPattern(key, pattern)
	}

	// Handle exact match (no wildcards)
	return key == pattern
}

// matchGlobPattern implements basic glob pattern matching
func matchGlobPattern(key, pattern string) bool {
	// Simple implementation for basic glob patterns
	// This can be enhanced to support more complex patterns like ?, [abc], etc.

	// Split pattern by wildcards
	parts := strings.Split(pattern, "*")

	// If no wildcards, it's an exact match
	if len(parts) == 1 {
		return key == pattern
	}

	// Handle patterns like "prefix*", "*suffix", "prefix*suffix", "*middle*"

	// Check if key starts with the first part (if it exists and is not empty)
	if parts[0] != "" && !strings.HasPrefix(key, parts[0]) {
		return false
	}

	// Check if key ends with the last part (if it exists and is not empty)
	if parts[len(parts)-1] != "" && !strings.HasSuffix(key, parts[len(parts)-1]) {
		return false
	}

	// For patterns with multiple parts, check that all parts appear in order
	if len(parts) > 2 {
		remainingKey := key
		for i := 0; i < len(parts)-1; i++ {
			if parts[i] == "" {
				continue // Skip empty parts (consecutive *s)
			}

			// Find the part in the remaining key
			index := strings.Index(remainingKey, parts[i])
			if index == -1 {
				return false
			}

			// Move to the position after this part
			remainingKey = remainingKey[index+len(parts[i]):]
		}
	}

	return true
}
