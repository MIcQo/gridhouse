package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"strconv"
)

// Hash Commands

// HSetHandler handles the HSET command
func HSetHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 3 || len(args)%2 == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HSET' command")
		}

		key := args[0].Str
		hash := store.GetOrCreateHash(key)

		// Handle multiple field-value pairs
		if len(args) == 3 {
			// Single field-value pair
			field := args[1].Str
			value := args[2].Str
			isNew := hash.HSet(field, value)
			if isNew {
				return resp.Value{Type: resp.Integer, Int: 1}, nil
			}
			return resp.Value{Type: resp.Integer, Int: 0}, nil
		}

		// Multiple field-value pairs
		added := 0
		for i := 1; i < len(args); i += 2 {
			field := args[i].Str
			value := args[i+1].Str
			if hash.HSet(field, value) {
				added++
			}
		}

		return resp.Value{Type: resp.Integer, Int: int64(added)}, nil
	}
}

// HGetHandler handles the HGET command
func HGetHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HGET' command")
		}

		key := args[0].Str
		field := args[1].Str

		hash := store.GetOrCreateHash(key)
		value, exists := hash.HGet(field)
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}

		return resp.Value{Type: resp.BulkString, Str: value}, nil
	}
}

// HDelHandler handles the HDEL command
func HDelHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HDEL' command")
		}

		key := args[0].Str
		fields := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			fields[i] = arg.Str
		}

		hash := store.GetOrCreateHash(key)
		removed := hash.HDel(fields...)

		return resp.Value{Type: resp.Integer, Int: int64(removed)}, nil
	}
}

// HExistsHandler handles the HEXISTS command
func HExistsHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HEXISTS' command")
		}

		key := args[0].Str
		field := args[1].Str

		hash := store.GetOrCreateHash(key)
		exists := hash.HExists(field)

		if exists {
			return resp.Value{Type: resp.Integer, Int: 1}, nil
		}
		return resp.Value{Type: resp.Integer, Int: 0}, nil
	}
}

// HGetAllHandler handles the HGETALL command
func HGetAllHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HGETALL' command")
		}

		key := args[0].Str
		hash := store.GetOrCreateHash(key)
		fields := hash.HGetAll()

		// Convert to RESP array of field-value pairs
		array := make([]resp.Value, len(fields)*2)
		i := 0
		for field, value := range fields {
			array[i] = resp.Value{Type: resp.BulkString, Str: field}
			array[i+1] = resp.Value{Type: resp.BulkString, Str: value}
			i += 2
		}

		return resp.Value{Type: resp.Array, Array: array}, nil
	}
}

// HKeysHandler handles the HKEYS command
func HKeysHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HKEYS' command")
		}

		key := args[0].Str
		hash := store.GetOrCreateHash(key)
		keys := hash.HKeys()

		// Convert to RESP array
		array := make([]resp.Value, len(keys))
		for i, key := range keys {
			array[i] = resp.Value{Type: resp.BulkString, Str: key}
		}

		return resp.Value{Type: resp.Array, Array: array}, nil
	}
}

// HValsHandler handles the HVALS command
func HValsHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HVALS' command")
		}

		key := args[0].Str
		hash := store.GetOrCreateHash(key)
		values := hash.HVals()

		// Convert to RESP array
		array := make([]resp.Value, len(values))
		for i, value := range values {
			array[i] = resp.Value{Type: resp.BulkString, Str: value}
		}

		return resp.Value{Type: resp.Array, Array: array}, nil
	}
}

// HLenHandler handles the HLEN command
func HLenHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HLEN' command")
		}

		key := args[0].Str
		hash := store.GetOrCreateHash(key)
		length := hash.HLen()

		return resp.Value{Type: resp.Integer, Int: int64(length)}, nil
	}
}

// HIncrByHandler handles the HINCRBY command
func HIncrByHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HINCRBY' command")
		}

		key := args[0].Str
		field := args[1].Str
		increment, err := strconv.ParseInt(args[2].Str, 10, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		hash := store.GetOrCreateHash(key)
		newValue, err := hash.HIncrBy(field, increment)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR hash value is not an integer")
		}

		return resp.Value{Type: resp.Integer, Int: newValue}, nil
	}
}

// HIncrByFloatHandler handles the HINCRBYFLOAT command
func HIncrByFloatHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HINCRBYFLOAT' command")
		}

		key := args[0].Str
		field := args[1].Str
		increment, err := strconv.ParseFloat(args[2].Str, 64)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not a valid float")
		}

		hash := store.GetOrCreateHash(key)
		newValue, err := hash.HIncrByFloat(field, increment)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR hash value is not a valid float")
		}

		return resp.Value{Type: resp.BulkString, Str: fmt.Sprintf("%f", newValue)}, nil
	}
}
