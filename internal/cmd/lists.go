package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"strconv"
)

// LPushHandler handles the LPUSH command
func LPushHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LPUSH' command")
		}

		key := args[0].Str
		elements := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			elements[i] = arg.Str
		}

		list := store.GetOrCreateList(key)
		length := list.LPush(elements...)

		return resp.Value{Type: resp.Integer, Int: int64(length)}, nil
	}
}

// RPushHandler handles the RPUSH command
func RPushHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'RPUSH' command")
		}

		key := args[0].Str
		elements := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			elements[i] = arg.Str
		}

		list := store.GetOrCreateList(key)
		length := list.RPush(elements...)

		return resp.Value{Type: resp.Integer, Int: int64(length)}, nil
	}
}

// LPopHandler handles the LPOP command
func LPopHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LPOP' command")
		}

		key := args[0].Str
		list := store.GetOrCreateList(key)

		element, exists := list.LPop()
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}

		return resp.Value{Type: resp.BulkString, Str: element}, nil
	}
}

// RPopHandler handles the RPOP command
func RPopHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'RPOP' command")
		}

		key := args[0].Str
		list := store.GetOrCreateList(key)

		element, exists := list.RPop()
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}

		return resp.Value{Type: resp.BulkString, Str: element}, nil
	}
}

// LLenHandler handles the LLEN command
func LLenHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LLEN' command")
		}

		key := args[0].Str
		list := store.GetOrCreateList(key)
		length := list.LLen()

		return resp.Value{Type: resp.Integer, Int: int64(length)}, nil
	}
}

// LRangeHandler handles the LRANGE command
func LRangeHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LRANGE' command")
		}

		key := args[0].Str
		start, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		stop, err := strconv.Atoi(args[2].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		list := store.GetOrCreateList(key)
		elements := list.LRange(start, stop)

		// Convert to RESP array
		array := make([]resp.Value, len(elements))
		for i, element := range elements {
			array[i] = resp.Value{Type: resp.BulkString, Str: element}
		}

		return resp.Value{Type: resp.Array, Array: array}, nil
	}
}

// LIndexHandler handles the LINDEX command
func LIndexHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LINDEX' command")
		}

		key := args[0].Str
		index, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		list := store.GetOrCreateList(key)
		element, exists := list.LIndex(index)
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}

		return resp.Value{Type: resp.BulkString, Str: element}, nil
	}
}

// LSetHandler handles the LSET command
func LSetHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LSET' command")
		}

		key := args[0].Str
		index, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		value := args[2].Str
		list := store.GetOrCreateList(key)

		success := list.LSet(index, value)
		if !success {
			return resp.Value{}, fmt.Errorf("ERR index out of range")
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// LRemHandler handles the LREM command
func LRemHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LREM' command")
		}

		key := args[0].Str
		count, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		value := args[2].Str
		list := store.GetOrCreateList(key)
		removed := list.LRem(count, value)

		return resp.Value{Type: resp.Integer, Int: int64(removed)}, nil
	}
}

// LTrimHandler handles the LTRIM command
func LTrimHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'LTRIM' command")
		}

		key := args[0].Str
		start, err := strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		stop, err := strconv.Atoi(args[2].Str)
		if err != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		list := store.GetOrCreateList(key)
		list.LTrim(start, stop)

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}
