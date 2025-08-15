package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
)

// TypeHandler handles TYPE key
func TypeHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'TYPE' command")
		}
		key := args[0].Str
		// If key does not exist, return "none"
		if !ds.Exists(key) {
			return resp.Value{Type: resp.SimpleString, Str: "none"}, nil
		}
		switch ds.GetDataType(key) {
		case store.TypeString:
			return resp.Value{Type: resp.SimpleString, Str: "string"}, nil
		case store.TypeList:
			return resp.Value{Type: resp.SimpleString, Str: "list"}, nil
		case store.TypeSet:
			return resp.Value{Type: resp.SimpleString, Str: "set"}, nil
		case store.TypeHash:
			return resp.Value{Type: resp.SimpleString, Str: "hash"}, nil
		case store.TypeSortedSet:
			return resp.Value{Type: resp.SimpleString, Str: "zset"}, nil
		case store.TypeStream:
			return resp.Value{Type: resp.SimpleString, Str: "stream"}, nil
		default:
			return resp.Value{Type: resp.SimpleString, Str: "none"}, nil
		}
	}
}
