package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
)

// DBSizeHandler implements DBSIZE command: returns the number of keys in the current database
func DBSizeHandler(store Store) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'DBSIZE' command")
		}
		count := int64(len(store.Keys()))
		return resp.Value{Type: resp.Integer, Int: count}, nil
	}
}
