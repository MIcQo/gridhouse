package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
)

// SaveHandler handles the SAVE command
func SaveHandler(persist PersistenceManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if err := persist.SaveRDB(); err != nil {
			return resp.Value{}, fmt.Errorf("ERR %v", err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}

// BgsaveHandler handles the BGSAVE command
func BgsaveHandler(persist PersistenceManager) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if err := persist.BGSaveAsync(); err != nil {
			return resp.Value{}, fmt.Errorf("ERR %v", err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "Background saving started"}, nil
	}
}
