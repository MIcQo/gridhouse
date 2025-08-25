package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
)

// ConfigHandler handles the CONFIG command
func ConfigHandler() Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'CONFIG' command")
		}

		subcommand := args[0].Str
		switch subcommand {
		case "GET", "get":
			if len(args) < 2 {
				return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'CONFIG GET' command")
			}

			// Return proper format for CONFIG GET (key-value pairs)
			configKey := args[1].Str
			var configValue string
			switch configKey {
			case "port":
				configValue = "6380"
			case "bind":
				configValue = "0.0.0.0"
			case "timeout":
				configValue = "0"
			case "tcp-keepalive":
				configValue = "300"
			case "databases":
				configValue = "16"
			case "appendonly":
				configValue = "true"
			case "save":
				configValue = "3600 1 300 100 60 10000"
			default:
				configValue = ""
			}

			if configValue == "" {
				return resp.Value{Type: resp.Array, Array: []resp.Value{}}, nil
			}

			return resp.Value{Type: resp.Array, Array: []resp.Value{
				{Type: resp.BulkString, Str: configKey},
				{Type: resp.BulkString, Str: configValue},
			}}, nil
		case "SET", "set":
			if len(args) < 3 {
				return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'CONFIG SET' command")
			}
			return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
		default:
			return resp.Value{}, fmt.Errorf("ERR unknown subcommand or wrong number of arguments for 'CONFIG' command")
		}
	}
}
