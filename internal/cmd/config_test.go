package cmd

import (
	"testing"

	"gridhouse/internal/resp"

	"github.com/stretchr/testify/assert"
)

func TestConfigHandler(t *testing.T) {
	handler := ConfigHandler()

	t.Run("CONFIG with no arguments", func(t *testing.T) {
		args := []resp.Value{}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'CONFIG' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("CONFIG GET with no key", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
		}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'CONFIG GET' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("CONFIG GET port", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "port"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, resp.BulkString, result.Array[0].Type)
		assert.Equal(t, "port", result.Array[0].Str)
		assert.Equal(t, resp.BulkString, result.Array[1].Type)
		assert.Equal(t, "6380", result.Array[1].Str)
	})

	t.Run("CONFIG GET bind", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "bind"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, "bind", result.Array[0].Str)
		assert.Equal(t, "0.0.0.0", result.Array[1].Str)
	})

	t.Run("CONFIG GET timeout", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "timeout"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, "timeout", result.Array[0].Str)
		assert.Equal(t, "0", result.Array[1].Str)
	})

	t.Run("CONFIG GET tcp-keepalive", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "tcp-keepalive"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, "tcp-keepalive", result.Array[0].Str)
		assert.Equal(t, "300", result.Array[1].Str)
	})

	t.Run("CONFIG GET databases", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "databases"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, "databases", result.Array[0].Str)
		assert.Equal(t, "16", result.Array[1].Str)
	})

	t.Run("CONFIG GET appendonly", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "appendonly"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, "appendonly", result.Array[0].Str)
		assert.Equal(t, "true", result.Array[1].Str)
	})

	t.Run("CONFIG GET save", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "save"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, "save", result.Array[0].Str)
		assert.Equal(t, "3600 1 300 100 60 10000", result.Array[1].Str)
	})

	t.Run("CONFIG GET unknown key", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "unknown-key"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 0) // Empty array for unknown keys
	})

	t.Run("CONFIG GET case insensitive", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "get"},
			{Type: resp.BulkString, Str: "port"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 2)
		assert.Equal(t, "port", result.Array[0].Str)
		assert.Equal(t, "6380", result.Array[1].Str)
	})

	t.Run("CONFIG SET with insufficient arguments", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "port"},
		}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "wrong number of arguments for 'CONFIG SET' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("CONFIG SET with correct arguments", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "port"},
			{Type: resp.BulkString, Str: "6381"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)
	})

	t.Run("CONFIG SET case insensitive", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "set"},
			{Type: resp.BulkString, Str: "port"},
			{Type: resp.BulkString, Str: "6381"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)
	})

	t.Run("CONFIG with unknown subcommand", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "UNKNOWN"},
		}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown subcommand or wrong number of arguments for 'CONFIG' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("CONFIG with empty subcommand", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: ""},
		}
		result, err := handler(args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown subcommand or wrong number of arguments for 'CONFIG' command")
		assert.Equal(t, resp.Value{}, result)
	})

	t.Run("CONFIG GET with empty key", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: ""},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 0) // Empty array for empty key
	})

	t.Run("CONFIG SET with empty key and value", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: ""},
			{Type: resp.BulkString, Str: ""},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)
	})

	t.Run("CONFIG GET with special characters in key", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "GET"},
			{Type: resp.BulkString, Str: "special-key-123"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.Array, result.Type)
		assert.Len(t, result.Array, 0) // Empty array for unknown key
	})

	t.Run("CONFIG SET with special characters", func(t *testing.T) {
		args := []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "special-key"},
			{Type: resp.BulkString, Str: "special-value-123"},
		}
		result, err := handler(args)
		assert.NoError(t, err)
		assert.Equal(t, resp.SimpleString, result.Type)
		assert.Equal(t, "OK", result.Str)
	})
}
