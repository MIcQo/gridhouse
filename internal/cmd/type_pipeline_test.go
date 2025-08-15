package cmd

import (
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypePipelineIssue(t *testing.T) {
	dataStore := newMockDataStore()

	// Set up different data types
	dataStore.Set("str_key", "test", time.Time{})

	list := dataStore.GetOrCreateList("list_key")
	list.LPush("item1")

	hash := dataStore.GetOrCreateHash("hash_key")
	hash.HSet("field1", "value1")

	set := dataStore.GetOrCreateSet("set_key")
	set.SAdd("member1")

	typeHandler := TypeHandler(dataStore)

	// Test TYPE on string
	result, err := typeHandler([]resp.Value{{Type: resp.BulkString, Str: "str_key"}})
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "string", result.Str)

	// Test TYPE on list
	result, err = typeHandler([]resp.Value{{Type: resp.BulkString, Str: "list_key"}})
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "list", result.Str)

	// Test TYPE on hash
	result, err = typeHandler([]resp.Value{{Type: resp.BulkString, Str: "hash_key"}})
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "hash", result.Str)

	// Test TYPE on set
	result, err = typeHandler([]resp.Value{{Type: resp.BulkString, Str: "set_key"}})
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "set", result.Str)

	// Test TYPE on non-existent key
	result, err = typeHandler([]resp.Value{{Type: resp.BulkString, Str: "nonexistent"}})
	require.NoError(t, err)
	assert.Equal(t, resp.SimpleString, result.Type)
	assert.Equal(t, "none", result.Str)

	// Test GetDataType directly
	assert.Equal(t, store.TypeString, dataStore.GetDataType("str_key"))
	assert.Equal(t, store.TypeList, dataStore.GetDataType("list_key"))
	assert.Equal(t, store.TypeHash, dataStore.GetDataType("hash_key"))
	assert.Equal(t, store.TypeSet, dataStore.GetDataType("set_key"))
}
