package cmd

import (
	"gridhouse/internal/resp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===== Sorted Set Command Tests =====

func TestZAddCardRangeHandlers(t *testing.T) {
	store := newMockDataStore()
	zadd := ZAddHandler(store)
	zcard := ZCardHandler(store)
	zrange := ZRangeHandler(store)

	// ZADD myz 1 one 2 two 1.5 mid
	args := []resp.Value{
		{Type: resp.BulkString, Str: "myz"},
		{Type: resp.BulkString, Str: "1"}, {Type: resp.BulkString, Str: "one"},
		{Type: resp.BulkString, Str: "2"}, {Type: resp.BulkString, Str: "two"},
		{Type: resp.BulkString, Str: "1.5"}, {Type: resp.BulkString, Str: "mid"},
	}
	res, err := zadd(args)
	require.NoError(t, err)
	assert.Equal(t, resp.Integer, res.Type)
	assert.Equal(t, int64(3), res.Int)

	// ZCARD myz => 3
	res, err = zcard([]resp.Value{{Type: resp.BulkString, Str: "myz"}})
	require.NoError(t, err)
	assert.Equal(t, int64(3), res.Int)

	// ZRANGE myz 0 -1 WITHSCORES => one 1 mid 1.5 two 2
	res, err = zrange([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "-1"}, {Type: resp.BulkString, Str: "WITHSCORES"}})
	require.NoError(t, err)
	require.Equal(t, resp.Array, res.Type)
	require.Equal(t, 6, len(res.Array))
	// order check
	assert.Equal(t, "one", res.Array[0].Str)
	assert.Equal(t, "1", res.Array[1].Str)
	assert.Equal(t, "mid", res.Array[2].Str)
	assert.Equal(t, "1.5", res.Array[3].Str)
	assert.Equal(t, "two", res.Array[4].Str)
	assert.Equal(t, "2", res.Array[5].Str)
}

func TestZAddErrorsAndZScoreHandler(t *testing.T) {
	store := newMockDataStore()
	zadd := ZAddHandler(store)
	zscore := ZScoreHandler(store)

	// Wrong arity: need key score member
	_, err := zadd([]resp.Value{{Type: resp.BulkString, Str: "myz"}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong number of arguments")

	// Invalid score
	_, err = zadd([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "oops"}, {Type: resp.BulkString, Str: "a"}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid float")

	// Valid add, then score fetch
	_, err = zadd([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "3.14"}, {Type: resp.BulkString, Str: "pi"}})
	require.NoError(t, err)
	res, err := zscore([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "pi"}})
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, res.Type)
	assert.Equal(t, "3.14", res.Str)

	// Missing member returns null bulk
	res, err = zscore([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "missing"}})
	require.NoError(t, err)
	assert.Equal(t, resp.BulkString, res.Type)
	assert.True(t, res.IsNull)
}

func TestZRemAndPopMinHandlers(t *testing.T) {
	store := newMockDataStore()
	zadd := ZAddHandler(store)
	zrem := ZRemHandler(store)
	zcard := ZCardHandler(store)
	zpopmin := ZPopMinHandler(store)

	// prepare data
	_, err := zadd([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "1"}, {Type: resp.BulkString, Str: "a"}, {Type: resp.BulkString, Str: "2"}, {Type: resp.BulkString, Str: "b"}, {Type: resp.BulkString, Str: "3"}, {Type: resp.BulkString, Str: "c"}})
	require.NoError(t, err)

	// ZREM myz b
	res, err := zrem([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "b"}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.Int)

	// ZCARD should be 2
	res, err = zcard([]resp.Value{{Type: resp.BulkString, Str: "myz"}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Int)

	// ZPOPMIN myz 2 => a 1, c 3 (since b removed, order a(1), c(3))
	res, err = zpopmin([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "2"}})
	require.NoError(t, err)
	require.Equal(t, resp.Array, res.Type)
	require.Equal(t, 4, len(res.Array))
	assert.Equal(t, "a", res.Array[0].Str)
	assert.Equal(t, "1", res.Array[1].Str)
	assert.Equal(t, "c", res.Array[2].Str)
	assert.Equal(t, "3", res.Array[3].Str)

	// ZPOPMIN with invalid count
	_, err = zpopmin([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "x"}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not an integer")

	// ZPOPMIN with non-positive count returns empty array
	res, err = zpopmin([]resp.Value{{Type: resp.BulkString, Str: "myz"}, {Type: resp.BulkString, Str: "0"}})
	require.NoError(t, err)
	require.Equal(t, 0, len(res.Array))
}
