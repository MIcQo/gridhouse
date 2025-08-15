package cmd

import (
	"strings"
	"testing"

	"gridhouse/internal/resp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===== Streams Command Tests =====

func TestXAddAndXLenAndIdOrdering(t *testing.T) {
	store := newMockDataStore()
	xadd := XAddHandler(store)
	xlen := XLenHandler(store)

	// XADD mystream * field1 v1
	res, err := xadd([]resp.Value{
		{Type: resp.BulkString, Str: "mystream"},
		{Type: resp.BulkString, Str: "*"},
		{Type: resp.BulkString, Str: "field1"}, {Type: resp.BulkString, Str: "v1"},
	})
	require.NoError(t, err)
	require.Equal(t, resp.BulkString, res.Type)
	firstID := res.Str
	require.True(t, strings.Contains(firstID, "-"))

	// XLEN mystream => 1
	lenRes, err := xlen([]resp.Value{{Type: resp.BulkString, Str: "mystream"}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), lenRes.Int)

	// Second XADD auto ID
	res, err = xadd([]resp.Value{
		{Type: resp.BulkString, Str: "mystream"},
		{Type: resp.BulkString, Str: "*"},
		{Type: resp.BulkString, Str: "field2"}, {Type: resp.BulkString, Str: "v2"},
	})
	require.NoError(t, err)
	secondID := res.Str
	require.NotEqual(t, firstID, secondID)

	// XLEN mystream => 2
	lenRes, err = xlen([]resp.Value{{Type: resp.BulkString, Str: "mystream"}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), lenRes.Int)

	// Adding with non-increasing explicit ID should error
	_, err = xadd([]resp.Value{
		{Type: resp.BulkString, Str: "mystream"},
		{Type: resp.BulkString, Str: firstID},
		{Type: resp.BulkString, Str: "f"}, {Type: resp.BulkString, Str: "v"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "equal or smaller")
}

func TestXRangeBasicAndCount(t *testing.T) {
	store := newMockDataStore()
	xadd := XAddHandler(store)
	xrange := XRangeHandler(store)

	// Create deterministic IDs
	_, err := xadd([]resp.Value{{Type: resp.BulkString, Str: "s"}, {Type: resp.BulkString, Str: "1000-0"}, {Type: resp.BulkString, Str: "a"}, {Type: resp.BulkString, Str: "1"}})
	require.NoError(t, err)
	_, err = xadd([]resp.Value{{Type: resp.BulkString, Str: "s"}, {Type: resp.BulkString, Str: "1000-1"}, {Type: resp.BulkString, Str: "b"}, {Type: resp.BulkString, Str: "2"}})
	require.NoError(t, err)
	_, err = xadd([]resp.Value{{Type: resp.BulkString, Str: "s"}, {Type: resp.BulkString, Str: "1001-0"}, {Type: resp.BulkString, Str: "c"}, {Type: resp.BulkString, Str: "3"}})
	require.NoError(t, err)

	// XRANGE s - + => 3 entries ordered
	res, err := xrange([]resp.Value{{Type: resp.BulkString, Str: "s"}, {Type: resp.BulkString, Str: "-"}, {Type: resp.BulkString, Str: "+"}})
	require.NoError(t, err)
	require.Equal(t, resp.Array, res.Type)
	require.Equal(t, 3, len(res.Array))
	assert.Equal(t, "1000-0", res.Array[0].Array[0].Str)
	assert.Equal(t, "1000-1", res.Array[1].Array[0].Str)
	assert.Equal(t, "1001-0", res.Array[2].Array[0].Str)

	// XRANGE with COUNT 1 starting at 1000-1 => single entry 1000-1
	res, err = xrange([]resp.Value{{Type: resp.BulkString, Str: "s"}, {Type: resp.BulkString, Str: "1000-1"}, {Type: resp.BulkString, Str: "1001-0"}, {Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "1"}})
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Array))
	assert.Equal(t, "1000-1", res.Array[0].Array[0].Str)
}

func TestXDelAndXTrim(t *testing.T) {
	store := newMockDataStore()
	xadd := XAddHandler(store)
	xdel := XDelHandler(store)
	xlen := XLenHandler(store)
	xrange := XRangeHandler(store)
	xtrim := XTrimHandler(store)

	// Add four entries with explicit IDs
	ids := []string{"1000-0", "1000-1", "1000-2", "1000-3"}
	for i, id := range ids {
		_, err := xadd([]resp.Value{{Type: resp.BulkString, Str: "t"}, {Type: resp.BulkString, Str: id}, {Type: resp.BulkString, Str: "f"}, {Type: resp.BulkString, Str: string(rune('a' + i))}})
		require.NoError(t, err)
	}

	// XDEL t 1000-1 1000-2 => 2
	res, err := xdel([]resp.Value{{Type: resp.BulkString, Str: "t"}, {Type: resp.BulkString, Str: "1000-1"}, {Type: resp.BulkString, Str: "1000-2"}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), res.Int)

	// XLEN should now be 2
	lenRes, err := xlen([]resp.Value{{Type: resp.BulkString, Str: "t"}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), lenRes.Int)

	// XTRIM t MAXLEN 1 => removes 1 (keep newest one: 1000-3)
	trimRes, err := xtrim([]resp.Value{{Type: resp.BulkString, Str: "t"}, {Type: resp.BulkString, Str: "MAXLEN"}, {Type: resp.BulkString, Str: "1"}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), trimRes.Int)

	// Verify only latest remains
	rangeRes, err := xrange([]resp.Value{{Type: resp.BulkString, Str: "t"}, {Type: resp.BulkString, Str: "-"}, {Type: resp.BulkString, Str: "+"}})
	require.NoError(t, err)
	require.Equal(t, 1, len(rangeRes.Array))
	assert.Equal(t, "1000-3", rangeRes.Array[0].Array[0].Str)
}

func TestXReadBasic(t *testing.T) {
	store := newMockDataStore()
	xadd := XAddHandler(store)
	xread := XReadHandler(store)

	// Prepare entries
	_, err := xadd([]resp.Value{{Type: resp.BulkString, Str: "r"}, {Type: resp.BulkString, Str: "2000-0"}, {Type: resp.BulkString, Str: "a"}, {Type: resp.BulkString, Str: "1"}})
	require.NoError(t, err)
	_, err = xadd([]resp.Value{{Type: resp.BulkString, Str: "r"}, {Type: resp.BulkString, Str: "2000-1"}, {Type: resp.BulkString, Str: "b"}, {Type: resp.BulkString, Str: "2"}})
	require.NoError(t, err)
	_, err = xadd([]resp.Value{{Type: resp.BulkString, Str: "r"}, {Type: resp.BulkString, Str: "2001-0"}, {Type: resp.BulkString, Str: "c"}, {Type: resp.BulkString, Str: "3"}})
	require.NoError(t, err)

	// XREAD COUNT 2 STREAMS r 2000-0 => returns two entries (2000-1, 2001-0)
	res, err := xread([]resp.Value{{Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "2"}, {Type: resp.BulkString, Str: "STREAMS"}, {Type: resp.BulkString, Str: "r"}, {Type: resp.BulkString, Str: "2000-0"}})
	require.NoError(t, err)
	require.Equal(t, resp.Array, res.Type)
	require.Equal(t, 1, len(res.Array))
	streamItem := res.Array[0]
	require.Equal(t, resp.Array, streamItem.Type)
	require.Equal(t, 2, len(streamItem.Array))
	assert.Equal(t, "r", streamItem.Array[0].Str)
	entries := streamItem.Array[1]
	require.Equal(t, resp.Array, entries.Type)
	require.Equal(t, 2, len(entries.Array))
	assert.Equal(t, "2000-1", entries.Array[0].Array[0].Str)
	assert.Equal(t, "2001-0", entries.Array[1].Array[0].Str)

	// XREAD STREAMS r 2001-0 => null array (no newer entries)
	res, err = xread([]resp.Value{{Type: resp.BulkString, Str: "STREAMS"}, {Type: resp.BulkString, Str: "r"}, {Type: resp.BulkString, Str: "2001-0"}})
	require.NoError(t, err)
	assert.Equal(t, resp.Array, res.Type)
	assert.True(t, res.IsNull)
}
