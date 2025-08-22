package v2

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"gridhouse/internal/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRDBV2RoundTripBasic writes a DB snapshot using the v2 writer, then reads it back
// using the v2 reader and verifies data structures and values.
func TestRDBV2RoundTripBasic(t *testing.T) {
	// Prepare a temporary directory and file path for RDB
	dir := t.TempDir()
	rdbPath := filepath.Join(dir, "test.rdb")

	// Populate source DB with different types
	src := store.NewUltraOptimizedDB()

	// String with a future expiration so it survives round-trip
	exp := time.Now().Add(1 * time.Hour)
	src.Set("str:hello", "world", exp)

	// List
	lst := src.GetOrCreateList("list:nums")
	lst.RPush("one", "two", "three") // Writer writes in given order; Reader uses LPush when loading

	// Set
	set := src.GetOrCreateSet("set:letters")
	set.SAdd("a", "b", "c")

	// Hash
	h := src.GetOrCreateHash("hash:user:1")
	h.HSet("name", "alice")
	h.HSet("age", "42")

	// ZSet
	z := src.GetOrCreateSortedSet("zset:rank")
	z.ZAdd(map[string]float64{"bob": 1.5, "carol": 3.2, "alice": 2.7})

	// Stream
	st := src.GetOrCreateStream("stream:orders")
	_, _ = st.XAdd(&store.StreamID{Ms: 1000, Seq: 1}, map[string]string{"type": "buy", "amount": "10"})
	_, _ = st.XAdd(&store.StreamID{Ms: 1001, Seq: 2}, map[string]string{"type": "sell", "amount": "5"})

	// Write RDB using v2 writer
	w, err := NewWriter(rdbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	keys := src.Keys()
	// Count TTL keys (only string above has TTL)
	var ttlCount uint64
	for _, k := range keys {
		if src.TTL(k) > 0 {
			ttlCount++
		}
	}

	require.NoError(t, w.WriteHeader(uint64(len(keys)), ttlCount))

	for _, k := range keys {
		// expiration only used for strings in Writer; compute absolute expiration
		var expiration time.Time
		if ttl := src.TTL(k); ttl > 0 {
			expiration = time.Now().Add(time.Duration(ttl) * time.Second)
		}
		switch src.GetDataType(k) {
		case store.TypeString:
			v, ok := src.Get(k)
			require.True(t, ok)
			require.NoError(t, w.WriteString(k, v, expiration))
		case store.TypeList:
			vals := src.GetOrCreateList(k).LRange(0, -1)
			require.NoError(t, w.WriteList(k, vals, expiration))
		case store.TypeSet:
			members := src.GetOrCreateSet(k).SMembers()
			require.NoError(t, w.WriteSet(k, members, expiration))
		case store.TypeHash:
			fields := src.GetOrCreateHash(k).HGetAll()
			require.NoError(t, w.WriteHash(k, fields, expiration))
		case store.TypeSortedSet:
			// Build map from ZSet ordered view
			pairs := map[string]float64{}
			arr := src.GetOrCreateSortedSet(k).ZRange(0, -1, true)
			for i := 0; i+1 < len(arr); i += 2 {
				// scores are string-formatted
				// Using ParseFloat would require strconv; but we can read back to validate anyway
				// Simpler: query scores directly
			}
			// Query scores directly
			for _, m := range src.GetOrCreateSortedSet(k).ZRange(0, -1, false) {
				if s, ok := src.GetOrCreateSortedSet(k).ZScore(m); ok {
					pairs[m] = s
				}
			}
			require.NoError(t, w.WriteZSet(k, pairs, expiration))
		case store.TypeStream:
			entries := src.GetOrCreateStream(k).XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
			wEntries := make([]store.StreamEntry, len(entries))
			for i, e := range entries {
				wEntries[i] = store.StreamEntry{ID: e.ID, Fields: e.Fields}
				fmt.Println(wEntries)
			}
			require.NoError(t, w.WriteStream(k, wEntries, expiration))
		}
	}
	require.NoError(t, w.WriteEOF())

	// Read back into a fresh DB
	dst := store.NewUltraOptimizedDB()
	r, err := NewReader(rdbPath)
	require.NoError(t, err)
	require.NoError(t, r.ReadAll(dst))
	require.NoError(t, r.Close())

	// Verify string
	val, ok := dst.Get("str:hello")
	require.True(t, ok, "string key missing after round-trip")
	assert.Equal(t, "world", val)

	// Verify list (Reader used LPush while loading; order becomes reversed)
	gotList := dst.GetOrCreateList("list:nums").LRange(0, -1)
	// Original: [one two three] -> After LPush: [three two one]
	assert.Equal(t, []string{"three", "two", "one"}, gotList)

	// Verify set (order not guaranteed)
	members := dst.GetOrCreateSet("set:letters").SMembers()
	assert.ElementsMatch(t, []string{"a", "b", "c"}, members)

	// Verify hash
	fields := dst.GetOrCreateHash("hash:user:1").HGetAll()
	assert.Equal(t, "alice", fields["name"])
	assert.Equal(t, "42", fields["age"])
	assert.Len(t, fields, 2)

	// Verify zset scores
	z2 := dst.GetOrCreateSortedSet("zset:rank")
	assert.Equal(t, 3, z2.ZCard())
	if s, ok := z2.ZScore("bob"); assert.True(t, ok) {
		assert.InDelta(t, 1.5, s, 1e-9)
	}
	if s, ok := z2.ZScore("carol"); assert.True(t, ok) {
		assert.InDelta(t, 3.2, s, 1e-9)
	}
	if s, ok := z2.ZScore("alice"); assert.True(t, ok) {
		assert.InDelta(t, 2.7, s, 1e-9)
	}

	entries := dst.GetOrCreateStream("stream:orders").
		XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
	assert.NotEmpty(t, entries)

	// Verify IDs and fields preserved
	if assert.Equal(t, len(entries), 2) {
		assert.Equal(t, uint64(1000), entries[0].ID.Ms)
		assert.Equal(t, uint64(1), entries[0].ID.Seq)
		assert.Equal(t, "buy", entries[0].Fields["type"])
		assert.Equal(t, "10", entries[0].Fields["amount"])

		assert.Equal(t, uint64(1001), entries[1].ID.Ms)
		assert.Equal(t, uint64(2), entries[1].ID.Seq)
		assert.Equal(t, "sell", entries[1].Fields["type"])
		assert.Equal(t, "5", entries[1].Fields["amount"])
	}
}
