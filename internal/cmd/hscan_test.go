package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
)

func TestHScanBasic(t *testing.T) {
	store := newMockDataStore()
	// Populate hash with 23 fields
	h := store.GetOrCreateHash("myhash")
	for i := 0; i < 23; i++ {
		h.HSet("f"+strconvI(i), "v"+strconvI(i))
	}

	hscan := HScanHandler(store)
	res, err := hscan([]resp.Value{{Type: resp.BulkString, Str: "myhash"}, {Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "7"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Type != resp.Array || len(res.Array) != 2 {
		t.Fatalf("unexpected response shape")
	}
	next := res.Array[0].Str
	// first page may complete or paginate depending on count and map order
	if next == "0" && len(res.Array[1].Array) != 46 { // 23 pairs -> 46 entries
		t.Fatalf("expected pagination or full hash on first call")
	}

	// Continue until cursor is 0 and count seen equals size
	cursor := next
	seenPairs := len(res.Array[1].Array) / 2
	for i := 0; i < 10 && cursor != "0"; i++ {
		res, err = hscan([]resp.Value{{Type: resp.BulkString, Str: "myhash"}, {Type: resp.BulkString, Str: cursor}, {Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "7"}})
		if err != nil {
			t.Fatalf("scan err: %v", err)
		}
		cursor = res.Array[0].Str
		seenPairs += len(res.Array[1].Array) / 2
	}
	if cursor != "0" {
		t.Fatalf("did not complete scan; cursor=%s", cursor)
	}
	if seenPairs != 23 {
		t.Fatalf("expected to see all fields, seen=%d", seenPairs)
	}
}

func TestHScanMatchAndMissing(t *testing.T) {
	store := newMockDataStore()
	h := store.GetOrCreateHash("letters")
	h.HSet("aa", "1")
	h.HSet("ab", "1")
	h.HSet("ba", "1")
	h.HSet("bb", "1")
	hscan := HScanHandler(store)
	// MATCH a*
	res, err := hscan([]resp.Value{{Type: resp.BulkString, Str: "letters"}, {Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "MATCH"}, {Type: resp.BulkString, Str: "a*"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Array[0].Str != "0" {
		t.Fatalf("expected completed scan")
	}
	if got := len(res.Array[1].Array); got != 4 { // 2 pairs -> 4 entries
		t.Fatalf("expected 2 matched fields (4 entries), got %d", got)
	}
	// Missing key
	res, err = hscan([]resp.Value{{Type: resp.BulkString, Str: "nosuch"}, {Type: resp.BulkString, Str: "0"}})
	if err != nil {
		t.Fatalf("unexpected err for missing: %v", err)
	}
	if res.Array[0].Str != "0" || len(res.Array[1].Array) != 0 {
		t.Fatalf("expected empty scan for missing key")
	}
}

func TestHScanWrongType(t *testing.T) {
	store := newMockDataStore()
	store.Set("s", "v", time.Time{})
	hscan := HScanHandler(store)
	_, err := hscan([]resp.Value{{Type: resp.BulkString, Str: "s"}, {Type: resp.BulkString, Str: "0"}})
	if err == nil {
		t.Fatalf("expected WRONGTYPE error")
	}
}
