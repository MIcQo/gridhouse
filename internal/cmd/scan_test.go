package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
)

func TestScanBasicCursorAndCount(t *testing.T) {
	store := newMockDataStore()
	// Populate 25 keys
	for i := 0; i < 25; i++ {
		store.Set("k"+string(rune('a'+(i%26)))+":"+strconvI(i), "v", time.Time{})
	}
	scan := ScanHandler(store)

	// First page: SCAN 0 COUNT 10
	res, err := scan([]resp.Value{{Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "10"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Type != resp.Array || len(res.Array) != 2 {
		t.Fatalf("unexpected response shape")
	}
	next := res.Array[0].Str
	if next == "0" {
		t.Fatalf("expected non-zero next cursor for first page")
	}
	if res.Array[1].Type != resp.Array {
		t.Fatalf("expected keys array")
	}
	if len(res.Array[1].Array) == 0 {
		t.Fatalf("expected some keys")
	}

	// Continue until cursor becomes 0
	cursor := next
	seen := len(res.Array[1].Array)
	for i := 0; i < 10 && cursor != "0"; i++ {
		res, err = scan([]resp.Value{{Type: resp.BulkString, Str: cursor}, {Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "10"}})
		if err != nil {
			t.Fatalf("scan err: %v", err)
		}
		cursor = res.Array[0].Str
		seen += len(res.Array[1].Array)
	}
	if cursor != "0" {
		t.Fatalf("did not complete scan; cursor=%s", cursor)
	}
	if seen == 0 {
		t.Fatalf("expected to see keys")
	}
}

func TestScanMatchFilter(t *testing.T) {
	store := newMockDataStore()
	store.Set("user:1", "a", time.Time{})
	store.Set("user:2", "b", time.Time{})
	store.Set("session:1", "c", time.Time{})

	scan := ScanHandler(store)
	// SCAN 0 MATCH user:*
	res, err := scan([]resp.Value{{Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "MATCH"}, {Type: resp.BulkString, Str: "user:*"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Type != resp.Array || len(res.Array) != 2 {
		t.Fatalf("unexpected response shape")
	}
	// Next cursor should be 0 because default COUNT is 10 (> number of matched keys)
	if res.Array[0].Str != "0" {
		t.Fatalf("expected next cursor 0, got %s", res.Array[0].Str)
	}
	if len(res.Array[1].Array) != 2 {
		t.Fatalf("expected 2 matched keys, got %d", len(res.Array[1].Array))
	}
}

// helper to avoid importing strconv in tests multiple times
func strconvI(i int) string {
	// simple int to string
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	x := i
	for x > 0 {
		n--
		buf[n] = byte('0' + (x % 10))
		x /= 10
	}
	return string(buf[n:])
}

func TestScanTypeFilter(t *testing.T) {
	store := newMockDataStore()
	// create different types
	store.Set("s1", "v", time.Time{})
	store.GetOrCreateList("l1").LPush("a")
	store.GetOrCreateList("l2").LPush("b")
	store.GetOrCreateSet("set1").SAdd("m")

	scan := ScanHandler(store)
	// SCAN 0 TYPE list
	res, err := scan([]resp.Value{{Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "TYPE"}, {Type: resp.BulkString, Str: "list"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Type != resp.Array || len(res.Array) != 2 {
		t.Fatalf("unexpected response shape")
	}
	// ensure only list keys returned
	for _, v := range res.Array[1].Array {
		k := v.Str
		if k != "l1" && k != "l2" {
			t.Fatalf("unexpected key in TYPE list scan: %s", k)
		}
	}
}
