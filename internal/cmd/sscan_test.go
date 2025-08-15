package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
)

func TestSScanBasic(t *testing.T) {
	store := newMockDataStore()
	// Populate set with 23 members
	set := store.GetOrCreateSet("myset")
	for i := 0; i < 23; i++ {
		set.SAdd("m" + strconvI(i))
	}

	sscan := SScanHandler(store)
	res, err := sscan([]resp.Value{{Type: resp.BulkString, Str: "myset"}, {Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "7"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Type != resp.Array || len(res.Array) != 2 {
		t.Fatalf("unexpected response shape")
	}
	next := res.Array[0].Str
	if next == "0" && len(res.Array[1].Array) != 23 {
		t.Fatalf("expected pagination or full set on first call")
	}

	// Continue until cursor is 0 and count seen equals size
	cursor := next
	seen := len(res.Array[1].Array)
	for i := 0; i < 10 && cursor != "0"; i++ {
		res, err = sscan([]resp.Value{{Type: resp.BulkString, Str: "myset"}, {Type: resp.BulkString, Str: cursor}, {Type: resp.BulkString, Str: "COUNT"}, {Type: resp.BulkString, Str: "7"}})
		if err != nil {
			t.Fatalf("scan err: %v", err)
		}
		cursor = res.Array[0].Str
		seen += len(res.Array[1].Array)
	}
	if cursor != "0" {
		t.Fatalf("did not complete scan; cursor=%s", cursor)
	}
	if seen != 23 {
		t.Fatalf("expected to see all members, seen=%d", seen)
	}
}

func TestSScanMatchAndMissing(t *testing.T) {
	store := newMockDataStore()
	set := store.GetOrCreateSet("letters")
	for _, m := range []string{"aa", "ab", "ba", "bb"} {
		set.SAdd(m)
	}
	sscan := SScanHandler(store)
	// MATCH a*
	res, err := sscan([]resp.Value{{Type: resp.BulkString, Str: "letters"}, {Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "MATCH"}, {Type: resp.BulkString, Str: "a*"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Array[0].Str != "0" {
		t.Fatalf("expected completed scan")
	}
	if got := len(res.Array[1].Array); got != 2 {
		t.Fatalf("expected 2 matches, got %d", got)
	}
	// Missing key
	res, err = sscan([]resp.Value{{Type: resp.BulkString, Str: "nosuch"}, {Type: resp.BulkString, Str: "0"}})
	if err != nil {
		t.Fatalf("unexpected err for missing: %v", err)
	}
	if res.Array[0].Str != "0" || len(res.Array[1].Array) != 0 {
		t.Fatalf("expected empty scan for missing key")
	}
}

func TestSScanWrongType(t *testing.T) {
	store := newMockDataStore()
	store.Set("s", "v", time.Time{})
	sscan := SScanHandler(store)
	_, err := sscan([]resp.Value{{Type: resp.BulkString, Str: "s"}, {Type: resp.BulkString, Str: "0"}})
	if err == nil {
		t.Fatalf("expected WRONGTYPE error")
	}
}
