package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
)

func TestTypeCommandBasicAndStructures(t *testing.T) {
	store := newMockDataStore()
	th := TypeHandler(store)

	// missing key
	res, err := th([]resp.Value{{Type: resp.BulkString, Str: "nope"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Type != resp.SimpleString || res.Str != "none" {
		t.Fatalf("expected none, got %+v", res)
	}

	// string
	store.Set("s", "v", time.Time{})
	res, err = th([]resp.Value{{Type: resp.BulkString, Str: "s"}})
	if err != nil || res.Str != "string" {
		t.Fatalf("expected string, got %+v, err=%v", res, err)
	}

	// list
	store.GetOrCreateList("l").LPush("a")
	res, err = th([]resp.Value{{Type: resp.BulkString, Str: "l"}})
	if err != nil || res.Str != "list" {
		t.Fatalf("expected list, got %+v, err=%v", res, err)
	}

	// set
	store.GetOrCreateSet("set").SAdd("x")
	res, err = th([]resp.Value{{Type: resp.BulkString, Str: "set"}})
	if err != nil || res.Str != "set" {
		t.Fatalf("expected set, got %+v, err=%v", res, err)
	}

	// hash
	store.GetOrCreateHash("h").HSet("f", "v")
	res, err = th([]resp.Value{{Type: resp.BulkString, Str: "h"}})
	if err != nil || res.Str != "hash" {
		t.Fatalf("expected hash, got %+v, err=%v", res, err)
	}

	// zset
	store.GetOrCreateSortedSet("z").ZAdd(map[string]float64{"m": 1})
	res, err = th([]resp.Value{{Type: resp.BulkString, Str: "z"}})
	if err != nil || res.Str != "zset" {
		t.Fatalf("expected zset, got %+v, err=%v", res, err)
	}

	// stream
	store.GetOrCreateStream("st") // creation sufficient
	res, err = th([]resp.Value{{Type: resp.BulkString, Str: "st"}})
	if err != nil || res.Str != "stream" {
		t.Fatalf("expected stream, got %+v, err=%v", res, err)
	}
}

func TestTypeArity(t *testing.T) {
	store := newMockDataStore()
	th := TypeHandler(store)
	if _, err := th([]resp.Value{}); err == nil {
		t.Fatalf("expected arity error")
	}
}
