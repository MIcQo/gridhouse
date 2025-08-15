package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
)

func TestGetRangeBasic(t *testing.T) {
	store := newMockDataStore()
	store.Set("k", "Hello, world!", time.Time{})
	h := GetRangeHandler(store)

	// GETRANGE k 0 4 => Hello
	res, err := h([]resp.Value{{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "4"}})
	if err != nil || res.Type != resp.BulkString || res.Str != "Hello" {
		t.Fatalf("expected 'Hello', got %+v, err=%v", res, err)
	}

	// inclusive end
	res, err = h([]resp.Value{{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "7"}, {Type: resp.BulkString, Str: "11"}})
	if err != nil || res.Str != "world" {
		t.Fatalf("expected 'world', got %+v, err=%v", res, err)
	}
}

func TestGetRangeNegativeAndBounds(t *testing.T) {
	store := newMockDataStore()
	store.Set("k", "abcdef", time.Time{})
	h := GetRangeHandler(store)

	// -1 is last character
	res, err := h([]resp.Value{{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "-1"}, {Type: resp.BulkString, Str: "-1"}})
	if err != nil || res.Str != "f" {
		t.Fatalf("expected 'f', got %+v, err=%v", res, err)
	}

	// -3 to -2 => de
	res, err = h([]resp.Value{{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "-3"}, {Type: resp.BulkString, Str: "-2"}})
	if err != nil || res.Str != "de" {
		t.Fatalf("expected 'de', got %+v, err=%v", res, err)
	}

	// Out of range clamps
	res, err = h([]resp.Value{{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "-100"}, {Type: resp.BulkString, Str: "100"}})
	if err != nil || res.Str != "abcdef" {
		t.Fatalf("expected full string, got %+v, err=%v", res, err)
	}

	// start > end => empty
	res, err = h([]resp.Value{{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "3"}, {Type: resp.BulkString, Str: "2"}})
	if err != nil || res.Str != "" {
		t.Fatalf("expected empty, got %+v, err=%v", res, err)
	}
}

func TestGetRangeMissingKeyAndErrors(t *testing.T) {
	store := newMockDataStore()
	h := GetRangeHandler(store)

	// Missing key => empty string
	res, err := h([]resp.Value{{Type: resp.BulkString, Str: "nope"}, {Type: resp.BulkString, Str: "0"}, {Type: resp.BulkString, Str: "10"}})
	if err != nil || res.Str != "" {
		t.Fatalf("expected empty string, got %+v, err=%v", res, err)
	}

	// Arity error
	if _, err := h([]resp.Value{{Type: resp.BulkString, Str: "k"}}); err == nil {
		t.Fatalf("expected arity error")
	}

	// Integer parse error
	if _, err := h([]resp.Value{{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "x"}, {Type: resp.BulkString, Str: "1"}}); err == nil {
		t.Fatalf("expected integer parse error")
	}
}
