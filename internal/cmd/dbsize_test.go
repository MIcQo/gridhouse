package cmd

import (
	"testing"
	"time"

	"gridhouse/internal/resp"
)

func TestDBSizeBasic(t *testing.T) {
	store := newMockDataStore()
	h := DBSizeHandler(store)

	// empty
	res, err := h([]resp.Value{})
	if err != nil || res.Type != resp.Integer || res.Int != 0 {
		t.Fatalf("expected 0, got %+v, err=%v", res, err)
	}

	// add some keys
	store.Set("a", "1", time.Time{})
	store.Set("b", "2", time.Time{})
	store.Set("c", "3", time.Time{})

	res, err = h([]resp.Value{})
	if err != nil || res.Int != 3 {
		t.Fatalf("expected 3, got %+v, err=%v", res, err)
	}
}

func TestDBSizeExcludesExpiredAndArity(t *testing.T) {
	store := newMockDataStore()
	h := DBSizeHandler(store)

	store.Set("k1", "v", time.Time{})
	store.Set("k2", "v", time.Now().Add(-1*time.Second)) // already expired due to past timestamp

	res, err := h([]resp.Value{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Int != 1 {
		t.Fatalf("expected 1 (expired excluded), got %d", res.Int)
	}

	// arity error
	if _, err := h([]resp.Value{{Type: resp.BulkString, Str: "x"}}); err == nil {
		t.Fatalf("expected arity error")
	}
}
