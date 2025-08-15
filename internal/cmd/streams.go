package cmd

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"gridhouse/internal/resp"
	"gridhouse/internal/store"
)

// XAddHandler handles XADD key id field value [field value ...]
// Minimal options: supports id of "*" or explicit "ms-seq".
func XAddHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		// need at least: key, id, field, value
		if len(args) < 4 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XADD' command")
		}
		key := args[0].Str
		idStr := strings.ToLower(args[1].Str)

		// parse field-value pairs
		if (len(args)-2)%2 != 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XADD' command")
		}

		fields := make(map[string]string, (len(args)-2)/2)
		for i := 2; i < len(args); i += 2 {
			field := args[i].Str
			value := args[i+1].Str
			fields[field] = value
		}

		stream := ds.GetOrCreateStream(key)

		var provided *store.StreamID
		if idStr == "*" {
			provided = nil
		} else {
			id, err := parseExactStreamID(idStr)
			if err != nil {
				return resp.Value{}, err
			}
			provided = &id
		}

		id, err := stream.XAdd(provided, fields)
		if err != nil {
			return resp.Value{}, err
		}

		return resp.Value{Type: resp.BulkString, Str: id.String()}, nil
	}
}

// XLEN key
func XLenHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XLEN' command")
		}
		stream := ds.GetOrCreateStream(args[0].Str)
		return resp.Value{Type: resp.Integer, Int: int64(stream.XLen())}, nil
	}
}

// XRANGE key start end [COUNT n]
func XRangeHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XRANGE' command")
		}
		key := args[0].Str
		startStr := strings.ToLower(args[1].Str)
		endStr := strings.ToLower(args[2].Str)
		count := 0
		if len(args) == 5 {
			if !strings.EqualFold(args[3].Str, "COUNT") {
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}
			n, err := strconv.Atoi(args[4].Str)
			if err != nil || n < 0 {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}
			count = n
		} else if len(args) != 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XRANGE' command")
		}

		stream := ds.GetOrCreateStream(key)
		startID, err := parseRangeID(startStr, true)
		if err != nil {
			return resp.Value{}, err
		}
		endID, err := parseRangeID(endStr, false)
		if err != nil {
			return resp.Value{}, err
		}
		entries := stream.XRange(startID, endID, count)
		return entriesToResp(entries), nil
	}
}

// XDEL key id [id ...]
func XDelHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XDEL' command")
		}
		key := args[0].Str
		ids := make([]store.StreamID, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			id, err := parseExactStreamID(strings.ToLower(args[i].Str))
			if err != nil {
				return resp.Value{}, err
			}
			ids = append(ids, id)
		}
		stream := ds.GetOrCreateStream(key)
		removed := stream.XDel(ids)
		return resp.Value{Type: resp.Integer, Int: int64(removed)}, nil
	}
}

// XTRIM key MAXLEN [~] count
func XTrimHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 3 || len(args) > 4 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XTRIM' command")
		}
		key := args[0].Str
		if !strings.EqualFold(args[1].Str, "MAXLEN") {
			return resp.Value{}, fmt.Errorf("ERR syntax error")
		}
		approx := false
		var nStr string
		if len(args) == 4 {
			if args[2].Str == "~" {
				approx = true
				nStr = args[3].Str
			} else {
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}
		} else {
			nStr = args[2].Str
		}
		n, err := strconv.Atoi(nStr)
		if err != nil || n < 0 {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		stream := ds.GetOrCreateStream(key)
		// For now, ignore approx and always do exact trim
		removed := stream.XTrimMaxLen(n)
		_ = approx
		return resp.Value{Type: resp.Integer, Int: int64(removed)}, nil
	}
}

// XREAD [COUNT n] STREAMS key id  (minimal, single stream)
func XReadHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XREAD' command")
		}
		count := 0
		idx := 0
		if strings.EqualFold(args[idx].Str, "COUNT") {
			if len(args) < 4 {
				return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XREAD' command")
			}
			c, err := strconv.Atoi(args[idx+1].Str)
			if err != nil || c < 0 {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}
			count = c
			idx += 2
		}
		if idx >= len(args) || !strings.EqualFold(args[idx].Str, "STREAMS") {
			return resp.Value{}, fmt.Errorf("ERR syntax error")
		}
		if len(args) < idx+3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'XREAD' command")
		}
		key := args[idx+1].Str
		idStr := strings.ToLower(args[idx+2].Str)
		// Expect explicit ms-seq id; do not implement BLOCK/"$" right now
		afterID, err := parseExactStreamID(idStr)
		if err != nil {
			return resp.Value{}, err
		}
		stream := ds.GetOrCreateStream(key)
		entries := stream.XReadAfter(afterID, count)
		if len(entries) == 0 {
			return resp.Value{Type: resp.Array, IsNull: true}, nil
		}
		// Build response: [[ key, [ entries... ] ]]
		entriesResp := entriesToResp(entries)
		streamKey := resp.Value{Type: resp.BulkString, Str: key}
		inner := resp.Value{Type: resp.Array, Array: []resp.Value{streamKey, entriesResp}}
		return resp.Value{Type: resp.Array, Array: []resp.Value{inner}}, nil
	}
}

// Helpers
func parseExactStreamID(s string) (store.StreamID, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return store.StreamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return store.StreamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return store.StreamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	return store.StreamID{Ms: ms, Seq: seq}, nil
}

func parseRangeID(s string, isStart bool) (store.StreamID, error) {
	s = strings.ToLower(s)
	switch s {
	case "-":
		return store.StreamID{Ms: 0, Seq: 0}, nil
	case "+":
		return store.StreamID{Ms: math.MaxUint64, Seq: math.MaxUint64}, nil
	default:
		return parseExactStreamID(s)
	}
}

func entriesToResp(entries []store.StreamEntry) resp.Value {
	arr := make([]resp.Value, len(entries))
	for i, e := range entries {
		// fields array
		fv := make([]resp.Value, 0, len(e.Fields)*2)
		for f, v := range e.Fields {
			fv = append(fv, resp.Value{Type: resp.BulkString, Str: f})
			fv = append(fv, resp.Value{Type: resp.BulkString, Str: v})
		}
		item := resp.Value{Type: resp.Array, Array: []resp.Value{
			{Type: resp.BulkString, Str: e.ID.String()},
			{Type: resp.Array, Array: fv},
		}}
		arr[i] = item
	}
	return resp.Value{Type: resp.Array, Array: arr}
}
