package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"gridhouse/internal/resp"
	st "gridhouse/internal/store"
)

// ScanHandler implements: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
// Minimal stateless implementation over a snapshot of keys from store.Keys().
// Cursor is an integer offset in the filtered key list. Returns [next-cursor, [keys...]]
func ScanHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SCAN' command")
		}

		// Parse cursor
		cursorStr := args[0].Str
		cursor, err := strconv.Atoi(cursorStr)
		if err != nil || cursor < 0 {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}

		// Options
		var pattern string
		var typeFilter string
		count := 10 // default like Redis
		i := 1
		for i < len(args) {
			opt := strings.ToUpper(args[i].Str)
			switch opt {
			case "MATCH":
				if i+1 >= len(args) {
					return resp.Value{}, fmt.Errorf("ERR syntax error")
				}
				pattern = args[i+1].Str
				i += 2
			case "COUNT":
				if i+1 >= len(args) {
					return resp.Value{}, fmt.Errorf("ERR syntax error")
				}
				c, err := strconv.Atoi(args[i+1].Str)
				if err != nil || c < 0 {
					return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
				}
				if c == 0 {
					c = 10 // treat 0 as default to avoid infinite loop behavior
				}
				count = c
				i += 2
			case "TYPE":
				if i+1 >= len(args) {
					return resp.Value{}, fmt.Errorf("ERR syntax error")
				}
				typeFilter = strings.ToLower(args[i+1].Str)
				i += 2
			default:
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}
		}

		// Obtain keys and filter by pattern if any
		keys := ds.Keys()
		if pattern != "" && pattern != "*" {
			filtered := make([]string, 0, len(keys)/2)
			for _, k := range keys {
				if matchPatternOptimized(k, pattern) {
					filtered = append(filtered, k)
				}
			}
			keys = filtered
		}

		// Filter by type if requested
		if typeFilter != "" {
			var targetType st.DataType
			switch typeFilter {
			case "string":
				targetType = st.TypeString
			case "list":
				targetType = st.TypeList
			case "set":
				targetType = st.TypeSet
			case "hash":
				targetType = st.TypeHash
			case "zset":
				targetType = st.TypeSortedSet
			case "stream":
				targetType = st.TypeStream
			default:
				// Unknown type: return empty result set gracefully
				keys = nil
			}
			if keys != nil {
				filtered := make([]string, 0, len(keys))
				for _, k := range keys {
					if ds.GetDataType(k) == targetType {
						filtered = append(filtered, k)
					}
				}
				keys = filtered
			}
		}

		// Paginate by cursor and count
		if cursor > len(keys) {
			cursor = len(keys)
		}
		end := cursor + count
		nextCursor := 0
		if end < len(keys) {
			nextCursor = end
		} else {
			end = len(keys)
			nextCursor = 0
		}

		// Build RESP response: [ next-cursor, [keys... ] ]
		cursorVal := resp.Value{Type: resp.BulkString, Str: strconv.Itoa(nextCursor)}
		arr := make([]resp.Value, end-cursor)
		for i := cursor; i < end; i++ {
			arr[i-cursor] = resp.Value{Type: resp.BulkString, Str: keys[i]}
		}
		keysVal := resp.Value{Type: resp.Array, Array: arr}
		return resp.Value{Type: resp.Array, Array: []resp.Value{cursorVal, keysVal}}, nil
	}
}

// SScanHandler implements: SSCAN key cursor [MATCH pattern] [COUNT count]
// Cursor is an integer offset in the filtered member list. Returns [next-cursor, [members...]]
func SScanHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SSCAN' command")
		}
		key := args[0].Str
		cursor, err := strconv.Atoi(args[1].Str)
		if err != nil || cursor < 0 {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		// Options
		var pattern string
		count := 10
		i := 2
		for i < len(args) {
			opt := strings.ToUpper(args[i].Str)
			switch opt {
			case "MATCH":
				if i+1 >= len(args) {
					return resp.Value{}, fmt.Errorf("ERR syntax error")
				}
				pattern = args[i+1].Str
				i += 2
			case "COUNT":
				if i+1 >= len(args) {
					return resp.Value{}, fmt.Errorf("ERR syntax error")
				}
				c, err := strconv.Atoi(args[i+1].Str)
				if err != nil || c < 0 {
					return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
				}
				if c == 0 {
					c = 10
				}
				count = c
				i += 2
			default:
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}
		}

		// Type check: if key exists and not a set, error
		if ds.Exists(key) && ds.GetDataType(key) != st.TypeSet {
			return resp.Value{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		// Snapshot members
		members := []string{}
		if ds.Exists(key) {
			members = ds.GetOrCreateSet(key).SMembers()
		}

		// Filter by pattern
		if pattern != "" && pattern != "*" {
			filtered := make([]string, 0, len(members)/2)
			for _, m := range members {
				if matchPatternOptimized(m, pattern) {
					filtered = append(filtered, m)
				}
			}
			members = filtered
		}

		// Paginate
		if cursor > len(members) {
			cursor = len(members)
		}
		end := cursor + count
		nextCursor := 0
		if end < len(members) {
			nextCursor = end
		} else {
			end = len(members)
			nextCursor = 0
		}

		cursorVal := resp.Value{Type: resp.BulkString, Str: strconv.Itoa(nextCursor)}
		arr := make([]resp.Value, end-cursor)
		for i := cursor; i < end; i++ {
			arr[i-cursor] = resp.Value{Type: resp.BulkString, Str: members[i]}
		}
		vals := resp.Value{Type: resp.Array, Array: arr}
		return resp.Value{Type: resp.Array, Array: []resp.Value{cursorVal, vals}}, nil
	}
}

// HScanHandler implements: HSCAN key cursor [MATCH pattern] [COUNT count]
// Cursor is an integer offset into the filtered list of fields. Returns [next-cursor, [field, value, ...]]
func HScanHandler(ds DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'HSCAN' command")
		}
		key := args[0].Str
		cursor, err := strconv.Atoi(args[1].Str)
		if err != nil || cursor < 0 {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		// Options
		var pattern string
		count := 10
		i := 2
		for i < len(args) {
			opt := strings.ToUpper(args[i].Str)
			switch opt {
			case "MATCH":
				if i+1 >= len(args) {
					return resp.Value{}, fmt.Errorf("ERR syntax error")
				}
				pattern = args[i+1].Str
				i += 2
			case "COUNT":
				if i+1 >= len(args) {
					return resp.Value{}, fmt.Errorf("ERR syntax error")
				}
				c, err := strconv.Atoi(args[i+1].Str)
				if err != nil || c < 0 {
					return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
				}
				if c == 0 {
					c = 10
				}
				count = c
				i += 2
			default:
				return resp.Value{}, fmt.Errorf("ERR syntax error")
			}
		}

		// Type check: if the key exists but is not a hash, return WRONGTYPE
		if ds.Exists(key) && ds.GetDataType(key) != st.TypeHash {
			return resp.Value{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		// Snapshot fields and values
		var fieldsMap map[string]string
		if ds.Exists(key) {
			fieldsMap = ds.GetOrCreateHash(key).HGetAll()
		} else {
			fieldsMap = map[string]string{}
		}

		// Build a slice of field names (for ordering and filtering)
		fields := make([]string, 0, len(fieldsMap))
		for f := range fieldsMap {
			fields = append(fields, f)
		}

		// Filter by MATCH pattern (applied to field names)
		if pattern != "" && pattern != "*" {
			filtered := make([]string, 0, len(fields)/2)
			for _, f := range fields {
				if matchPatternOptimized(f, pattern) {
					filtered = append(filtered, f)
				}
			}
			fields = filtered
		}

		// Paginate fields by cursor/count
		if cursor > len(fields) {
			cursor = len(fields)
		}
		end := cursor + count
		nextCursor := 0
		if end < len(fields) {
			nextCursor = end
		} else {
			end = len(fields)
			nextCursor = 0
		}

		// Build alternating field/value array for the selected page
		pairsCount := end - cursor
		arr := make([]resp.Value, pairsCount*2)
		idx := 0
		for i := cursor; i < end; i++ {
			f := fields[i]
			arr[idx] = resp.Value{Type: resp.BulkString, Str: f}
			arr[idx+1] = resp.Value{Type: resp.BulkString, Str: fieldsMap[f]}
			idx += 2
		}

		cursorVal := resp.Value{Type: resp.BulkString, Str: strconv.Itoa(nextCursor)}
		vals := resp.Value{Type: resp.Array, Array: arr}
		return resp.Value{Type: resp.Array, Array: []resp.Value{cursorVal, vals}}, nil
	}
}
