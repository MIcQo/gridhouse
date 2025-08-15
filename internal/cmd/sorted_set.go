package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"strconv"
)

// ===== Sorted Set Commands =====

// ZPopMinHandler handles the ZPOPMIN command: ZPOPMIN key [count]
func ZPopMinHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 1 || len(args) > 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ZPOPMIN' command")
		}
		key := args[0].Str
		count := 1
		if len(args) == 2 {
			c, err := strconv.Atoi(args[1].Str)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
			}
			if c < 1 {
				// As a safe behavior, return empty array for non-positive count
				return resp.Value{Type: resp.Array, Array: []resp.Value{}}, nil
			}
			count = c
		}
		z := store.GetOrCreateSortedSet(key)
		popped := z.ZPopMin(count)
		// Build array of member, score pairs
		arr := make([]resp.Value, 0, len(popped)*2)
		for _, e := range popped {
			arr = append(arr, resp.Value{Type: resp.BulkString, Str: e.Member})
			arr = append(arr, resp.Value{Type: resp.BulkString, Str: strconv.FormatFloat(e.Score, 'f', -1, 64)})
		}
		return resp.Value{Type: resp.Array, Array: arr}, nil
	}
}

// ZAddHandler handles the ZADD command: ZADD key score member [score member ...]
func ZAddHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 3 || len(args)%2 == 0 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ZADD' command")
		}
		key := args[0].Str
		pairs := make(map[string]float64)
		for i := 1; i < len(args); i += 2 {
			score, err := strconv.ParseFloat(args[i].Str, 64)
			if err != nil {
				return resp.Value{}, fmt.Errorf("ERR value is not a valid float")
			}
			member := args[i+1].Str
			pairs[member] = score
		}
		z := store.GetOrCreateSortedSet(key)
		added := z.ZAdd(pairs)
		return resp.Value{Type: resp.Integer, Int: int64(added)}, nil
	}
}

// ZRemHandler handles the ZREM command
func ZRemHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ZREM' command")
		}
		key := args[0].Str
		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			members[i-1] = args[i].Str
		}
		z := store.GetOrCreateSortedSet(key)
		removed := z.ZRem(members...)
		return resp.Value{Type: resp.Integer, Int: int64(removed)}, nil
	}
}

// ZCardHandler handles the ZCARD command
func ZCardHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ZCARD' command")
		}
		key := args[0].Str
		z := store.GetOrCreateSortedSet(key)
		return resp.Value{Type: resp.Integer, Int: int64(z.ZCard())}, nil
	}
}

// ZScoreHandler handles the ZSCORE command
func ZScoreHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ZSCORE' command")
		}
		key := args[0].Str
		member := args[1].Str
		z := store.GetOrCreateSortedSet(key)
		if score, ok := z.ZScore(member); ok {
			return resp.Value{Type: resp.BulkString, Str: strconv.FormatFloat(score, 'f', -1, 64)}, nil
		}
		return resp.Value{Type: resp.BulkString, IsNull: true}, nil
	}
}

// ZRangeHandler handles the ZRANGE command: ZRANGE key start stop [WITHSCORES]
func ZRangeHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'ZRANGE' command")
		}
		key := args[0].Str
		start, err1 := strconv.Atoi(args[1].Str)
		stop, err2 := strconv.Atoi(args[2].Str)
		if err1 != nil || err2 != nil {
			return resp.Value{}, fmt.Errorf("ERR value is not an integer or out of range")
		}
		withScores := false
		if len(args) >= 4 {
			opt := args[3].Str
			if opt == "WITHSCORES" || opt == "withscores" || opt == "WithScores" {
				withScores = true
			}
		}
		z := store.GetOrCreateSortedSet(key)
		res := z.ZRange(start, stop, withScores)
		arr := make([]resp.Value, len(res))
		for i, s := range res {
			arr[i] = resp.Value{Type: resp.BulkString, Str: s}
		}
		return resp.Value{Type: resp.Array, Array: arr}, nil
	}
}
