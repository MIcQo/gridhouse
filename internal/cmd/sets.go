package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
)

// SAddHandler handles the SADD command
func SAddHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SADD' command")
		}

		key := args[0].Str
		elements := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			elements[i] = arg.Str
		}

		set := store.GetOrCreateSet(key)
		added := set.SAdd(elements...)

		return resp.Value{Type: resp.Integer, Int: int64(added)}, nil
	}
}

// SRemHandler handles the SREM command
func SRemHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SREM' command")
		}

		key := args[0].Str
		elements := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			elements[i] = arg.Str
		}

		set := store.GetOrCreateSet(key)
		removed := set.SRem(elements...)

		return resp.Value{Type: resp.Integer, Int: int64(removed)}, nil
	}
}

// SIsMemberHandler handles the SISMEMBER command
func SIsMemberHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SISMEMBER' command")
		}

		key := args[0].Str
		member := args[1].Str

		set := store.GetOrCreateSet(key)
		isMember := set.SIsMember(member)

		if isMember {
			return resp.Value{Type: resp.Integer, Int: 1}, nil
		}
		return resp.Value{Type: resp.Integer, Int: 0}, nil
	}
}

// SMembersHandler handles the SMEMBERS command
func SMembersHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SMEMBERS' command")
		}

		key := args[0].Str
		set := store.GetOrCreateSet(key)
		members := set.SMembers()

		// Convert to RESP array
		array := make([]resp.Value, len(members))
		for i, member := range members {
			array[i] = resp.Value{Type: resp.BulkString, Str: member}
		}

		return resp.Value{Type: resp.Array, Array: array}, nil
	}
}

// SCardHandler handles the SCARD command
func SCardHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SCARD' command")
		}

		key := args[0].Str
		set := store.GetOrCreateSet(key)
		card := set.SCard()

		return resp.Value{Type: resp.Integer, Int: int64(card)}, nil
	}
}

// SPopHandler handles the SPOP command
func SPopHandler(store DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 1 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'SPOP' command")
		}

		key := args[0].Str
		set := store.GetOrCreateSet(key)

		element, exists := set.SPop()
		if !exists {
			return resp.Value{Type: resp.BulkString, IsNull: true}, nil
		}

		return resp.Value{Type: resp.BulkString, Str: element}, nil
	}
}
