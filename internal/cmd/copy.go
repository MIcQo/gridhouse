package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"time"
)

// CopyHandler handles the COPY command: COPY source destination [REPLACE]
// Copies source key to destination key without deleting the source
func CopyHandler(dataStore DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) < 2 || len(args) > 3 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'COPY' command")
		}

		sourceKey := args[0].Str
		destKey := args[1].Str
		replace := false

		// Check for REPLACE option
		if len(args) == 3 {
			if args[2].Str != "REPLACE" {
				return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'COPY' command")
			}
			replace = true
		}

		// Check if source key exists
		if !dataStore.Exists(sourceKey) {
			return resp.Value{}, fmt.Errorf("ERR no such key")
		}

		// Check if destination key already exists (unless REPLACE is specified)
		if !replace && dataStore.Exists(destKey) {
			return resp.Value{Type: resp.Integer, Int: 0}, nil
		}

		// If REPLACE is specified and destination exists, delete it first
		// But don't delete if source and destination are the same
		if replace && dataStore.Exists(destKey) && sourceKey != destKey {
			dataStore.Del(destKey)
		}

		// Get the data type of the source key
		dataType := dataStore.GetDataType(sourceKey)

		// Copy data based on type
		switch dataType {
		case store.TypeString:
			// Handle string type
			if value, exists := dataStore.Get(sourceKey); exists {
				// Get TTL for the source key
				ttl := dataStore.TTL(sourceKey)
				var expiration time.Time
				if ttl > 0 {
					expiration = time.Now().Add(time.Duration(ttl) * time.Second)
				}
				dataStore.Set(destKey, value, expiration)
			}

		case store.TypeList:
			// Handle list type
			sourceList := dataStore.GetOrCreateList(sourceKey)
			destList := dataStore.GetOrCreateList(destKey)

			// Copy all elements
			items := sourceList.LRange(0, -1)
			for _, item := range items {
				destList.RPush(item)
			}

		case store.TypeSet:
			// Handle set type
			sourceSet := dataStore.GetOrCreateSet(sourceKey)
			destSet := dataStore.GetOrCreateSet(destKey)

			// Copy all members
			members := sourceSet.SMembers()
			for _, member := range members {
				destSet.SAdd(member)
			}

		case store.TypeHash:
			// Handle hash type
			sourceHash := dataStore.GetOrCreateHash(sourceKey)
			destHash := dataStore.GetOrCreateHash(destKey)

			// Copy all fields
			fields := sourceHash.HGetAll()
			for field, value := range fields {
				destHash.HSet(field, value)
			}

		case store.TypeSortedSet:
			// Handle sorted set type
			sourceZSet := dataStore.GetOrCreateSortedSet(sourceKey)
			destZSet := dataStore.GetOrCreateSortedSet(destKey)

			// Copy all members with their scores
			members := sourceZSet.ZRange(0, -1, false)
			for _, member := range members {
				if score, exists := sourceZSet.ZScore(member); exists {
					destZSet.ZAdd(map[string]float64{member: score})
				}
			}

		case store.TypeStream:
			// Handle stream type
			sourceStream := dataStore.GetOrCreateStream(sourceKey)
			destStream := dataStore.GetOrCreateStream(destKey)

			// Copy all entries
			entries := sourceStream.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
			for _, entry := range entries {
				destStream.XAdd(&entry.ID, entry.Fields)
			}

		default:
			// Fallback to string type for unknown types
			if value, exists := dataStore.Get(sourceKey); exists {
				dataStore.Set(destKey, value, time.Time{})
			}
		}

		return resp.Value{Type: resp.Integer, Int: 1}, nil
	}
}
