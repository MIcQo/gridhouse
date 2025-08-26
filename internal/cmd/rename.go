package cmd

import (
	"fmt"
	"gridhouse/internal/resp"
	"gridhouse/internal/store"
	"time"
)

// RenameHandler handles the RENAME command: RENAME key newkey
func RenameHandler(dataStore DataStore) Handler {
	return func(args []resp.Value) (resp.Value, error) {
		if len(args) != 2 {
			return resp.Value{}, fmt.Errorf("ERR wrong number of arguments for 'RENAME' command")
		}

		oldKey := args[0].Str
		newKey := args[1].Str

		// Check if source key exists
		if !dataStore.Exists(oldKey) {
			return resp.Value{}, fmt.Errorf("ERR no such key")
		}

		// Get the data type of the source key
		dataType := dataStore.GetDataType(oldKey)

		// If destination key exists and is different from source, delete it first (implicit DEL)
		if dataStore.Exists(newKey) && oldKey != newKey {
			dataStore.Del(newKey)
		}

		// Copy data based on type
		switch dataType {
		case store.TypeString:
			// Handle string type
			if value, exists := dataStore.Get(oldKey); exists {
				// Get TTL for the old key
				ttl := dataStore.TTL(oldKey)
				var expiration time.Time
				if ttl > 0 {
					expiration = time.Now().Add(time.Duration(ttl) * time.Second)
				}
				dataStore.Set(newKey, value, expiration)
			}

		case store.TypeList:
			// Handle list type
			oldList := dataStore.GetOrCreateList(oldKey)
			newList := dataStore.GetOrCreateList(newKey)

			// Copy all elements
			items := oldList.LRange(0, -1)
			for _, item := range items {
				newList.RPush(item)
			}

		case store.TypeSet:
			// Handle set type
			oldSet := dataStore.GetOrCreateSet(oldKey)
			newSet := dataStore.GetOrCreateSet(newKey)

			// Copy all members
			members := oldSet.SMembers()
			for _, member := range members {
				newSet.SAdd(member)
			}

		case store.TypeHash:
			// Handle hash type
			oldHash := dataStore.GetOrCreateHash(oldKey)
			newHash := dataStore.GetOrCreateHash(newKey)

			// Copy all fields
			fields := oldHash.HGetAll()
			for field, value := range fields {
				newHash.HSet(field, value)
			}

		case store.TypeSortedSet:
			// Handle sorted set type
			oldZSet := dataStore.GetOrCreateSortedSet(oldKey)
			newZSet := dataStore.GetOrCreateSortedSet(newKey)

			// Copy all members with their scores
			members := oldZSet.ZRange(0, -1, false)
			for _, member := range members {
				if score, exists := oldZSet.ZScore(member); exists {
					newZSet.ZAdd(map[string]float64{member: score})
				}
			}

		case store.TypeStream:
			// Handle stream type
			oldStream := dataStore.GetOrCreateStream(oldKey)
			newStream := dataStore.GetOrCreateStream(newKey)

			// Copy all entries
			entries := oldStream.XRange(store.StreamID{Ms: 0, Seq: 0}, store.StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}, 0)
			for _, entry := range entries {
				newStream.XAdd(&entry.ID, entry.Fields)
			}

		default:
			// Fallback to string type for unknown types
			if value, exists := dataStore.Get(oldKey); exists {
				dataStore.Set(newKey, value, time.Time{})
			}
		}

		// Delete the old key only if it's different from the new key
		if oldKey != newKey {
			dataStore.Del(oldKey)
		}

		return resp.Value{Type: resp.SimpleString, Str: "OK"}, nil
	}
}
