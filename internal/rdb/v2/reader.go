package v2

import (
	"fmt"
	"gridhouse/internal/store"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/rdb/parser"
)

type Reader struct {
	file *os.File
}

func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open RDB file: %w", err)
	}

	return &Reader{
		file: file,
	}, nil
}

func (r *Reader) ReadAll(db store.DataStore) error {
	decoder := parser.NewDecoder(r.file)
	return decoder.Parse(func(o parser.RedisObject) bool {
		switch o.GetType() {
		case parser.StringType:
			str := o.(*parser.StringObject)
			var t time.Time
			if str.Expiration != nil {
				t = *str.Expiration
			}
			db.Set(str.Key, string(str.Value), t)
			println("string", str.Key, str.Value)
		case parser.ListType:
			list := o.(*parser.ListObject)
			var l = db.GetOrCreateList(list.Key)
			for _, v := range list.Values {
				l.LPush(string(v))
			}
			println("list", list.Key, list.Values)
		case parser.HashType:
			hash := o.(*parser.HashObject)
			var h = db.GetOrCreateHash(hash.Key)
			for k, v := range hash.Hash {
				h.HSet(k, string(v))
			}

			println("hash", hash.Key, hash.Hash)
		case parser.SetType:
			set := o.(*parser.SetObject)
			var h = db.GetOrCreateSet(set.Key)
			for _, v := range set.Members {
				h.SAdd(string(v))
			}
			println("set", set.Key, set.Members)
		case parser.ZSetType:
			zset := o.(*parser.ZSetObject)
			var z = db.GetOrCreateSortedSet(zset.Key)
			for _, v := range zset.Entries {
				z.ZAdd(map[string]float64{v.Member: v.Score})
			}

			println("zset", zset.Key, zset.Entries)
		case parser.StreamType:
			// Convert parser.StreamObject into our store.Stream
			so := o.(*parser.StreamObject)
			st := db.GetOrCreateStream(getStreamKey(so))
			// Extract and append entries in order
			forEachStreamEntry(so, func(id store.StreamID, fields map[string]string) {
				// Ignore error here; if IDs are not strictly increasing, XAdd will error.
				// Since RDB guarantees order, this should succeed.
				_, _ = st.XAdd(&id, fields)
			})
		}
		// return true to continue, return false to stop the iteration
		return true
	})
}

func (r *Reader) Close() error {
	return r.file.Close()
}

// getStreamKey attempts to read the Key field from parser.StreamObject via reflection
func getStreamKey(so *parser.StreamObject) string {
	v := reflect.ValueOf(so)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.IsValid() && v.Kind() == reflect.Struct {
		f := v.FieldByName("Key")
		if f.IsValid() && f.Kind() == reflect.String {
			return f.String()
		}
	}
	return ""
}

// forEachStreamEntry walks over Entries of parser.StreamObject and calls cb with converted ID and fields
func forEachStreamEntry(so *parser.StreamObject, cb func(id store.StreamID, fields map[string]string)) {
	v := reflect.ValueOf(so)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return
	}
	entries := v.FieldByName("Entries")
	if !entries.IsValid() || entries.Kind() != reflect.Slice {
		return
	}
	for i := 0; i < entries.Len(); i++ {
		ev := entries.Index(i)
		// Handle pointer to struct entry as well
		if ev.Kind() == reflect.Ptr {
			ev = ev.Elem()
		}
		if !ev.IsValid() || ev.Kind() != reflect.Struct {
			continue
		}
		id := extractStreamID(ev.FieldByName("ID"))
		fields := extractFieldsMap(ev.FieldByName("Fields"))
		cb(id, fields)
	}
}

func extractStreamID(idV reflect.Value) store.StreamID {
	if idV.IsValid() && idV.Kind() == reflect.Struct {
		ms := extractUintField(idV, []string{"Ms", "MsTime", "Time"})
		seq := extractUintField(idV, []string{"Seq", "Sequence"})
		return store.StreamID{Ms: ms, Seq: seq}
	}
	// Try string form e.g., "1234-0"
	if idV.IsValid() && idV.Kind() == reflect.String {
		if id, ok := parseIDString(idV.String()); ok {
			return id
		}
	}
	return store.StreamID{Ms: 0, Seq: 0}
}

func extractUintField(v reflect.Value, names []string) uint64 {
	for _, name := range names {
		f := v.FieldByName(name)
		if f.IsValid() {
			switch f.Kind() {
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				return f.Convert(reflect.TypeOf(uint64(0))).Uint()
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return uint64(f.Int())
			}
		}
	}
	return 0
}

func extractFieldsMap(mv reflect.Value) map[string]string {
	res := make(map[string]string)
	if !mv.IsValid() || mv.Kind() != reflect.Map {
		return res
	}
	iter := mv.MapRange()
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		var ks string
		switch k.Kind() {
		case reflect.String:
			ks = k.String()
		default:
			ks = fmt.Sprint(k.Interface())
		}
		var vs string
		switch v.Kind() {
		case reflect.String:
			vs = v.String()
		case reflect.Slice:
			// assume []byte
			if v.Type().Elem().Kind() == reflect.Uint8 {
				bs := v.Bytes()
				vs = string(bs)
			} else {
				vs = fmt.Sprint(v.Interface())
			}
		default:
			vs = fmt.Sprint(v.Interface())
		}
		res[ks] = vs
	}
	return res
}

func parseIDString(s string) (store.StreamID, bool) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return store.StreamID{}, false
	}
	ms, err1 := strconv.ParseUint(parts[0], 10, 64)
	seq, err2 := strconv.ParseUint(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return store.StreamID{}, false
	}
	return store.StreamID{Ms: ms, Seq: seq}, true
}
