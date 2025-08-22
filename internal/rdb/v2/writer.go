package v2

import (
	"fmt"
	"gridhouse/internal/store"
	"os"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
)

type Writer struct {
	file *os.File
	enc  *core.Encoder
}

func (w *Writer) Close() error {
	return w.file.Close()
}

func (w *Writer) WriteHeader(keyCount, ttlCount uint64) error {
	if keyCount == 0 {
		return nil
	}
	auxMap := map[string]string{
		"redis-ver":    "7.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		var err = w.enc.WriteAux(k, v)
		if err != nil {
			return err
		}
	}

	return w.enc.WriteDBHeader(0, keyCount, ttlCount)
}

func (w *Writer) WriteEOF() error {
	return w.enc.WriteEnd()
}

func (w *Writer) WriteString(key string, value string, expiration time.Time) error {
	return w.enc.WriteStringObject(key, []byte(value), encoder.WithTTL(uint64(expiration.Unix()*1000)))
}

func (w *Writer) WriteList(key string, vals []string, _ time.Time) error {
	return w.enc.WriteListObject(key, convertStringValsToBytes(vals))
}

func (w *Writer) WriteSet(key string, members []string, _ time.Time) error {
	return w.enc.WriteSetObject(key, convertStringValsToBytes(members))
}

func (w *Writer) WriteHash(key string, fields map[string]string, _ time.Time) error {
	return w.enc.WriteHashMapObject(key, convertStringMapToBytes(fields))
}

func (w *Writer) WriteZSet(key string, pairs map[string]float64, _ time.Time) error {
	var zset []*model.ZSetEntry
	for k, p := range pairs {
		zset = append(zset, &model.ZSetEntry{
			Member: k,
			Score:  p,
		})
	}
	return w.enc.WriteZSetObject(key, zset)
}

func (w *Writer) WriteStream(key string, entries []store.StreamEntry, exp time.Time) error {
	// Build model.StreamObject from store entries
	stream := &model.StreamObject{
		Version: 2,
		Entries: make([]*model.StreamEntry, 0, len(entries)),
		Groups:  nil,
	}
	// Helper to track first/last IDs
	var (
		minMs  uint64
		minSeq uint64
		maxMs  uint64
		maxSeq uint64
	)
	if len(entries) > 0 {
		minMs, minSeq = entries[0].ID.Ms, entries[0].ID.Seq
		maxMs, maxSeq = minMs, minSeq
	}
	for _, e := range entries {
		// Determine deterministic field order (sorted by field name)
		fieldNames := make([]string, 0, len(e.Fields))
		for k := range e.Fields {
			fieldNames = append(fieldNames, k)
		}
		// Sort to get stable encoding
		sort.Strings(fieldNames)

		// Build message map according to the same field order
		msgFields := make(map[string]string, len(e.Fields))
		for _, k := range fieldNames {
			msgFields[k] = e.Fields[k]
		}

		id := &model.StreamId{Ms: e.ID.Ms, Sequence: e.ID.Seq}
		me := &model.StreamEntry{
			FirstMsgId: id,
			Fields:     fieldNames,
			Msgs: []*model.StreamMessage{
				{Id: id, Fields: msgFields, Deleted: false},
			},
		}
		stream.Entries = append(stream.Entries, me)

		// Track min/max IDs
		if e.ID.Ms < minMs || (e.ID.Ms == minMs && e.ID.Seq < minSeq) {
			minMs, minSeq = e.ID.Ms, e.ID.Seq
		}
		if e.ID.Ms > maxMs || (e.ID.Ms == maxMs && e.ID.Seq > maxSeq) {
			maxMs, maxSeq = e.ID.Ms, e.ID.Seq
		}
	}
	// Populate metadata
	stream.Length = uint64(len(entries))
	if len(entries) > 0 {
		stream.FirstId = &model.StreamId{Ms: minMs, Sequence: minSeq}
		stream.LastId = &model.StreamId{Ms: maxMs, Sequence: maxSeq}
		stream.AddedEntriesCount = uint64(len(entries))
	} else {
		stream.FirstId = &model.StreamId{Ms: 0, Sequence: 0}
		stream.LastId = &model.StreamId{Ms: 0, Sequence: 0}
		stream.AddedEntriesCount = 0
	}

	// Pass TTL option if expiration is set
	var opts []interface{}
	if !exp.IsZero() {
		opts = append(opts, encoder.WithTTL(uint64(exp.Unix()*1000)))
	}
	return w.enc.WriteStreamObject(key, stream, opts...)
}

func NewWriter(path string) (*Writer, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create RDB file: %w", err)
	}
	enc := encoder.NewEncoder(file)
	err = enc.WriteHeader()
	if err != nil {
		return nil, err
	}

	return &Writer{file: file, enc: enc}, nil
}

func convertStringMapToBytes(val map[string]string) map[string][]byte {
	var byteVals = make(map[string][]byte, len(val))
	for k, v := range val {
		byteVals[k] = []byte(v)
	}
	return byteVals
}

func convertStringValsToBytes(vals []string) [][]byte {
	var byteVals [][]byte
	for _, v := range vals {
		byteVals = append(byteVals, []byte(v))
	}
	return byteVals
}
