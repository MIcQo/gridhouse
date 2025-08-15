package v2

import (
	"fmt"
	"gridhouse/internal/store"
	"os"
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

func (w *Writer) WriteStream(_ string, _ []store.StreamEntry, _ time.Time) error {
	return nil
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
