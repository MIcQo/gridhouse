package store

import (
	"hash/maphash"
	"runtime"
	"sync"
	"time"
)

const (
	shardCount = 256 // Increased from 256 to 4096 for massive pipelines
	shardMask  = shardCount - 1

	cleanupTick     = 30 * time.Second
	cleanupCheckMax = 8192
)

// UltraOptimizedItem represents a stored value with minimal overhead
type UltraOptimizedItem struct {
	Value      string
	Expiration int64 // Unix nano for faster comparison
	DataType   DataType
	List       *List
	Set        *Set
	Hash       *Hash
	ZSet       *SortedSet
	Stream     *Stream
}

// UltraOptimizedDB is the fastest possible database implementation
type shard struct {
	_  [64]byte
	mu sync.RWMutex
	m  map[string]UltraOptimizedItem
	_  [64]byte
}

type UltraOptimizedDB struct {
	shards [shardCount]*shard
	seed   maphash.Seed
	stop   chan struct{}
}

func NewUltraOptimizedDB() *UltraOptimizedDB {
	db := &UltraOptimizedDB{
		seed: maphash.MakeSeed(),
		stop: make(chan struct{}),
	}
	for i := 0; i < shardCount; i++ {
		db.shards[i] = &shard{m: make(map[string]UltraOptimizedItem, 1024)}
	}
	go db.cleanupExpired()
	return db
}

func (db *UltraOptimizedDB) shardIndex(key string) uint64 {
	var h maphash.Hash
	h.SetSeed(db.seed)
	_, _ = h.WriteString(key)
	return h.Sum64() & shardMask
}

func (db *UltraOptimizedDB) shardFor(key string) *shard { return db.shards[db.shardIndex(key)] }

// Basic operations

func (db *UltraOptimizedDB) Set(key, value string, expiration time.Time) {
	s := db.shardFor(key)
	var exp int64
	if !expiration.IsZero() {
		exp = expiration.UnixNano()
	}
	it := UltraOptimizedItem{Value: value, Expiration: exp, DataType: TypeString}

	// Minimize lock scope - prepare item before lock
	s.mu.Lock()
	s.m[key] = it
	s.mu.Unlock()
}

func (db *UltraOptimizedDB) Get(key string) (string, bool) {
	s := db.shardFor(key)

	// Fast path for non-expired keys - single read lock
	s.mu.RLock()
	it, ok := s.m[key]
	if !ok || it.DataType != TypeString {
		s.mu.RUnlock()
		return "", false
	}

	// Check expiration while still holding read lock
	if it.Expiration != 0 {
		now := time.Now().UnixNano()
		if now > it.Expiration {
			s.mu.RUnlock()
			// Need write lock to delete - upgrade to write lock
			s.mu.Lock()
			// Double-check in case another goroutine already deleted
			if it2, exists := s.m[key]; exists && it2.Expiration != 0 && now > it2.Expiration {
				delete(s.m, key)
			}
			s.mu.Unlock()
			return "", false
		}
	}

	value := it.Value
	s.mu.RUnlock()
	return value, true
}

func (db *UltraOptimizedDB) Del(key string) bool {
	s := db.shardFor(key)
	s.mu.Lock()
	_, ok := s.m[key]
	if ok {
		delete(s.m, key)
	}
	s.mu.Unlock()
	return ok
}

func (db *UltraOptimizedDB) Exists(key string) bool {
	s := db.shardFor(key)
	s.mu.RLock()
	it, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	if it.Expiration != 0 && time.Now().UnixNano() > it.Expiration {
		s.mu.Lock()
		delete(s.m, key)
		s.mu.Unlock()
		return false
	}
	return true
}

func (db *UltraOptimizedDB) TTL(key string) int64 {
	s := db.shardFor(key)
	s.mu.RLock()
	it, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return -2
	}
	if it.Expiration == 0 {
		return -1
	}
	now := time.Now().UnixNano()
	if now > it.Expiration {
		s.mu.Lock()
		delete(s.m, key)
		s.mu.Unlock()
		return -2
	}
	return (it.Expiration - now) / int64(time.Second)
}

func (db *UltraOptimizedDB) PTTL(key string) int64 {
	s := db.shardFor(key)
	s.mu.RLock()
	it, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return -2
	}
	if it.Expiration == 0 {
		return -1
	}
	now := time.Now().UnixNano()
	if now > it.Expiration {
		s.mu.Lock()
		delete(s.m, key)
		s.mu.Unlock()
		return -2
	}
	return (it.Expiration - now) / int64(time.Millisecond)
}

func (db *UltraOptimizedDB) Expire(key string, d time.Duration) bool {
	s := db.shardFor(key)
	s.mu.Lock()
	it, ok := s.m[key]
	if ok {
		if it.Expiration != 0 && time.Now().UnixNano() > it.Expiration {
			delete(s.m, key)
			s.mu.Unlock()
			return false
		}
		it.Expiration = time.Now().Add(d).UnixNano()
		s.m[key] = it
	}
	s.mu.Unlock()
	return ok
}

func (db *UltraOptimizedDB) Keys() []string {
	res := make([]string, 0, 4096)
	now := time.Now().UnixNano()
	for _, s := range db.shards {
		s.mu.RLock()
		for k, it := range s.m {
			if it.Expiration == 0 || now <= it.Expiration {
				res = append(res, k)
			}
		}
		s.mu.RUnlock()
	}
	return res
}

// Batch operations

type kv struct{ k, v string }

func (db *UltraOptimizedDB) MSet(pairs [][2]string) {
	buckets := make([][]kv, shardCount)
	for _, p := range pairs {
		idx := db.shardIndex(p[0])
		buckets[idx] = append(buckets[idx], kv{p[0], p[1]})
	}
	for i := 0; i < shardCount; i++ {
		if len(buckets[i]) == 0 {
			continue
		}
		s := db.shards[i]
		s.mu.Lock()
		for _, r := range buckets[i] {
			s.m[r.k] = UltraOptimizedItem{Value: r.v, DataType: TypeString}
		}
		s.mu.Unlock()
	}
}

func (db *UltraOptimizedDB) MSetParallel(pairs [][2]string) {
	buckets := make([][]kv, shardCount)
	for _, p := range pairs {
		idx := db.shardIndex(p[0])
		buckets[idx] = append(buckets[idx], kv{p[0], p[1]})
	}
	workers := runtime.GOMAXPROCS(0)
	ch := make(chan int, workers)
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range ch {
				s := db.shards[i]
				s.mu.Lock()
				for _, r := range buckets[i] {
					s.m[r.k] = UltraOptimizedItem{Value: r.v, DataType: TypeString}
				}
				s.mu.Unlock()
			}
		}()
	}
	for i := 0; i < shardCount; i++ {
		if len(buckets[i]) > 0 {
			ch <- i
		}
	}
	close(ch)
	wg.Wait()
}

// Data structure helpers

func (db *UltraOptimizedDB) GetOrCreateList(key string) *List {
	s := db.shardFor(key)
	s.mu.Lock()
	it, ok := s.m[key]
	if !ok || it.DataType != TypeList || it.List == nil {
		lst := NewList()
		s.m[key] = UltraOptimizedItem{DataType: TypeList, List: lst}
		s.mu.Unlock()
		return lst
	}
	s.mu.Unlock()
	return it.List
}

func (db *UltraOptimizedDB) GetOrCreateSet(key string) *Set {
	s := db.shardFor(key)
	s.mu.Lock()
	it, ok := s.m[key]
	if !ok || it.DataType != TypeSet || it.Set == nil {
		st := NewSet()
		s.m[key] = UltraOptimizedItem{DataType: TypeSet, Set: st}
		s.mu.Unlock()
		return st
	}
	s.mu.Unlock()
	return it.Set
}

func (db *UltraOptimizedDB) GetOrCreateHash(key string) *Hash {
	s := db.shardFor(key)
	s.mu.Lock()
	it, ok := s.m[key]
	if !ok || it.DataType != TypeHash || it.Hash == nil {
		h := NewHash()
		s.m[key] = UltraOptimizedItem{DataType: TypeHash, Hash: h}
		s.mu.Unlock()
		return h
	}
	s.mu.Unlock()
	return it.Hash
}

func (db *UltraOptimizedDB) GetOrCreateSortedSet(key string) *SortedSet {
	s := db.shardFor(key)
	s.mu.Lock()
	it, ok := s.m[key]
	if !ok || it.DataType != TypeSortedSet || it.ZSet == nil {
		z := NewSortedSet()
		s.m[key] = UltraOptimizedItem{DataType: TypeSortedSet, ZSet: z}
		s.mu.Unlock()
		return z
	}
	s.mu.Unlock()
	return it.ZSet
}

func (db *UltraOptimizedDB) GetDataType(key string) DataType {
	s := db.shardFor(key)
	s.mu.RLock()
	it, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return TypeString
	}
	return it.DataType
}

func (db *UltraOptimizedDB) GetOrCreateStream(key string) *Stream {
	s := db.shardFor(key)
	s.mu.Lock()
	it, ok := s.m[key]
	if !ok || it.DataType != TypeStream || it.Stream == nil {
		st := NewStream()
		s.m[key] = UltraOptimizedItem{DataType: TypeStream, Stream: st}
		s.mu.Unlock()
		return st
	}
	s.mu.Unlock()
	return it.Stream
}

// Cleanup

func (db *UltraOptimizedDB) cleanupExpired() {
	t := time.NewTicker(cleanupTick)
	defer t.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-t.C:
			now := time.Now().UnixNano()
			for _, s := range db.shards {
				s.mu.Lock()
				checked := 0
				for k, it := range s.m {
					if it.Expiration != 0 && now > it.Expiration {
						delete(s.m, k)
					}
					checked++
					if checked >= cleanupCheckMax {
						break
					}
				}
				s.mu.Unlock()
			}
		}
	}
}

func (db *UltraOptimizedDB) Close() { close(db.stop) }
