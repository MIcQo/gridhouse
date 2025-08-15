package store

import (
	"strconv"
	"testing"
	"time"
)

// BenchmarkDBSet measures the performance of Set operations
func BenchmarkDBSet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i)
			value := "value" + strconv.Itoa(i)
			db.Set(key, value, time.Time{})
			i++
		}
	})
}

// BenchmarkDBGet measures the performance of Get operations
func BenchmarkDBGet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Pre-populate with data
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		db.Set(key, value, time.Time{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i%numKeys)
			db.Get(key)
			i++
		}
	})
}

// BenchmarkDBSetGet measures mixed Set/Get operations
func BenchmarkDBSetGet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				key := "key" + strconv.Itoa(i)
				value := "value" + strconv.Itoa(i)
				db.Set(key, value, time.Time{})
			} else {
				key := "key" + strconv.Itoa(i/2)
				db.Get(key)
			}
			i++
		}
	})
}

// Compare current DB vs optimized DB implementations
func BenchmarkCurrentDBSet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i)
			value := "value" + strconv.Itoa(i)
			db.Set(key, value, time.Time{})
			i++
		}
	})
}

func BenchmarkCurrentDBGet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Pre-populate with data
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		db.Set(key, value, time.Time{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i%numKeys)
			db.Get(key)
			i++
		}
	})
}

// Benchmark simple map operations as baseline
func BenchmarkMapSet(b *testing.B) {
	m := make(map[string]string)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		m[key] = value
	}
}

func BenchmarkMapGet(b *testing.B) {
	m := make(map[string]string)

	// Pre-populate with data
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		m[key] = value
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i%numKeys)
		_ = m[key]
	}
}

// Ultra-optimized benchmarks
func BenchmarkUltraOptimizedDBSet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i)
			value := "value" + strconv.Itoa(i)
			db.Set(key, value, time.Time{})
			i++
		}
	})
}

func BenchmarkUltraOptimizedDBGet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	// Pre-populate with data
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		db.Set(key, value, time.Time{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i%numKeys)
			db.Get(key)
			i++
		}
	})
}

func BenchmarkUltraOptimizedDBSetGet(b *testing.B) {
	db := NewUltraOptimizedDB()
	defer db.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				key := "key" + strconv.Itoa(i)
				value := "value" + strconv.Itoa(i)
				db.Set(key, value, time.Time{})
			} else {
				key := "key" + strconv.Itoa(i/2)
				db.Get(key)
			}
			i++
		}
	})
}
