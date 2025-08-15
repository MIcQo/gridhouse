package store

import (
	"strconv"
	"testing"
)

// Optimized List benchmarks
func BenchmarkOptimizedListLPush(b *testing.B) {
	list := NewOptimizedList()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LPush("item" + strconv.Itoa(i))
	}
}

func BenchmarkOptimizedListRPush(b *testing.B) {
	list := NewOptimizedList()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}
}

func BenchmarkOptimizedListLPop(b *testing.B) {
	list := NewOptimizedList()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		list.LPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LPop()
	}
}

func BenchmarkOptimizedListRPop(b *testing.B) {
	list := NewOptimizedList()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.RPop()
	}
}

func BenchmarkOptimizedListLRange(b *testing.B) {
	list := NewOptimizedList()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LRange(0, 99) // Get first 100 items
	}
}

func BenchmarkOptimizedListLIndex(b *testing.B) {
	list := NewOptimizedList()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LIndex(i % 1000)
	}
}

// Optimized Set benchmarks
func BenchmarkOptimizedSetSAdd(b *testing.B) {
	set := NewOptimizedSet()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}
}

func BenchmarkOptimizedSetSRem(b *testing.B) {
	set := NewOptimizedSet()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SRem("item" + strconv.Itoa(i))
	}
}

func BenchmarkOptimizedSetSIsMember(b *testing.B) {
	set := NewOptimizedSet()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SIsMember("item" + strconv.Itoa(i%1000))
	}
}

func BenchmarkOptimizedSetSMembers(b *testing.B) {
	set := NewOptimizedSet()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SMembers()
	}
}

// Optimized Hash benchmarks
func BenchmarkOptimizedHashHSet(b *testing.B) {
	hash := NewOptimizedHash()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}
}

func BenchmarkOptimizedHashHGet(b *testing.B) {
	hash := NewOptimizedHash()
	// Pre-populate with 1000 fields
	for i := 0; i < 1000; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HGet("field" + strconv.Itoa(i%1000))
	}
}

func BenchmarkOptimizedHashHDel(b *testing.B) {
	hash := NewOptimizedHash()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HDel("field" + strconv.Itoa(i))
	}
}

func BenchmarkOptimizedHashHGetAll(b *testing.B) {
	hash := NewOptimizedHash()
	// Pre-populate with 1000 fields
	for i := 0; i < 1000; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HGetAll()
	}
}

// Concurrent benchmarks for optimized data structures
func BenchmarkOptimizedListConcurrent(b *testing.B) {
	list := NewOptimizedList()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				list.LPush("item" + strconv.Itoa(i))
			} else {
				list.LPop()
			}
			i++
		}
	})
}

func BenchmarkOptimizedSetConcurrent(b *testing.B) {
	set := NewOptimizedSet()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 3 {
			case 0:
				set.SAdd("item" + strconv.Itoa(i))
			case 1:
				set.SIsMember("item" + strconv.Itoa(i/2))
			case 2:
				set.SRem("item" + strconv.Itoa(i/3))
			}
			i++
		}
	})
}

func BenchmarkOptimizedHashConcurrent(b *testing.B) {
	hash := NewOptimizedHash()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 3 {
			case 0:
				hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
			case 1:
				hash.HGet("field" + strconv.Itoa(i/2))
			case 2:
				hash.HDel("field" + strconv.Itoa(i/3))
			}
			i++
		}
	})
}

// Comparison benchmarks: Original vs Optimized
func BenchmarkListLPushComparison(b *testing.B) {
	b.Run("Original", func(b *testing.B) {
		list := NewList()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			list.LPush("item" + strconv.Itoa(i))
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		list := NewOptimizedList()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			list.LPush("item" + strconv.Itoa(i))
		}
	})
}

func BenchmarkSetSAddComparison(b *testing.B) {
	b.Run("Original", func(b *testing.B) {
		set := NewSet()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			set.SAdd("item" + strconv.Itoa(i))
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		set := NewOptimizedSet()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			set.SAdd("item" + strconv.Itoa(i))
		}
	})
}

func BenchmarkHashHSetComparison(b *testing.B) {
	b.Run("Original", func(b *testing.B) {
		hash := NewHash()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		hash := NewOptimizedHash()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
		}
	})
}
