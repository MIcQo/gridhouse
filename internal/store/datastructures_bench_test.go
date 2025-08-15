package store

import (
	"strconv"
	"testing"
)

// List benchmarks
func BenchmarkListLPush(b *testing.B) {
	list := NewList()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LPush("item" + strconv.Itoa(i))
	}
}

func BenchmarkListRPush(b *testing.B) {
	list := NewList()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}
}

func BenchmarkListLPop(b *testing.B) {
	list := NewList()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		list.LPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LPop()
	}
}

func BenchmarkListRPop(b *testing.B) {
	list := NewList()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.RPop()
	}
}

func BenchmarkListLRange(b *testing.B) {
	list := NewList()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LRange(0, 99) // Get first 100 items
	}
}

func BenchmarkListLIndex(b *testing.B) {
	list := NewList()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		list.RPush("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.LIndex(i % 1000)
	}
}

func BenchmarkListLRem(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := NewList()
		// Pre-populate with items
		for j := 0; j < 100; j++ {
			list.RPush("item")
			list.RPush("other")
		}
		b.StartTimer()

		list.LRem(5, "item")
	}
}

// Set benchmarks
func BenchmarkSetSAdd(b *testing.B) {
	set := NewSet()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}
}

func BenchmarkSetSRem(b *testing.B) {
	set := NewSet()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SRem("item" + strconv.Itoa(i))
	}
}

func BenchmarkSetSIsMember(b *testing.B) {
	set := NewSet()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SIsMember("item" + strconv.Itoa(i%1000))
	}
}

func BenchmarkSetSMembers(b *testing.B) {
	set := NewSet()
	// Pre-populate with 1000 items
	for i := 0; i < 1000; i++ {
		set.SAdd("item" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.SMembers()
	}
}

func BenchmarkSetSPop(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		set := NewSet()
		// Pre-populate
		for j := 0; j < 100; j++ {
			set.SAdd("item" + strconv.Itoa(j))
		}
		b.StartTimer()

		set.SPop()
	}
}

// Hash benchmarks
func BenchmarkHashHSet(b *testing.B) {
	hash := NewHash()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}
}

func BenchmarkHashHGet(b *testing.B) {
	hash := NewHash()
	// Pre-populate with 1000 fields
	for i := 0; i < 1000; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HGet("field" + strconv.Itoa(i%1000))
	}
}

func BenchmarkHashHDel(b *testing.B) {
	hash := NewHash()
	// Pre-populate
	for i := 0; i < b.N; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HDel("field" + strconv.Itoa(i))
	}
}

func BenchmarkHashHGetAll(b *testing.B) {
	hash := NewHash()
	// Pre-populate with 1000 fields
	for i := 0; i < 1000; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HGetAll()
	}
}

func BenchmarkHashHKeys(b *testing.B) {
	hash := NewHash()
	// Pre-populate with 1000 fields
	for i := 0; i < 1000; i++ {
		hash.HSet("field"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HKeys()
	}
}

func BenchmarkHashHIncrBy(b *testing.B) {
	hash := NewHash()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash.HIncrBy("counter", 1)
	}
}

// Concurrent benchmarks
func BenchmarkListConcurrent(b *testing.B) {
	list := NewList()

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

func BenchmarkSetConcurrent(b *testing.B) {
	set := NewSet()

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

func BenchmarkHashConcurrent(b *testing.B) {
	hash := NewHash()

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
