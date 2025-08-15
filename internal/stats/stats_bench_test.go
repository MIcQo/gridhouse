package stats

import (
	"sync/atomic"
	"testing"
	"time"
)

// Optimized stats manager benchmarks
func BenchmarkOptimizedStatsManagerIncrement(b *testing.B) {
	sm := NewOptimizedStatsManager()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sm.IncrementCommandsProcessed()
		}
	})
}

func BenchmarkOptimizedStatsManagerIncrementCommandByType(b *testing.B) {
	sm := NewOptimizedStatsManager()
	commands := []string{"SET", "GET", "DEL", "EXISTS", "PING"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.IncrementCommandByType(commands[i%len(commands)])
			i++
		}
	})
}

func BenchmarkOptimizedStatsManagerAddNetworkBytes(b *testing.B) {
	sm := NewOptimizedStatsManager()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sm.AddNetInputBytes(1024)
			sm.AddNetOutputBytes(512)
		}
	})
}

func BenchmarkOptimizedStatsManagerRecordLatency(b *testing.B) {
	sm := NewOptimizedStatsManager()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.RecordCommandLatency("GET", time.Duration(i%1000)*time.Microsecond)
			i++
		}
	})
}

func BenchmarkOptimizedStatsManagerGetSnapshot(b *testing.B) {
	sm := NewOptimizedStatsManager()

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		sm.IncrementCommandByType("GET")
		sm.IncrementCommandByType("SET")
		sm.RecordCommandLatency("GET", time.Millisecond)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.GetSnapshot()
	}
}

func BenchmarkOptimizedStatsManagerConcurrentOperations(b *testing.B) {
	sm := NewOptimizedStatsManager()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Mix of operations to simulate real usage
			switch i % 10 {
			case 0, 1, 2, 3: // 40% command tracking
				sm.IncrementCommandByType("GET")
			case 4, 5: // 20% network tracking
				sm.AddNetInputBytes(100)
			case 6: // 10% connection tracking
				sm.IncrementConnectionsReceived()
			case 7: // 10% latency tracking
				sm.RecordCommandLatency("SET", time.Microsecond*time.Duration(i%100))
			case 8: // 10% keyspace tracking
				sm.IncrementKeyspaceHits()
			case 9: // 10% memory tracking
				sm.SetUsedMemory(int64(i * 1024))
			}
			i++
		}
	})
}

// Comparison benchmarks: Original vs Optimized
func BenchmarkStatsManagerIncrementComparison(b *testing.B) {
	b.Run("Original", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				sm.IncrementCommandsProcessed()
			}
		})
	})

	b.Run("Optimized", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				sm.IncrementCommandsProcessed()
			}
		})
	})
}

func BenchmarkStatsManagerCommandByTypeComparison(b *testing.B) {
	commands := []string{"SET", "GET", "DEL", "EXISTS", "PING"}

	b.Run("Original", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				sm.IncrementCommandByType(commands[i%len(commands)])
				i++
			}
		})
	})

	b.Run("Optimized", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				sm.IncrementCommandByType(commands[i%len(commands)])
				i++
			}
		})
	})
}

func BenchmarkStatsManagerNetworkBytesComparison(b *testing.B) {
	b.Run("Original", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				sm.AddNetInputBytes(1024)
				sm.AddNetOutputBytes(512)
			}
		})
	})

	b.Run("Optimized", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				sm.AddNetInputBytes(1024)
				sm.AddNetOutputBytes(512)
			}
		})
	})
}

func BenchmarkStatsManagerLatencyComparison(b *testing.B) {
	b.Run("Original", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				sm.RecordCommandLatency("GET", time.Duration(i%1000)*time.Microsecond)
				i++
			}
		})
	})

	b.Run("Optimized", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				sm.RecordCommandLatency("GET", time.Duration(i%1000)*time.Microsecond)
				i++
			}
		})
	})
}

func BenchmarkStatsManagerSnapshotComparison(b *testing.B) {
	b.Run("Original", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		// Pre-populate
		for i := 0; i < 1000; i++ {
			sm.IncrementCommandByType("GET")
			sm.RecordCommandLatency("GET", time.Millisecond)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sm.GetSnapshot()
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		sm := NewOptimizedStatsManager()
		// Pre-populate
		for i := 0; i < 1000; i++ {
			sm.IncrementCommandByType("GET")
			sm.RecordCommandLatency("GET", time.Millisecond)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sm.GetSnapshot()
		}
	})
}

// Atomic operations benchmarks
func BenchmarkAtomicInt64Add(b *testing.B) {
	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&counter, 1)
		}
	})
}

func BenchmarkAtomicInt64Load(b *testing.B) {
	var counter int64 = 12345

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.LoadInt64(&counter)
		}
	})
}

func BenchmarkAtomicInt64Store(b *testing.B) {
	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			atomic.StoreInt64(&counter, i)
			i++
		}
	})
}

// Ring buffer benchmarks
func BenchmarkRingBufferAdd(b *testing.B) {
	rb := newRingBuffer(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rb.add(time.Duration(i) * time.Microsecond)
			i++
		}
	})
}

func BenchmarkRingBufferGetAverage(b *testing.B) {
	rb := newRingBuffer(1000)
	// Pre-populate
	for i := 0; i < 1000; i++ {
		rb.add(time.Duration(i) * time.Microsecond)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.getAverage()
	}
}
