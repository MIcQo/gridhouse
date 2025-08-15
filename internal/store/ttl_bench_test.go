package store

import (
	"strconv"
	"testing"
	"time"
)

// BenchmarkTTLWheelSet measures TTL wheel set performance
func BenchmarkTTLWheelSet(b *testing.B) {
	wheel := NewOptimizedTTLWheel(time.Second)
	expiration := time.Now().Add(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wheel.Set("key"+strconv.Itoa(i), expiration)
	}
}

// BenchmarkTTLWheelExpired measures TTL wheel expiration check performance
func BenchmarkTTLWheelExpired(b *testing.B) {
	wheel := NewOptimizedTTLWheel(time.Second)

	// Pre-populate with keys
	numKeys := 1000
	futureTime := time.Now().Add(time.Hour)
	pastTime := time.Now().Add(-time.Hour)

	for i := 0; i < numKeys; i++ {
		if i%2 == 0 {
			wheel.Set("key"+strconv.Itoa(i), futureTime) // Not expired
		} else {
			wheel.Set("key"+strconv.Itoa(i), pastTime) // Expired
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wheel.Expired("key" + strconv.Itoa(i%numKeys))
	}
}

// BenchmarkTTLWheelMixed measures mixed TTL wheel operations
func BenchmarkTTLWheelMixed(b *testing.B) {
	wheel := NewOptimizedTTLWheel(time.Second)
	expiration := time.Now().Add(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			wheel.Set("key"+strconv.Itoa(i), expiration)
		} else {
			wheel.Expired("key" + strconv.Itoa(i/2))
		}
	}
}

// BenchmarkTTLWheelConcurrent measures concurrent TTL wheel operations
func BenchmarkTTLWheelConcurrent(b *testing.B) {
	wheel := NewOptimizedTTLWheel(time.Second)
	expiration := time.Now().Add(time.Hour)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				wheel.Set("key"+strconv.Itoa(i), expiration)
			} else {
				wheel.Expired("key" + strconv.Itoa(i/2))
			}
			i++
		}
	})
}

// BenchmarkTTLWheelMemoryUsage measures memory usage of TTL wheel
func BenchmarkTTLWheelMemoryUsage(b *testing.B) {
	expiration := time.Now().Add(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wheel := NewOptimizedTTLWheel(time.Second)
		for j := 0; j < 1000; j++ {
			wheel.Set("key"+strconv.Itoa(j), expiration)
		}
	}
}

// BenchmarkTimeNowCall measures the performance of time.Now() calls
func BenchmarkTimeNowCall(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = time.Now()
	}
}

// BenchmarkTimeAfterComparison measures time comparison performance
func BenchmarkTimeAfterComparison(b *testing.B) {
	now := time.Now()
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			_ = now.After(future)
		} else {
			_ = now.After(past)
		}
	}
}
