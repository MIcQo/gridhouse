package store

import (
	"strconv"
	"testing"
	"time"
)

// TTL Optimization benchmarks
func BenchmarkTTLWheelSetComparison(b *testing.B) {
	expiration := time.Now().Add(time.Hour)

	b.Run("Original", func(b *testing.B) {
		wheel := NewOptimizedTTLWheel(time.Second)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wheel.Set("key"+strconv.Itoa(i), expiration)
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		wheel := NewOptimizedTTLWheel(time.Second)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wheel.Set("key"+strconv.Itoa(i), expiration)
		}
	})

	b.Run("UltraOptimized", func(b *testing.B) {
		wheel := NewUltraOptimizedTTLWheel(time.Second)
		expirationNano := expiration.UnixNano()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wheel.SetUnsafe("key"+strconv.Itoa(i), expirationNano)
		}
	})
}

func BenchmarkTTLWheelExpiredComparison(b *testing.B) {
	numKeys := 1000
	futureTime := time.Now().Add(time.Hour)
	pastTime := time.Now().Add(-time.Hour)

	b.Run("Original", func(b *testing.B) {
		wheel := NewOptimizedTTLWheel(time.Second)
		for i := 0; i < numKeys; i++ {
			if i%2 == 0 {
				wheel.Set("key"+strconv.Itoa(i), futureTime)
			} else {
				wheel.Set("key"+strconv.Itoa(i), pastTime)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wheel.Expired("key" + strconv.Itoa(i%numKeys))
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		wheel := NewOptimizedTTLWheel(time.Second)
		for i := 0; i < numKeys; i++ {
			if i%2 == 0 {
				wheel.Set("key"+strconv.Itoa(i), futureTime)
			} else {
				wheel.Set("key"+strconv.Itoa(i), pastTime)
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wheel.Expired("key" + strconv.Itoa(i%numKeys))
		}
	})

	b.Run("UltraOptimized", func(b *testing.B) {
		wheel := NewUltraOptimizedTTLWheel(time.Second)
		futureNano := futureTime.UnixNano()
		pastNano := pastTime.UnixNano()
		for i := 0; i < numKeys; i++ {
			if i%2 == 0 {
				wheel.SetUnsafe("key"+strconv.Itoa(i), futureNano)
			} else {
				wheel.SetUnsafe("key"+strconv.Itoa(i), pastNano)
			}
		}
		nowNano := time.Now().UnixNano()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wheel.ExpiredUnsafe("key"+strconv.Itoa(i%numKeys), nowNano)
		}
	})
}

// Time operation benchmarks
func BenchmarkTimeOperations(b *testing.B) {
	b.Run("TimeNow", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = time.Now()
		}
	})

	b.Run("TimeNowUnixNano", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = time.Now().UnixNano()
		}
	})

	b.Run("CachedTimeNow", func(b *testing.B) {
		cached := NewCachedTimeNow(10 * time.Millisecond)
		defer cached.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = cached.Now()
		}
	})
}

func BenchmarkTimeComparison(b *testing.B) {
	now := time.Now()
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)

	b.Run("TimeAfter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				_ = now.After(future)
			} else {
				_ = now.After(past)
			}
		}
	})

	b.Run("Int64Comparison", func(b *testing.B) {
		nowNano := now.UnixNano()
		futureNano := future.UnixNano()
		pastNano := past.UnixNano()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				_ = nowNano > futureNano
			} else {
				_ = nowNano > pastNano
			}
		}
	})
}

func BenchmarkBatchExpiredCheck(b *testing.B) {
	wheel := NewUltraOptimizedTTLWheel(time.Second)

	// Setup keys
	keys := make([]string, 100)
	nowNano := time.Now().UnixNano()
	futureNano := nowNano + int64(time.Hour)
	pastNano := nowNano - int64(time.Hour)

	for i := 0; i < 100; i++ {
		keys[i] = "key" + strconv.Itoa(i)
		if i%2 == 0 {
			wheel.SetUnsafe(keys[i], futureNano)
		} else {
			wheel.SetUnsafe(keys[i], pastNano)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wheel.BatchExpiredCheck(keys, nowNano)
	}
}

func BenchmarkCleanupExpired(b *testing.B) {
	b.Run("Optimized", func(b *testing.B) {
		wheel := NewOptimizedTTLWheel(time.Second)

		// Setup with expired and non-expired keys
		nowNano := time.Now().UnixNano()
		futureNano := nowNano + int64(time.Hour)
		pastNano := nowNano - int64(time.Hour)

		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				wheel.SetNano("key"+strconv.Itoa(i), futureNano)
			} else {
				wheel.SetNano("key"+strconv.Itoa(i), pastNano)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			wheel.CleanupExpired()
		}
	})

	b.Run("UltraOptimized", func(b *testing.B) {
		wheel := NewUltraOptimizedTTLWheel(time.Second)

		// Setup with expired and non-expired keys
		nowNano := time.Now().UnixNano()
		futureNano := nowNano + int64(time.Hour)
		pastNano := nowNano - int64(time.Hour)

		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				wheel.SetUnsafe("key"+strconv.Itoa(i), futureNano)
			} else {
				wheel.SetUnsafe("key"+strconv.Itoa(i), pastNano)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			expiredKeys := wheel.GetExpiredKeys(nowNano)
			_ = len(expiredKeys)
		}
	})
}
