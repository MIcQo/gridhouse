package repl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBacklogClear(t *testing.T) {
	t.Run("clear_empty_backlog", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Clear empty backlog
		backlog.Clear()

		// Verify backlog is empty
		assert.Equal(t, 0, backlog.Size())
		assert.Equal(t, int64(0), backlog.Offset())
		assert.Equal(t, 100, backlog.Capacity())
	})

	t.Run("clear_populated_backlog", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		// Verify data was added
		assert.Equal(t, len(testData), backlog.Size())
		assert.Equal(t, int64(len(testData)), backlog.Offset())

		// Clear backlog
		backlog.Clear()

		// Verify backlog is empty
		assert.Equal(t, 0, backlog.Size())
		assert.Equal(t, int64(0), backlog.Offset())
		assert.Equal(t, 100, backlog.Capacity())
	})

	t.Run("clear_after_overflow", func(t *testing.T) {
		backlog := NewBacklog(10)

		// Add data that exceeds capacity
		largeData := []byte("this is larger than capacity")
		backlog.Append(largeData)

		// Verify overflow occurred
		assert.Equal(t, 10, backlog.Size())
		assert.Equal(t, int64(28), backlog.Offset()) // base should be 18 (28-10)

		// Clear backlog
		backlog.Clear()

		// Verify backlog is reset
		assert.Equal(t, 0, backlog.Size())
		assert.Equal(t, int64(0), backlog.Offset())
		assert.Equal(t, 10, backlog.Capacity())
	})

	t.Run("clear_multiple_times", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add data
		testData := []byte("test data")
		backlog.Append(testData)

		// Clear multiple times
		backlog.Clear()
		backlog.Clear()
		backlog.Clear()

		// Verify backlog is still empty
		assert.Equal(t, 0, backlog.Size())
		assert.Equal(t, int64(0), backlog.Offset())
	})
}

func TestBacklogReadFromEdgeCases(t *testing.T) {
	t.Run("read_from_before_base", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		// Try to read from before base
		result := backlog.ReadFrom(0, 5)

		// Should return data from base onwards
		assert.Equal(t, testData[:5], result)
	})

	t.Run("read_from_exact_base", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		base := backlog.Offset() - int64(len(testData))

		// Read from exact base
		result := backlog.ReadFrom(base, 5)

		// Should return first 5 bytes
		assert.Equal(t, testData[:5], result)
	})

	t.Run("read_from_after_data", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		// Try to read from after all data
		result := backlog.ReadFrom(backlog.Offset()+10, 5)

		// Should return nil
		assert.Nil(t, result)
	})

	t.Run("read_partial_data", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		base := backlog.Offset() - int64(len(testData))

		// Read more than available
		result := backlog.ReadFrom(base, 20)

		// Should return all available data
		assert.Equal(t, testData, result)
	})

	t.Run("read_zero_bytes", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		base := backlog.Offset() - int64(len(testData))

		// Read zero bytes
		result := backlog.ReadFrom(base, 0)

		// Should return empty slice
		assert.Equal(t, []byte{}, result)
	})

	t.Run("read_from_overflow_scenario", func(t *testing.T) {
		backlog := NewBacklog(10)

		// Add data that exceeds capacity
		largeData := []byte("this is larger than capacity")
		backlog.Append(largeData)

		// Verify overflow occurred
		assert.Equal(t, 10, backlog.Size())
		assert.Equal(t, int64(28), backlog.Offset())

		// Read from different positions
		result1 := backlog.ReadFrom(18, 5) // From base
		result2 := backlog.ReadFrom(21, 5) // From middle
		result3 := backlog.ReadFrom(24, 5) // From end
		result4 := backlog.ReadFrom(30, 5) // After data

		// Verify results - should contain the last 10 bytes of the large data
		assert.Equal(t, []byte("n cap"), result1)
		assert.Equal(t, []byte("apaci"), result2)
		assert.Equal(t, []byte("city"), result3)
		assert.Nil(t, result4)
	})
}

func TestBacklogConcurrentAccess(t *testing.T) {
	t.Run("concurrent_append_and_read", func(t *testing.T) {
		backlog := NewBacklog(1000)
		done := make(chan bool, 20)

		// Concurrent appends
		for i := 0; i < 10; i++ {
			go func(id int) {
				data := []byte(fmt.Sprintf("data_%d", id))
				backlog.Append(data)
				done <- true
			}(i)
		}

		// Concurrent reads
		for i := 0; i < 10; i++ {
			go func() {
				offset := backlog.Offset()
				if offset > 0 {
					backlog.ReadFrom(offset-10, 5)
				}
				done <- true
			}()
		}

		// Wait for all operations
		for i := 0; i < 20; i++ {
			<-done
		}

		// Verify backlog is still functional
		assert.Greater(t, backlog.Size(), 0)
		assert.Greater(t, backlog.Offset(), int64(0))
	})

	t.Run("concurrent_clear", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		done := make(chan bool, 5)

		// Concurrent clears
		for i := 0; i < 5; i++ {
			go func() {
				backlog.Clear()
				done <- true
			}()
		}

		// Wait for all operations
		for i := 0; i < 5; i++ {
			<-done
		}

		// Verify backlog is cleared
		assert.Equal(t, 0, backlog.Size())
		assert.Equal(t, int64(0), backlog.Offset())
	})
}

func TestBacklogMemoryEfficiency(t *testing.T) {
	t.Run("large_data_handling", func(t *testing.T) {
		backlog := NewBacklog(1024)

		// Add large amounts of data
		for i := 0; i < 100; i++ {
			data := make([]byte, 100)
			for j := range data {
				data[j] = byte(i + j)
			}
			backlog.Append(data)
		}

		// Verify size doesn't exceed capacity
		assert.LessOrEqual(t, backlog.Size(), backlog.Capacity())

		// Verify we can still read data
		offset := backlog.Offset()
		if offset > 0 {
			result := backlog.ReadFrom(offset-100, 50)
			assert.NotNil(t, result)
			assert.Len(t, result, 50)
		}
	})

	t.Run("frequent_overflow", func(t *testing.T) {
		backlog := NewBacklog(10)

		// Add data that frequently overflows
		for i := 0; i < 100; i++ {
			data := []byte(fmt.Sprintf("data_%d", i))
			backlog.Append(data)
		}

		// Verify size is maintained at capacity
		assert.Equal(t, backlog.Capacity(), backlog.Size())

		// Verify offset increases correctly
		assert.Greater(t, backlog.Offset(), int64(0))
	})
}

func TestBacklogBoundaryConditions(t *testing.T) {
	t.Run("zero_capacity", func(t *testing.T) {
		backlog := NewBacklog(0)

		// Try to append data
		testData := []byte("test")
		backlog.Append(testData)

		// Verify behavior with zero capacity - data should be dropped immediately
		assert.Equal(t, 0, backlog.Size())
		assert.Equal(t, int64(4), backlog.Offset()) // base should be 4 since we dropped 4 bytes
	})

	t.Run("negative_read_length", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		// Try to read negative length - should panic due to make([]byte, negative)
		assert.Panics(t, func() {
			backlog.ReadFrom(0, -5)
		})
	})

	t.Run("very_large_read_request", func(t *testing.T) {
		backlog := NewBacklog(100)

		// Add some data
		testData := []byte("test data")
		backlog.Append(testData)

		// Try to read very large amount
		result := backlog.ReadFrom(0, 1000000)

		// Should return available data
		assert.Equal(t, testData, result)
	})
}
