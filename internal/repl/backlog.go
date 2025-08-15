package repl

import (
	"sync"
)

// Backlog represents a circular buffer for replication commands
type Backlog struct {
	mu   sync.RWMutex
	buf  []byte
	cap  int
	base int64
}

// NewBacklog creates a new backlog with the specified capacity
func NewBacklog(n int) *Backlog {
	return &Backlog{buf: make([]byte, 0, n), cap: n}
}

// Append adds data to the backlog
func (b *Backlog) Append(p []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buf = append(b.buf, p...)
	if len(b.buf) > b.cap {
		drop := len(b.buf) - b.cap
		b.buf = b.buf[drop:]
		b.base += int64(drop)
	}
}

// Offset returns the current offset (base + length)
func (b *Backlog) Offset() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.base + int64(len(b.buf))
}

// ReadFrom reads data from the specified offset
func (b *Backlog) ReadFrom(off int64, n int) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if off < b.base {
		off = b.base
	}
	idx := int(off - b.base)
	end := idx + n
	if idx > len(b.buf) {
		return nil
	}
	if end > len(b.buf) {
		end = len(b.buf)
	}
	out := make([]byte, end-idx)
	copy(out, b.buf[idx:end])
	return out
}

// Size returns the current size of the backlog
func (b *Backlog) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.buf)
}

// Capacity returns the maximum capacity of the backlog
func (b *Backlog) Capacity() int {
	return b.cap
}

// Clear clears the backlog
func (b *Backlog) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = b.buf[:0]
	b.base = 0
}
