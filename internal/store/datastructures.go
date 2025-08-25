package store

import (
	"fmt"
	"gridhouse/internal/logger"
	"sort"
	"strconv"
	"sync"
	"time"
)

// DataType represents the type of data structure
type DataType int

const (
	TypeString DataType = iota
	TypeList
	TypeSet
	TypeHash
	TypeSortedSet
	TypeStream
)

// List represents a Redis list (doubly-linked list) - compatibility wrapper
type List struct {
	*OptimizedList
}

// NewList creates a new list (compatibility wrapper)
func NewList() *List {
	return &List{NewOptimizedList()}
}

// Set represents a Redis set - compatibility wrapper
type Set struct {
	*OptimizedSet
}

// NewSet creates a new set (compatibility wrapper)
func NewSet() *Set {
	return &Set{NewOptimizedSet()}
}

// Hash represents a Redis hash - compatibility wrapper
type Hash struct {
	*OptimizedHash
}

// NewHash creates a new hash (compatibility wrapper)
func NewHash() *Hash {
	return &Hash{NewOptimizedHash()}
}

// Add missing methods to List compatibility wrapper
func (l *List) LRem(count int, element string) int {
	return l.OptimizedList.LRem(count, element)
}

func (l *List) LTrim(start, stop int) {
	l.OptimizedList.LTrim(start, stop)
}

// Add missing methods to Hash compatibility wrapper
func (h *Hash) HIncrBy(field string, increment int64) (int64, error) {
	return h.OptimizedHash.HIncrBy(field, increment)
}

func (h *Hash) HIncrByFloat(field string, increment float64) (float64, error) {
	return h.OptimizedHash.HIncrByFloat(field, increment)
}

// OptimizedList represents a high-performance Redis list using a deque implementation
type OptimizedList struct {
	mu    sync.RWMutex
	items []string
	head  int // Index of first element
	tail  int // Index of last element + 1
	cap   int // Capacity of underlying slice
}

// NewOptimizedList creates a new high-performance list
func NewOptimizedList() *OptimizedList {
	initialCap := 16
	return &OptimizedList{
		items: make([]string, initialCap),
		head:  initialCap / 2,
		tail:  initialCap / 2,
		cap:   initialCap,
	}
}

// LPush adds elements to the left (head) of the list - OPTIMIZED
func (l *OptimizedList) LPush(elements ...string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if we need to grow or shift
	if l.head-len(elements) < 0 {
		l.grow(len(elements))
	}

	// Add elements to the left
	for i := len(elements) - 1; i >= 0; i-- {
		l.head--
		l.items[l.head] = elements[i]
	}

	length := l.tail - l.head
	logger.Debugf("LPUSH added %d elements, list length: %d", len(elements), length)
	return length
}

// RPush adds elements to the right (tail) of the list - OPTIMIZED
func (l *OptimizedList) RPush(elements ...string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if we need to grow
	if l.tail+len(elements) > l.cap {
		l.grow(len(elements))
	}

	// Add elements to the right
	for _, element := range elements {
		l.items[l.tail] = element
		l.tail++
	}

	length := l.tail - l.head
	logger.Debugf("RPUSH added %d elements, list length: %d", len(elements), length)
	return length
}

// grow expands the list capacity and recenters elements
func (l *OptimizedList) grow(minSpace int) {
	currentLen := l.tail - l.head
	newCap := l.cap * 2

	// Ensure we have enough space
	for newCap < currentLen+minSpace+16 {
		newCap *= 2
	}

	newItems := make([]string, newCap)
	newHead := (newCap - currentLen) / 2

	// Copy existing elements to new slice
	copy(newItems[newHead:], l.items[l.head:l.tail])

	l.items = newItems
	l.head = newHead
	l.tail = newHead + currentLen
	l.cap = newCap
}

// LPop removes and returns the leftmost element - OPTIMIZED
func (l *OptimizedList) LPop() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head >= l.tail {
		return "", false
	}

	element := l.items[l.head]
	l.items[l.head] = "" // Clear reference for GC
	l.head++

	// Shrink if needed
	l.maybeShrink()

	logger.Debugf("LPOP returned '%s', list length: %d", element, l.tail-l.head)
	return element, true
}

// RPop removes and returns the rightmost element - OPTIMIZED
func (l *OptimizedList) RPop() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head >= l.tail {
		return "", false
	}

	l.tail--
	element := l.items[l.tail]
	l.items[l.tail] = "" // Clear reference for GC

	// Shrink if needed
	l.maybeShrink()

	logger.Debugf("RPOP returned '%s', list length: %d", element, l.tail-l.head)
	return element, true
}

// maybeShrink reduces capacity if the list is much smaller than allocated
func (l *OptimizedList) maybeShrink() {
	currentLen := l.tail - l.head
	if currentLen == 0 {
		// Reset to center for optimal future operations
		l.head = l.cap / 2
		l.tail = l.cap / 2
	} else if l.cap > 64 && currentLen < l.cap/4 {
		// Shrink and recenter
		newCap := l.cap / 2
		newItems := make([]string, newCap)
		newHead := (newCap - currentLen) / 2

		copy(newItems[newHead:], l.items[l.head:l.tail])

		l.items = newItems
		l.head = newHead
		l.tail = newHead + currentLen
		l.cap = newCap
	}
}

// LLen returns the length of the list - OPTIMIZED
func (l *OptimizedList) LLen() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.tail - l.head
}

// LRange returns elements from start to stop (inclusive) - OPTIMIZED
func (l *OptimizedList) LRange(start, stop int) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	length := l.tail - l.head
	if length == 0 {
		return []string{}
	}

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Bounds checking
	if start >= length || start > stop {
		return []string{}
	}
	if stop >= length {
		stop = length - 1
	}

	// Calculate actual indices in the items slice
	actualStart := l.head + start
	actualStop := l.head + stop + 1

	// Create result slice
	result := make([]string, actualStop-actualStart)
	copy(result, l.items[actualStart:actualStop])
	return result
}

// LIndex returns the element at the specified index - OPTIMIZED
func (l *OptimizedList) LIndex(index int) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	length := l.tail - l.head
	if length == 0 {
		return "", false
	}

	// Handle negative indices
	if index < 0 {
		index = length + index
	}

	if index < 0 || index >= length {
		return "", false
	}

	return l.items[l.head+index], true
}

// LSet sets the element at the specified index - OPTIMIZED
func (l *OptimizedList) LSet(index int, value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	length := l.tail - l.head
	if length == 0 {
		return false
	}

	// Handle negative indices
	if index < 0 {
		index = length + index
	}

	if index < 0 || index >= length {
		return false
	}

	l.items[l.head+index] = value
	logger.Debugf("LSET index %d = '%s'", index, value)
	return true
}

// LRem removes elements from the list - OPTIMIZED
func (l *OptimizedList) LRem(count int, element string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head >= l.tail {
		return 0
	}

	items := l.items[l.head:l.tail]
	length := len(items)

	// Handle different count values
	var toRemove []int
	if count == 0 {
		// Remove all occurrences
		for i := 0; i < length; i++ {
			if items[i] == element {
				toRemove = append(toRemove, i)
			}
		}
	} else if count > 0 {
		// Remove first 'count' occurrences from left
		for i := 0; i < length && len(toRemove) < count; i++ {
			if items[i] == element {
				toRemove = append(toRemove, i)
			}
		}
	} else {
		// Remove first '|count|' occurrences from right
		count = -count
		for i := length - 1; i >= 0 && len(toRemove) < count; i-- {
			if items[i] == element {
				toRemove = append(toRemove, i)
			}
		}
	}

	if len(toRemove) == 0 {
		return 0
	}

	// Rebuild the list without removed elements
	newItems := make([]string, 0, length-len(toRemove))
	removeIndex := 0
	for i := 0; i < length; i++ {
		if removeIndex < len(toRemove) && i == toRemove[removeIndex] {
			removeIndex++
		} else {
			newItems = append(newItems, items[i])
		}
	}

	// Update the list
	l.items = make([]string, l.cap)
	copy(l.items, newItems)
	l.head = 0
	l.tail = len(newItems)

	return len(toRemove)
}

// LTrim trims the list to the specified range - OPTIMIZED
func (l *OptimizedList) LTrim(start, stop int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head >= l.tail {
		return
	}

	items := l.items[l.head:l.tail]
	length := len(items)

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp indices
	if start < 0 {
		start = 0
	}
	if start >= length {
		start = length
	}
	if stop >= length {
		stop = length - 1
	}
	if stop < start {
		// Invalid range, result should be empty
		l.items = make([]string, l.cap)
		l.head = 0
		l.tail = 0
		return
	}

	// Extract the range
	trimmedItems := items[start : stop+1]

	// Update the list
	l.items = make([]string, l.cap)
	copy(l.items, trimmedItems)
	l.head = 0
	l.tail = len(trimmedItems)
}

// OptimizedSet represents a high-performance Redis set
type OptimizedSet struct {
	mu    sync.RWMutex
	items map[string]struct{} // Use struct{} instead of bool for better memory efficiency
}

// NewOptimizedSet creates a new high-performance set
func NewOptimizedSet() *OptimizedSet {
	return &OptimizedSet{
		items: make(map[string]struct{}),
	}
}

// SAdd adds elements to the set - OPTIMIZED
func (s *OptimizedSet) SAdd(elements ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := 0
	for _, element := range elements {
		if _, exists := s.items[element]; !exists {
			s.items[element] = struct{}{}
			added++
		}
	}

	logger.Debugf("SADD added %d new elements, set size: %d", added, len(s.items))
	return added
}

// SRem removes elements from the set - OPTIMIZED
func (s *OptimizedSet) SRem(elements ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, element := range elements {
		if _, exists := s.items[element]; exists {
			delete(s.items, element)
			removed++
		}
	}

	logger.Debugf("SREM removed %d elements, set size: %d", removed, len(s.items))
	return removed
}

// SIsMember checks if an element is a member of the set - OPTIMIZED
func (s *OptimizedSet) SIsMember(element string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.items[element]
	return exists
}

// SMembers returns all members of the set - OPTIMIZED
func (s *OptimizedSet) SMembers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	members := make([]string, 0, len(s.items))
	for member := range s.items {
		members = append(members, member)
	}
	return members
}

// SCard returns the number of elements in the set - OPTIMIZED
func (s *OptimizedSet) SCard() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// SPop removes and returns a random element from the set - OPTIMIZED
func (s *OptimizedSet) SPop() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return "", false
	}

	// Get a random element (first one in iteration)
	for element := range s.items {
		delete(s.items, element)
		logger.Debugf("SPOP returned '%s', set size: %d", element, len(s.items))
		return element, true
	}

	return "", false
}

// OptimizedHash represents a high-performance Redis hash
type OptimizedHash struct {
	mu     sync.RWMutex
	fields map[string]string
}

// NewOptimizedHash creates a new high-performance hash
func NewOptimizedHash() *OptimizedHash {
	return &OptimizedHash{
		fields: make(map[string]string),
	}
}

// HSet sets a field in the hash - OPTIMIZED
func (h *OptimizedHash) HSet(field, value string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, exists := h.fields[field]
	h.fields[field] = value

	logger.Debugf("HSET %s = '%s' (existed: %v)", field, value, exists)
	return !exists // Return true if field was new
}

// HGet gets a field from the hash - OPTIMIZED
func (h *OptimizedHash) HGet(field string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	value, exists := h.fields[field]
	return value, exists
}

// HDel removes fields from the hash - OPTIMIZED
func (h *OptimizedHash) HDel(fields ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	removed := 0
	for _, field := range fields {
		if _, exists := h.fields[field]; exists {
			delete(h.fields, field)
			removed++
		}
	}

	logger.Debugf("HDEL removed %d fields", removed)
	return removed
}

// HExists checks if a field exists in the hash - OPTIMIZED
func (h *OptimizedHash) HExists(field string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, exists := h.fields[field]
	return exists
}

// HGetAll returns all field-value pairs in the hash - OPTIMIZED
func (h *OptimizedHash) HGetAll() map[string]string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[string]string, len(h.fields))
	for field, value := range h.fields {
		result[field] = value
	}
	return result
}

// HKeys returns all field names in the hash - OPTIMIZED
func (h *OptimizedHash) HKeys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	keys := make([]string, 0, len(h.fields))
	for field := range h.fields {
		keys = append(keys, field)
	}
	return keys
}

// HVals returns all values in the hash - OPTIMIZED
func (h *OptimizedHash) HVals() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	values := make([]string, 0, len(h.fields))
	for _, value := range h.fields {
		values = append(values, value)
	}
	return values
}

// HLen returns the number of fields in the hash - OPTIMIZED
func (h *OptimizedHash) HLen() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.fields)
}

// HIncrBy increments a field by an integer value - OPTIMIZED
func (h *OptimizedHash) HIncrBy(field string, increment int64) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	currentValue, exists := h.fields[field]
	if !exists {
		currentValue = "0"
	}

	// Parse current value
	current, err := strconv.ParseInt(currentValue, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer or out of range")
	}

	// Add increment
	newValue := current + increment
	h.fields[field] = strconv.FormatInt(newValue, 10)

	return newValue, nil
}

// HIncrByFloat increments a field by a float value - OPTIMIZED
func (h *OptimizedHash) HIncrByFloat(field string, increment float64) (float64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	currentValue, exists := h.fields[field]
	if !exists {
		currentValue = "0"
	}

	// Parse current value
	current, err := strconv.ParseFloat(currentValue, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not a valid float")
	}

	// Add increment
	newValue := current + increment
	h.fields[field] = strconv.FormatFloat(newValue, 'f', -1, 64)

	return newValue, nil
}

// ===== Sorted Set (ZSET) =====

// SortedSet is a compatibility wrapper
type SortedSet struct {
	*OptimizedSortedSet
}

func NewSortedSet() *SortedSet { return &SortedSet{NewOptimizedSortedSet()} }

// entry holds member and score
type entry struct {
	Member string
	Score  float64
}

// OptimizedSortedSet is a simple sorted set implementation
// Note: optimized for simplicity over absolute performance
// Maintains a map for scores and a slice sorted by (score, member)
type OptimizedSortedSet struct {
	mu     sync.RWMutex
	scores map[string]float64
	order  []entry
	dirty  bool // true if order needs rebuild
}

func NewOptimizedSortedSet() *OptimizedSortedSet {
	return &OptimizedSortedSet{scores: make(map[string]float64), order: make([]entry, 0)}
}

// ZAdd adds or updates members with scores. Returns count of new insertions.
func (z *OptimizedSortedSet) ZAdd(pairs map[string]float64) int {
	z.mu.Lock()
	defer z.mu.Unlock()
	added := 0
	for member, score := range pairs {
		if _, exists := z.scores[member]; !exists {
			added++
		}
		z.scores[member] = score
	}
	// Mark as dirty; defer rebuild until a read path requires it
	z.dirty = true
	return added
}

// ZRem removes members, returns count removed
func (z *OptimizedSortedSet) ZRem(members ...string) int {
	z.mu.Lock()
	defer z.mu.Unlock()
	removed := 0
	for _, m := range members {
		if _, ok := z.scores[m]; ok {
			delete(z.scores, m)
			removed++
		}
	}
	if removed > 0 {
		z.dirty = true
	}
	return removed
}

// ZCard returns number of members
func (z *OptimizedSortedSet) ZCard() int {
	z.mu.RLock()
	defer z.mu.RUnlock()
	return len(z.scores)
}

// ZScore returns the score for a member
func (z *OptimizedSortedSet) ZScore(member string) (float64, bool) {
	z.mu.RLock()
	defer z.mu.RUnlock()
	s, ok := z.scores[member]
	return s, ok
}

// ZRange returns members in index range [start, stop] inclusive, supports negative indices
// If withScores is true, returns alternating member and score strings
func (z *OptimizedSortedSet) ZRange(start, stop int, withScores bool) []string {
	// We need a consistent sorted view; rebuild lazily if needed
	z.mu.Lock()
	if z.dirty {
		z.rebuild()
	}
	// Downgrade-like behavior: we'll keep the write lock for simplicity and correctness
	defer z.mu.Unlock()
	n := len(z.order)
	if n == 0 {
		return []string{}
	}
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return []string{}
	}
	res := make([]string, 0, (stop-start+1)*(1+boolToInt(withScores)))
	for i := start; i <= stop; i++ {
		res = append(res, z.order[i].Member)
		if withScores {
			res = append(res, strconv.FormatFloat(z.order[i].Score, 'f', -1, 64))
		}
	}
	return res
}

func (z *OptimizedSortedSet) Rebuild() {
	z.rebuild()
}

func (z *OptimizedSortedSet) rebuild() {
	// assumes caller holds z.mu (write)
	n := len(z.scores)
	if cap(z.order) < n {
		z.order = make([]entry, 0, n)
	} else {
		z.order = z.order[:0]
	}
	for m, s := range z.scores {
		z.order = append(z.order, entry{Member: m, Score: s})
	}
	// sort by score, then member lex
	sort.Slice(z.order, func(i, j int) bool {
		if z.order[i].Score == z.order[j].Score {
			return z.order[i].Member < z.order[j].Member
		}
		return z.order[i].Score < z.order[j].Score
	})
	z.dirty = false
}

// ZPopMin pops up to count elements with the lowest scores and returns them in order
func (z *OptimizedSortedSet) ZPopMin(count int) []entry {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.dirty {
		z.rebuild()
	}
	if count < 1 {
		return []entry{}
	}
	n := len(z.order)
	if n == 0 {
		return []entry{}
	}
	if count > n {
		count = n
	}
	res := make([]entry, count)
	copy(res, z.order[:count])
	// delete from map
	for _, e := range res {
		delete(z.scores, e.Member)
	}
	// keep remaining order
	remaining := make([]entry, n-count)
	copy(remaining, z.order[count:])
	z.order = remaining
	return res
}

func boolToInt(b bool) int {
	if b {
		return 2
	}
	return 1
}

// ===== Streams (XADD minimal) =====

// StreamID represents a stream entry ID of the form millis-seq
type StreamID struct {
	Ms  uint64
	Seq uint64
}

func (id StreamID) String() string {
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

// StreamEntry is a single entry with ID and fields
type StreamEntry struct {
	ID     StreamID
	Fields map[string]string
}

// Stream is a very simple append-only structure keeping entries in order
// Not optimized for large histories; enough for basic XADD.
type Stream struct {
	mu      sync.Mutex
	entries []StreamEntry
	lastMs  uint64
	lastSeq uint64
}

func NewStream() *Stream {
	return &Stream{entries: make([]StreamEntry, 0, 16)}
}

// XAdd appends a new entry. If providedID is nil, it autogenerates based on current time ms.
// If provided is not nil, it must be >= last ID and not duplicate.
func (s *Stream) XAdd(provided *StreamID, fields map[string]string) (StreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var id StreamID
	if provided == nil {
		// auto-generate: ms now, seq incremental if same ms
		nowMs := uint64(time.Now().UnixMilli())
		if nowMs == s.lastMs {
			id = StreamID{Ms: nowMs, Seq: s.lastSeq + 1}
		} else if nowMs > s.lastMs {
			id = StreamID{Ms: nowMs, Seq: 0}
		} else {
			// clock went backwards; still make it monotonic
			id = StreamID{Ms: s.lastMs, Seq: s.lastSeq + 1}
		}
	} else {
		// custom id
		id = *provided
		// must be greater than last
		if id.Ms < s.lastMs || (id.Ms == s.lastMs && id.Seq <= s.lastSeq) {
			return StreamID{}, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	s.entries = append(s.entries, StreamEntry{ID: id, Fields: fields})
	s.lastMs = id.Ms
	s.lastSeq = id.Seq
	return id, nil
}

// XLen returns number of entries in the stream
func (s *Stream) XLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// compare IDs: returns -1 if a<b, 0 if equal, 1 if a>b
func compareStreamID(a, b StreamID) int {
	if a.Ms < b.Ms {
		return -1
	}
	if a.Ms > b.Ms {
		return 1
	}
	if a.Seq < b.Seq {
		return -1
	}
	if a.Seq > b.Seq {
		return 1
	}
	return 0
}

// findFirstGreater finds index of first entry with ID > target (for XREAD)
func (s *Stream) findFirstGreater(target StreamID) int {
	// entries are appended in order; linear scan is fine for minimal impl
	for i, e := range s.entries {
		if compareStreamID(e.ID, target) > 0 {
			return i
		}
	}
	return len(s.entries)
}

// findFirstAtLeast finds index of first entry with ID >= target (for XRANGE start)
func (s *Stream) findFirstAtLeast(target StreamID) int {
	for i, e := range s.entries {
		c := compareStreamID(e.ID, target)
		if c >= 0 {
			return i
		}
	}
	return len(s.entries)
}

// findLastAtMost finds index of last entry with ID <= target (for XRANGE end)
func (s *Stream) findLastAtMost(target StreamID) int {
	for i := len(s.entries) - 1; i >= 0; i-- {
		if compareStreamID(s.entries[i].ID, target) <= 0 {
			return i
		}
	}
	return -1
}

// XRange returns entries with start <= id <= end, optionally limited by count if >0
func (s *Stream) XRange(start, end StreamID, count int) []StreamEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return []StreamEntry{}
	}
	startIdx := s.findFirstAtLeast(start)
	endIdx := s.findLastAtMost(end)
	if startIdx == len(s.entries) || endIdx < 0 || startIdx > endIdx {
		return []StreamEntry{}
	}
	res := s.entries[startIdx : endIdx+1]
	if count > 0 && count < len(res) {
		res = res[:count]
	}
	// Copy to avoid external mutation
	out := make([]StreamEntry, len(res))
	copy(out, res)
	return out
}

// XReadAfter returns up to count entries with ID > after
func (s *Stream) XReadAfter(after StreamID, count int) []StreamEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.findFirstGreater(after)
	if idx >= len(s.entries) {
		return []StreamEntry{}
	}
	res := s.entries[idx:]
	if count > 0 && count < len(res) {
		res = res[:count]
	}
	out := make([]StreamEntry, len(res))
	copy(out, res)
	return out
}

// XDel deletes entries by IDs and returns how many were removed
func (s *Stream) XDel(ids []StreamID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 || len(ids) == 0 {
		return 0
	}
	// build a set for quick match
	target := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		target[id.String()] = struct{}{}
	}
	removed := 0
	newEntries := make([]StreamEntry, 0, len(s.entries))
	for _, e := range s.entries {
		if _, ok := target[e.ID.String()]; ok {
			removed++
			continue
		}
		newEntries = append(newEntries, e)
	}
	s.entries = newEntries
	return removed
}

// XTrimMaxLen trims the stream to keep at most maxLen newest entries. Returns removed count.
func (s *Stream) XTrimMaxLen(maxLen int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.entries)
	if maxLen < 0 || n <= maxLen {
		return 0
	}
	toRemove := n - maxLen
	// remove from the beginning (oldest)
	s.entries = append([]StreamEntry(nil), s.entries[toRemove:]...)
	return toRemove
}
