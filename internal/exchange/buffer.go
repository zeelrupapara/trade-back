package exchange

import (
	"sync"
	"time"

	"github.com/trade-back/pkg/models"
)

// CircularBuffer is a thread-safe circular buffer for price data
type CircularBuffer struct {
	data       []*models.PriceData
	head       int
	tail       int
	size       int
	capacity   int
	totalMsgs  uint64
	
	// Symbol index for fast lookups
	symbolIndex map[string][]int
	
	mu         sync.RWMutex
}

// BufferStats contains buffer statistics
type BufferStats struct {
	Size          int
	Capacity      int
	TotalMessages uint64
	OldestTime    time.Time
	NewestTime    time.Time
}

// NewCircularBuffer creates a new circular buffer
func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		data:        make([]*models.PriceData, capacity),
		capacity:    capacity,
		symbolIndex: make(map[string][]int),
	}
}

// Add adds a price data entry to the buffer
func (cb *CircularBuffer) Add(price *models.PriceData) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	// Check if buffer is full
	if cb.size == cb.capacity {
		// Remove oldest element from index
		if oldest := cb.data[cb.tail]; oldest != nil {
			cb.removeFromIndex(oldest.Symbol, cb.tail)
		}
		
		// Move tail forward (overwrite oldest)
		cb.tail = (cb.tail + 1) % cb.capacity
	} else {
		cb.size++
	}
	
	// Add new element
	cb.data[cb.head] = price
	cb.addToIndex(price.Symbol, cb.head)
	
	// Move head forward
	cb.head = (cb.head + 1) % cb.capacity
	cb.totalMsgs++
}

// Get retrieves the most recent n messages
func (cb *CircularBuffer) GetRecent(n int) []*models.PriceData {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	if n > cb.size {
		n = cb.size
	}
	
	result := make([]*models.PriceData, 0, n)
	
	// Start from the most recent (head - 1) and go backwards
	idx := (cb.head - 1 + cb.capacity) % cb.capacity
	for i := 0; i < n; i++ {
		if cb.data[idx] != nil {
			result = append(result, cb.data[idx])
		}
		idx = (idx - 1 + cb.capacity) % cb.capacity
	}
	
	return result
}

// GetRecentBySymbol retrieves the most recent n messages for a specific symbol
func (cb *CircularBuffer) GetRecentBySymbol(symbol string, n int) []*models.PriceData {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	indices, exists := cb.symbolIndex[symbol]
	if !exists || len(indices) == 0 {
		return nil
	}
	
	// Get the most recent n indices
	start := len(indices) - n
	if start < 0 {
		start = 0
	}
	
	result := make([]*models.PriceData, 0, n)
	for i := start; i < len(indices); i++ {
		if data := cb.data[indices[i]]; data != nil {
			result = append(result, data)
		}
	}
	
	return result
}

// GetRecentBySymbols retrieves recent messages for multiple symbols
func (cb *CircularBuffer) GetRecentBySymbols(symbols []string, nPerSymbol int) []*models.PriceData {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	result := make([]*models.PriceData, 0)
	
	for _, symbol := range symbols {
		indices, exists := cb.symbolIndex[symbol]
		if !exists || len(indices) == 0 {
			continue
		}
		
		// Get the most recent n indices for this symbol
		start := len(indices) - nPerSymbol
		if start < 0 {
			start = 0
		}
		
		for i := start; i < len(indices); i++ {
			if data := cb.data[indices[i]]; data != nil {
				result = append(result, data)
			}
		}
	}
	
	return result
}

// GetRange retrieves messages within a time range
func (cb *CircularBuffer) GetRange(from, to time.Time) []*models.PriceData {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	result := make([]*models.PriceData, 0)
	
	// Iterate through all data
	if cb.size == 0 {
		return result
	}
	
	// Start from tail (oldest) to head (newest)
	idx := cb.tail
	for i := 0; i < cb.size; i++ {
		if data := cb.data[idx]; data != nil {
			if data.Timestamp.After(from) && data.Timestamp.Before(to) {
				result = append(result, data)
			} else if data.Timestamp.After(to) {
				// Since messages are ordered by time, we can stop here
				break
			}
		}
		idx = (idx + 1) % cb.capacity
	}
	
	return result
}

// FindGaps finds sequence gaps for a symbol
func (cb *CircularBuffer) FindGaps(symbol string) []SequenceGap {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	indices, exists := cb.symbolIndex[symbol]
	if !exists || len(indices) < 2 {
		return nil
	}
	
	var gaps []SequenceGap
	
	for i := 1; i < len(indices); i++ {
		prev := cb.data[indices[i-1]]
		curr := cb.data[indices[i]]
		
		if prev == nil || curr == nil {
			continue
		}
		
		expectedSeq := prev.Sequence + 1
		if curr.Sequence > expectedSeq {
			gaps = append(gaps, SequenceGap{
				Symbol:    symbol,
				FromSeq:   expectedSeq,
				ToSeq:     curr.Sequence - 1,
				GapSize:   curr.Sequence - expectedSeq,
				FromTime:  prev.Timestamp,
				ToTime:    curr.Timestamp,
			})
		}
	}
	
	return gaps
}

// Stats returns buffer statistics
func (cb *CircularBuffer) Stats() BufferStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	stats := BufferStats{
		Size:          cb.size,
		Capacity:      cb.capacity,
		TotalMessages: cb.totalMsgs,
	}
	
	if cb.size > 0 {
		// Find oldest
		if oldest := cb.data[cb.tail]; oldest != nil {
			stats.OldestTime = oldest.Timestamp
		}
		
		// Find newest
		newestIdx := (cb.head - 1 + cb.capacity) % cb.capacity
		if newest := cb.data[newestIdx]; newest != nil {
			stats.NewestTime = newest.Timestamp
		}
	}
	
	return stats
}

// Clear clears the buffer
func (cb *CircularBuffer) Clear() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	cb.data = make([]*models.PriceData, cb.capacity)
	cb.symbolIndex = make(map[string][]int)
	cb.head = 0
	cb.tail = 0
	cb.size = 0
}

// Size returns the current size of the buffer
func (cb *CircularBuffer) Size() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.size
}

// IsFull returns whether the buffer is full
func (cb *CircularBuffer) IsFull() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.size == cb.capacity
}

// addToIndex adds an index to the symbol index
func (cb *CircularBuffer) addToIndex(symbol string, idx int) {
	cb.symbolIndex[symbol] = append(cb.symbolIndex[symbol], idx)
	
	// Keep index size reasonable (max 1000 per symbol)
	if len(cb.symbolIndex[symbol]) > 1000 {
		cb.symbolIndex[symbol] = cb.symbolIndex[symbol][100:] // Remove oldest 100
	}
}

// removeFromIndex removes an index from the symbol index
func (cb *CircularBuffer) removeFromIndex(symbol string, idx int) {
	indices := cb.symbolIndex[symbol]
	for i, index := range indices {
		if index == idx {
			// Remove this index
			cb.symbolIndex[symbol] = append(indices[:i], indices[i+1:]...)
			break
		}
	}
	
	// Clean up empty entries
	if len(cb.symbolIndex[symbol]) == 0 {
		delete(cb.symbolIndex, symbol)
	}
}

// SequenceGap represents a gap in sequence numbers
type SequenceGap struct {
	Symbol   string
	FromSeq  uint64
	ToSeq    uint64
	GapSize  uint64
	FromTime time.Time
	ToTime   time.Time
}

// GetSymbolCount returns the number of unique symbols in the buffer
func (cb *CircularBuffer) GetSymbolCount() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return len(cb.symbolIndex)
}

// GetSymbols returns all unique symbols in the buffer
func (cb *CircularBuffer) GetSymbols() []string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	
	symbols := make([]string, 0, len(cb.symbolIndex))
	for symbol := range cb.symbolIndex {
		symbols = append(symbols, symbol)
	}
	return symbols
}