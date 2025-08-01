package exchange

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/pkg/models"
)

// InfluxBatcher batches price data writes to InfluxDB
type InfluxBatcher struct {
	client    *database.InfluxClient
	logger    *logrus.Entry
	
	// Batching configuration
	batchSize     int
	flushInterval time.Duration
	
	// Batch buffer
	buffer    []*models.PriceData
	bufferMu  sync.Mutex
	
	// Error rate limiting
	lastErrorLog  time.Time
	errorCount    int
	errorInterval time.Duration // Minimum time between error logs
	
	// Control
	done      chan struct{}
	wg        sync.WaitGroup
}

// NewInfluxBatcher creates a new InfluxDB batcher
func NewInfluxBatcher(client *database.InfluxClient, logger *logrus.Logger) *InfluxBatcher {
	return &InfluxBatcher{
		client:        client,
		logger:        logger.WithField("component", "influx-batcher"),
		batchSize:     100,            // Write in batches of 100
		flushInterval: 100 * time.Millisecond, // Flush every 100ms
		buffer:        make([]*models.PriceData, 0, 100),
		errorInterval: 5 * time.Second, // Log errors at most once every 5 seconds
		done:          make(chan struct{}),
	}
}

// Start starts the batcher
func (ib *InfluxBatcher) Start() {
	ib.wg.Add(1)
	go ib.flushLoop()
	ib.logger.Info("InfluxDB batcher started")
}

// Stop stops the batcher
func (ib *InfluxBatcher) Stop() {
	close(ib.done)
	ib.wg.Wait()
	
	// Flush any remaining data
	ib.flush()
	ib.logger.Info("InfluxDB batcher stopped")
}

// Write adds price data to the batch
func (ib *InfluxBatcher) Write(price *models.PriceData) error {
	ib.bufferMu.Lock()
	ib.buffer = append(ib.buffer, price)
	shouldFlush := len(ib.buffer) >= ib.batchSize
	ib.bufferMu.Unlock()
	
	// Flush immediately if batch is full
	if shouldFlush {
		go ib.flush()
	}
	
	return nil
}

// flushLoop periodically flushes the buffer
func (ib *InfluxBatcher) flushLoop() {
	defer ib.wg.Done()
	
	ticker := time.NewTicker(ib.flushInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ib.done:
			return
		case <-ticker.C:
			ib.flush()
		}
	}
}

// flush writes buffered data to InfluxDB
func (ib *InfluxBatcher) flush() {
	ib.bufferMu.Lock()
	if len(ib.buffer) == 0 {
		ib.bufferMu.Unlock()
		return
	}
	
	// Copy buffer and reset
	batch := make([]*models.PriceData, len(ib.buffer))
	copy(batch, ib.buffer)
	ib.buffer = ib.buffer[:0]
	ib.bufferMu.Unlock()
	
	// Write batch to InfluxDB
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := ib.client.WritePriceBatch(ctx, batch); err != nil {
		ib.handleError(err, len(batch))
	} else {
		// Reset error count on success
		ib.errorCount = 0
	}
}

// handleError handles write errors with rate limiting
func (ib *InfluxBatcher) handleError(err error, batchSize int) {
	ib.errorCount++
	
	// Rate limit error logging
	now := time.Now()
	if now.Sub(ib.lastErrorLog) >= ib.errorInterval {
		ib.logger.WithFields(logrus.Fields{
			"error":       err,
			"batch_size":  batchSize,
			"error_count": ib.errorCount,
			"since_last":  now.Sub(ib.lastErrorLog),
		}).Error("Failed to write batch to InfluxDB")
		
		ib.lastErrorLog = now
		
		// Log additional info if errors are persistent
		if ib.errorCount > 10 {
			ib.logger.WithField("error_count", ib.errorCount).
				Warn("Persistent InfluxDB write errors detected. Check InfluxDB health and connection.")
		}
	}
}