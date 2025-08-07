package exchange

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/models"
)

// PriceProcessor handles price updates with rate limiting and batching
type PriceProcessor struct {
	hub         *Hub
	priceQueue  chan *models.PriceData
	workerCount int
	batchSize   int
	batchTime   time.Duration
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *logrus.Entry
}

// NewPriceProcessor creates a new price processor
func NewPriceProcessor(hub *Hub, workerCount int) *PriceProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &PriceProcessor{
		hub:         hub,
		priceQueue:  make(chan *models.PriceData, 10000), // Large buffer
		workerCount: workerCount,
		batchSize:   100,
		batchTime:   50 * time.Millisecond,
		ctx:         ctx,
		cancel:      cancel,
		logger:      hub.logger.WithField("component", "price-processor"),
	}
}

// Start starts the price processor workers
func (pp *PriceProcessor) Start() {
	
	// Start worker goroutines
	for i := 0; i < pp.workerCount; i++ {
		pp.wg.Add(1)
		go pp.worker(i)
	}
}

// Stop stops the price processor
func (pp *PriceProcessor) Stop() {
	// Signal shutdown
	pp.cancel()
	
	// Give a small grace period for ongoing sends to complete
	time.Sleep(100 * time.Millisecond)
	
	// Now close the channel
	close(pp.priceQueue)
	
	// Wait for workers to finish
	pp.wg.Wait()
}

// ProcessPrice adds a price to the processing queue
func (pp *PriceProcessor) ProcessPrice(price *models.PriceData) {
	// Check if context is cancelled (shutting down)
	select {
	case <-pp.ctx.Done():
		// Processor is shutting down, ignore the price
		return
	default:
		// Continue with processing
	}
	
	select {
	case pp.priceQueue <- price:
		// Successfully queued
	case <-pp.ctx.Done():
		// Processor is shutting down
		return
	default:
		// Queue full, drop the price update
		pp.logger.WithField("symbol", price.Symbol).Warn("Price queue full, dropping update")
	}
}

// worker processes prices from the queue
func (pp *PriceProcessor) worker(id int) {
	defer pp.wg.Done()
	
	batch := make([]*models.PriceData, 0, pp.batchSize)
	ticker := time.NewTicker(pp.batchTime)
	defer ticker.Stop()
	
	for {
		select {
		case <-pp.ctx.Done():
			// Process remaining batch
			if len(batch) > 0 {
				pp.processBatch(batch)
			}
			return
			
		case price, ok := <-pp.priceQueue:
			if !ok {
				// Channel closed, process remaining batch
				if len(batch) > 0 {
					pp.processBatch(batch)
				}
				return
			}
			
			batch = append(batch, price)
			
			// Process batch if it's full
			if len(batch) >= pp.batchSize {
				pp.processBatch(batch)
				batch = batch[:0] // Reset batch
			}
			
		case <-ticker.C:
			// Process batch on timeout
			if len(batch) > 0 {
				pp.processBatch(batch)
				batch = batch[:0] // Reset batch
			}
		}
	}
}

// processBatch processes a batch of prices
func (pp *PriceProcessor) processBatch(batch []*models.PriceData) {
	// Group prices by symbol for deduplication
	symbolPrices := make(map[string]*models.PriceData)
	for _, price := range batch {
		// Keep only the latest price for each symbol
		if existing, ok := symbolPrices[price.Symbol]; !ok || price.Timestamp.After(existing.Timestamp) {
			symbolPrices[price.Symbol] = price
		}
	}
	
	// Process deduplicated prices
	for _, price := range symbolPrices {
		pp.hub.handlePriceUpdateDirect(price)
	}
}