package services

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/models"
)

// UltraFastHistoricalLoader provides extremely fast historical data loading
type UltraFastHistoricalLoader struct {
	influx       *database.InfluxClient
	mysql        *database.MySQLClient
	nats         *messaging.NATSClient
	binanceREST  *exchange.BinanceRESTClient
	logger       *logrus.Entry
	
	// Performance settings
	maxWorkers      int           // Number of concurrent workers
	batchSize       int           // Klines per batch
	chunkDays       int           // Days per chunk
	writeBatchSize  int           // Database write batch size
	maxRetries      int           // Max retries for failed requests
	retryDelay      time.Duration // Delay between retries
	
	// Progress tracking
	totalProgress   int64
	currentProgress int64
	
	// Memory pools for reduced allocations
	barPool   sync.Pool
	klinesPool sync.Pool
}

// NewUltraFastHistoricalLoader creates an ultra-fast loader with optimized settings
func NewUltraFastHistoricalLoader(
	influx *database.InfluxClient,
	mysql *database.MySQLClient,
	nats *messaging.NATSClient,
	logger *logrus.Logger,
) *UltraFastHistoricalLoader {
	// Use all available CPU cores
	numCPU := runtime.NumCPU()
	
	return &UltraFastHistoricalLoader{
		influx:       influx,
		mysql:        mysql,
		nats:         nats,
		binanceREST:  exchange.NewBinanceRESTClient(logger),
		logger:       logger.WithField("component", "ultra-fast-loader"),
		
		// Optimized settings for maximum performance
		maxWorkers:     numCPU * 2,        // 2 workers per CPU core
		batchSize:      1000,               // Large batch size for API calls
		chunkDays:      90,                 // Load 3 months at a time
		writeBatchSize: 5000,               // Write 5000 records at once
		maxRetries:     3,
		retryDelay:     time.Second,
		
		// Memory pools
		barPool: sync.Pool{
			New: func() interface{} {
				return &models.Bar{}
			},
		},
		klinesPool: sync.Pool{
			New: func() interface{} {
				return make([]exchange.HistoricalKline, 0, 1000)
			},
		},
	}
}

// LoadHistoricalDataUltraFast loads historical data with maximum performance
func (h *UltraFastHistoricalLoader) LoadHistoricalDataUltraFast(
	ctx context.Context,
	symbol string,
	interval string,
	days int,
) error {
	startTime := time.Now()
	h.logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
		"days":     days,
		"workers":  h.maxWorkers,
	}).Info("Starting ultra-fast historical data sync")
	
	// Update sync status
	if err := h.mysql.UpdateSyncStatus(ctx, symbol, "syncing", 0, 0, ""); err != nil {
		h.logger.WithError(err).Warn("Failed to update sync status")
	}
	
	// Calculate time ranges for parallel loading
	endTime := time.Now()
	startTimeTarget := endTime.AddDate(0, 0, -days)
	
	// Create time chunks for parallel processing
	chunks := h.createTimeChunks(startTimeTarget, endTime, h.chunkDays)
	totalChunks := len(chunks)
	
	// Progress tracking
	atomic.StoreInt64(&h.totalProgress, int64(totalChunks))
	atomic.StoreInt64(&h.currentProgress, 0)
	
	// Create channels for work distribution
	chunkChan := make(chan timeChunk, totalChunks)
	resultChan := make(chan *chunkResult, totalChunks)
	errorChan := make(chan error, totalChunks)
	
	// Start progress reporter
	stopProgress := make(chan struct{})
	go h.reportProgress(ctx, symbol, interval, stopProgress)
	
	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < h.maxWorkers; i++ {
		wg.Add(1)
		go h.worker(ctx, symbol, interval, chunkChan, resultChan, errorChan, &wg)
	}
	
	// Send work to workers
	for _, chunk := range chunks {
		select {
		case chunkChan <- chunk:
		case <-ctx.Done():
			close(chunkChan)
			close(stopProgress)
			return ctx.Err()
		}
	}
	close(chunkChan)
	
	// Start result processor
	doneChan := make(chan struct{})
	go h.processResults(ctx, symbol, interval, resultChan, doneChan)
	
	// Wait for workers to complete
	wg.Wait()
	close(resultChan)
	close(errorChan)
	
	// Wait for result processor
	<-doneChan
	close(stopProgress)
	
	// Check for errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}
	
	if len(errors) > 0 {
		h.mysql.UpdateSyncStatus(ctx, symbol, "failed", 
			int(atomic.LoadInt64(&h.currentProgress)), 
			int(atomic.LoadInt64(&h.totalProgress)), 
			fmt.Sprintf("%d errors occurred", len(errors)))
		return fmt.Errorf("sync completed with %d errors", len(errors))
	}
	
	// Update final status
	duration := time.Since(startTime)
	h.mysql.UpdateSyncStatus(ctx, symbol, "completed", 100, int(h.totalProgress), "")
	
	h.logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"duration": duration,
		"chunks":   totalChunks,
		"speed":    fmt.Sprintf("%.2f chunks/sec", float64(totalChunks)/duration.Seconds()),
	}).Info("Ultra-fast sync completed successfully")
	
	// Publish completion via NATS
	if h.nats != nil {
		h.nats.PublishSyncComplete(symbol, totalChunks, startTimeTarget, endTime)
	}
	
	return nil
}

// worker processes time chunks in parallel
func (h *UltraFastHistoricalLoader) worker(
	ctx context.Context,
	symbol string,
	interval string,
	chunkChan <-chan timeChunk,
	resultChan chan<- *chunkResult,
	errorChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	
	for chunk := range chunkChan {
		select {
		case <-ctx.Done():
			return
		default:
			result, err := h.processChunk(ctx, symbol, interval, chunk)
			if err != nil {
				// Retry logic
				for retry := 0; retry < h.maxRetries; retry++ {
					time.Sleep(h.retryDelay * time.Duration(retry+1))
					result, err = h.processChunk(ctx, symbol, interval, chunk)
					if err == nil {
						break
					}
				}
				
				if err != nil {
					errorChan <- err
					continue
				}
			}
			
			if result != nil {
				resultChan <- result
			}
			
			// Update progress
			atomic.AddInt64(&h.currentProgress, 1)
		}
	}
}

// processChunk fetches and prepares data for a time chunk
func (h *UltraFastHistoricalLoader) processChunk(
	ctx context.Context,
	symbol string,
	interval string,
	chunk timeChunk,
) (*chunkResult, error) {
	// Fetch klines from Binance
	klines, err := h.binanceREST.GetKlinesBatch(
		ctx,
		symbol,
		interval,
		chunk.start.UnixMilli(),
		chunk.end.UnixMilli(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch klines: %w", err)
	}
	
	// Convert to bars using pool
	bars := make([]*models.Bar, 0, len(klines))
	for _, kline := range klines {
		bar := h.barPool.Get().(*models.Bar)
		if err := h.convertKlineToBar(symbol, kline, bar); err != nil {
			h.barPool.Put(bar)
			continue
		}
		bars = append(bars, bar)
	}
	
	return &chunkResult{
		bars:     bars,
		interval: interval,
		count:    len(bars),
	}, nil
}

// processResults handles writing results to database in batches
func (h *UltraFastHistoricalLoader) processResults(
	ctx context.Context,
	symbol string,
	interval string,
	resultChan <-chan *chunkResult,
	done chan<- struct{},
) {
	defer close(done)
	
	// Accumulate bars for batch writing
	batchBars := make([]*models.Bar, 0, h.writeBatchSize)
	
	for result := range resultChan {
		batchBars = append(batchBars, result.bars...)
		
		// Return bars to pool after use
		defer func(bars []*models.Bar) {
			for _, bar := range bars {
				h.barPool.Put(bar)
			}
		}(result.bars)
		
		// Write when batch is full
		if len(batchBars) >= h.writeBatchSize {
			if err := h.writeBatch(ctx, batchBars, interval); err != nil {
				h.logger.WithError(err).Error("Failed to write batch")
			}
			batchBars = batchBars[:0] // Reset slice
		}
	}
	
	// Write remaining bars
	if len(batchBars) > 0 {
		if err := h.writeBatch(ctx, batchBars, interval); err != nil {
			h.logger.WithError(err).Error("Failed to write final batch")
		}
	}
}

// writeBatch writes bars to InfluxDB in a single batch
func (h *UltraFastHistoricalLoader) writeBatch(ctx context.Context, bars []*models.Bar, interval string) error {
	if len(bars) == 0 {
		return nil
	}
	
	// Use bulk write for maximum performance
	return h.influx.WriteBars(ctx, bars, interval)
}

// reportProgress sends progress updates via WebSocket
func (h *UltraFastHistoricalLoader) reportProgress(ctx context.Context, symbol string, interval string, stop <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond) // Update every 500ms
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			current := atomic.LoadInt64(&h.currentProgress)
			total := atomic.LoadInt64(&h.totalProgress)
			
			if total > 0 {
				progress := int((float64(current) / float64(total)) * 100)
				
				// Send progress via NATS
				if h.nats != nil {
					h.nats.PublishSyncProgress(symbol, progress, int(total))
				}
				
				// Update database
				h.mysql.UpdateSyncStatus(ctx, symbol, "syncing", progress, int(total), "")
			}
			
		case <-stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// createTimeChunks divides time range into chunks for parallel processing
func (h *UltraFastHistoricalLoader) createTimeChunks(start, end time.Time, chunkDays int) []timeChunk {
	var chunks []timeChunk
	
	current := end
	for current.After(start) {
		chunkStart := current.AddDate(0, 0, -chunkDays)
		if chunkStart.Before(start) {
			chunkStart = start
		}
		
		chunks = append(chunks, timeChunk{
			start: chunkStart,
			end:   current,
		})
		
		current = chunkStart
		if current.Equal(start) {
			break
		}
	}
	
	return chunks
}

// convertKlineToBar converts exchange kline to bar model (reuses existing bar)
func (h *UltraFastHistoricalLoader) convertKlineToBar(symbol string, kline exchange.HistoricalKline, bar *models.Bar) error {
	open, err := strconv.ParseFloat(kline.Open, 64)
	if err != nil {
		return fmt.Errorf("failed to parse open: %w", err)
	}
	high, err := strconv.ParseFloat(kline.High, 64)
	if err != nil {
		return fmt.Errorf("failed to parse high: %w", err)
	}
	low, err := strconv.ParseFloat(kline.Low, 64)
	if err != nil {
		return fmt.Errorf("failed to parse low: %w", err)
	}
	close_, err := strconv.ParseFloat(kline.Close, 64)
	if err != nil {
		return fmt.Errorf("failed to parse close: %w", err)
	}
	volume, err := strconv.ParseFloat(kline.Volume, 64)
	if err != nil {
		return fmt.Errorf("failed to parse volume: %w", err)
	}
	
	bar.Symbol = symbol
	bar.Timestamp = time.Unix(kline.OpenTime/1000, (kline.OpenTime%1000)*1e6)
	bar.Open = open
	bar.High = high
	bar.Low = low
	bar.Close = close_
	bar.Volume = volume
	bar.TradeCount = int64(kline.NumberOfTrades)
	return nil
}

// LoadMultipleSymbolsConcurrent loads multiple symbols concurrently
func (h *UltraFastHistoricalLoader) LoadMultipleSymbolsConcurrent(
	ctx context.Context,
	symbols []string,
	interval string,
	days int,
) error {
	h.logger.WithFields(logrus.Fields{
		"symbols":  len(symbols),
		"interval": interval,
		"days":     days,
	}).Info("Starting concurrent multi-symbol sync")
	
	// Limit concurrent symbol processing to prevent overwhelming the system
	semaphore := make(chan struct{}, h.maxWorkers/2) // Use half the workers per symbol
	var wg sync.WaitGroup
	errors := make(chan error, len(symbols))
	
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			
			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			// Load symbol data
			if err := h.LoadHistoricalDataUltraFast(ctx, sym, interval, days); err != nil {
				errors <- fmt.Errorf("symbol %s: %w", sym, err)
			}
		}(symbol)
	}
	
	// Wait for all symbols
	wg.Wait()
	close(errors)
	
	// Collect errors
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}
	
	if len(allErrors) > 0 {
		return fmt.Errorf("failed to sync %d symbols", len(allErrors))
	}
	
	return nil
}

// Types for internal use
type timeChunk struct {
	start time.Time
	end   time.Time
}

type chunkResult struct {
	bars     []*models.Bar
	interval string
	count    int
}