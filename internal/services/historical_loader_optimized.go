package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/models"
)

// OptimizedHistoricalLoader loads historical data with optimizations
type OptimizedHistoricalLoader struct {
	*HistoricalLoader
	maxWorkers    int
	batchSize     int
	chunkDays     int
	nats          *messaging.NATSClient
}

// NewOptimizedHistoricalLoader creates a new optimized loader
func NewOptimizedHistoricalLoader(
	influx *database.InfluxClient,
	mysql *database.MySQLClient,
	nats *messaging.NATSClient,
	logger *logrus.Logger,
) *OptimizedHistoricalLoader {
	return &OptimizedHistoricalLoader{
		HistoricalLoader: NewHistoricalLoader(influx, mysql, logger),
		maxWorkers:       10,  // Process 10 symbols concurrently
		batchSize:        50,  // Process 50 symbols per batch
		chunkDays:        30,  // Load 30 days at a time
		nats:            nats,
	}
}

// LoadHistoricalDataIncremental loads data from latest to oldest
func (h *OptimizedHistoricalLoader) LoadHistoricalDataIncremental(
	ctx context.Context, 
	symbol string, 
	interval string, 
	totalDays int,
) error {
	h.logger.WithFields(logrus.Fields{
		"symbol":    symbol,
		"interval":  interval,
		"totalDays": totalDays,
	})

	endTime := time.Now()
	daysLoaded := 0
	
	// Calculate expected total klines based on interval
	// For 1m interval: 1440 klines per day (24 * 60)
	// For 5m interval: 288 klines per day (24 * 60 / 5)
	// For 1h interval: 24 klines per day
	var expectedKlinesPerDay int
	switch interval {
	case "1m":
		expectedKlinesPerDay = 1440 // 24 * 60
	case "3m":
		expectedKlinesPerDay = 480  // 24 * 60 / 3
	case "5m":
		expectedKlinesPerDay = 288  // 24 * 60 / 5
	case "15m":
		expectedKlinesPerDay = 96   // 24 * 60 / 15
	case "30m":
		expectedKlinesPerDay = 48   // 24 * 60 / 30
	case "1h":
		expectedKlinesPerDay = 24   // 24
	case "4h":
		expectedKlinesPerDay = 6    // 24 / 4
	case "1d":
		expectedKlinesPerDay = 1
	default:
		expectedKlinesPerDay = 1440 // Default to 1m
	}
	
	expectedTotalKlines := totalDays * expectedKlinesPerDay
	actualKlinesLoaded := 0
	
	// Load data in chunks from latest to oldest
	for daysLoaded < totalDays {
		chunkDays := h.chunkDays
		if daysLoaded + chunkDays > totalDays {
			chunkDays = totalDays - daysLoaded
		}
		
		startTime := endTime.AddDate(0, 0, -chunkDays)
		
		h.logger.WithFields(logrus.Fields{
			"symbol":    symbol,
			"chunk":     fmt.Sprintf("%d-%d days ago", daysLoaded, daysLoaded+chunkDays),
			"startTime": startTime.Format("2006-01-02"),
			"endTime":   endTime.Format("2006-01-02"),
		})
		
		// Fetch and store this chunk
		klines, err := h.binanceREST.GetKlinesBatch(
			ctx,
			symbol,
			interval,
			startTime.UnixMilli(),
			endTime.UnixMilli(),
		)
		if err != nil {
			errorMsg := fmt.Sprintf("Failed to fetch klines: %v", err)
			// Send error notification via WebSocket
			if h.nats != nil {
				h.nats.PublishSyncError(symbol, errorMsg)
			}
			// Update database with error status
			if h.mysql != nil {
				currentProgress := int((float64(actualKlinesLoaded) / float64(expectedTotalKlines)) * 100)
				h.mysql.UpdateSyncStatus(ctx, symbol, "failed", currentProgress, expectedTotalKlines, errorMsg)
			}
			return fmt.Errorf("failed to fetch klines for chunk: %w", err)
		}
		
		// Store klines in parallel
		if err := h.storeKlinesBatch(ctx, symbol, interval, klines); err != nil {
			h.logger.WithError(err).Warn("Failed to store some klines")
		}
		
		// Update actual klines loaded count
		actualKlinesLoaded += len(klines)
		
		// Update progress
		daysLoaded += chunkDays
		endTime = startTime
		
		// Calculate REAL progress percentage based on actual klines loaded
		// This gives accurate percentage even if some klines are missing
		progress := 0
		if expectedTotalKlines > 0 {
			progress = int((float64(actualKlinesLoaded) / float64(expectedTotalKlines)) * 100)
			// Cap at 99% until actually complete (to handle missing data)
			if progress > 99 && daysLoaded < totalDays {
				progress = 99
			}
		}
		
		h.logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"progress": fmt.Sprintf("%d%%", progress),
			"days":     fmt.Sprintf("%d/%d", daysLoaded, totalDays),
			"klines":   fmt.Sprintf("%d/%d", actualKlinesLoaded, expectedTotalKlines),
		}).Info("Sync progress")
		
		// Send progress update via NATS with REAL data
		if h.nats != nil {
			// Send actual progress and total expected klines
			if err := h.nats.PublishSyncProgress(symbol, progress, expectedTotalKlines); err != nil {
				h.logger.WithError(err).Warn("Failed to publish sync progress")
			}
		}
		
		// Also update in database for persistence
		if h.mysql != nil {
			if err := h.mysql.UpdateSyncStatus(ctx, symbol, "syncing", progress, expectedTotalKlines, ""); err != nil {
				h.logger.WithError(err).Warn("Failed to update sync status in database")
			}
		}
		
		// Small delay to avoid rate limits
		time.Sleep(100 * time.Millisecond)
	}
	
	// Send completion notification with ACTUAL data
	if h.nats != nil {
		endTime := time.Now()
		startTime := endTime.AddDate(0, 0, -totalDays)
		
		// Send actual klines loaded as totalBars
		if err := h.nats.PublishSyncComplete(symbol, actualKlinesLoaded, startTime, endTime); err != nil {
			h.logger.WithError(err).Warn("Failed to publish sync complete")
		}
		
		h.logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"totalKlinesLoaded": actualKlinesLoaded,
			"expectedKlines": expectedTotalKlines,
			"completionRate": fmt.Sprintf("%.2f%%", (float64(actualKlinesLoaded)/float64(expectedTotalKlines))*100),
		}).Info("Sync completed")
	}
	
	// Update database with final 100% progress
	if h.mysql != nil {
		if err := h.mysql.UpdateSyncStatus(ctx, symbol, "completed", 100, actualKlinesLoaded, ""); err != nil {
			h.logger.WithError(err).Warn("Failed to update sync completion in database")
		}
	}
	
	return nil
}

// LoadAllSymbolsParallel loads multiple symbols in parallel
func (h *OptimizedHistoricalLoader) LoadAllSymbolsParallel(
	ctx context.Context,
	interval string,
	days int,
) error {
	// Get all active symbols
	symbols, err := h.mysql.GetSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols: %w", err)
	}
	
	// Filter active symbols
	var activeSymbols []string
	for _, s := range symbols {
		if s.IsActive {
			activeSymbols = append(activeSymbols, s.Symbol)
		}
	}
	
	h.logger.WithField("symbols", len(activeSymbols)).Info("Loading historical data for all symbols")
	
	// Process symbols in batches
	for i := 0; i < len(activeSymbols); i += h.batchSize {
		end := i + h.batchSize
		if end > len(activeSymbols) {
			end = len(activeSymbols)
		}
		
		batch := activeSymbols[i:end]
		h.logger.WithFields(logrus.Fields{
			"batch":     fmt.Sprintf("%d-%d", i, end),
			"batchSize": len(batch),
		})
		
		if err := h.processBatch(ctx, batch, interval, days); err != nil {
			h.logger.WithError(err).Error("Batch processing failed")
		}
		
		// Delay between batches
		time.Sleep(time.Second)
	}
	
	return nil
}

// processBatch processes a batch of symbols in parallel
func (h *OptimizedHistoricalLoader) processBatch(
	ctx context.Context,
	symbols []string,
	interval string,
	days int,
) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, h.maxWorkers)
	errors := make(chan error, len(symbols))
	
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			
			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()
			
			// Load data incrementally
			if err := h.LoadHistoricalDataIncremental(ctx, sym, interval, days); err != nil {
				errors <- fmt.Errorf("symbol %s: %w", sym, err)
				h.logger.WithError(err).WithField("symbol", sym).Error("Failed to load historical data")
			}
		}(symbol)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	close(errors)
	
	// Collect errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	
	if len(errs) > 0 {
		h.logger.WithField("errors", len(errs)).Warn("Some symbols failed to load")
	}
	
	return nil
}

// storeKlinesBatch stores klines in batches for better performance
func (h *OptimizedHistoricalLoader) storeKlinesBatch(
	ctx context.Context,
	symbol string,
	interval string,
	klines []exchange.HistoricalKline,
) error {
	batchSize := 100
	for i := 0; i < len(klines); i += batchSize {
		end := i + batchSize
		if end > len(klines) {
			end = len(klines)
		}
		
		batch := klines[i:end]
		bars := make([]*models.Bar, 0, len(batch))
		
		// Convert batch
		for _, kline := range batch {
			bar, err := h.convertKlineToBar(symbol, kline)
			if err != nil {
				continue
			}
			bars = append(bars, bar)
		}
		
		// Write batch to InfluxDB
		if err := h.influx.WriteBars(ctx, bars, interval); err != nil {
			return fmt.Errorf("failed to write bars batch: %w", err)
		}
	}
	
	return nil
}

// ResumeIncompleteSync resumes any incomplete historical data syncs
func (h *OptimizedHistoricalLoader) ResumeIncompleteSync(ctx context.Context) error {
	h.logger.Info("Checking for incomplete syncs to resume...")
	
	// Get all symbols with pending or syncing status
	pendingSymbols, err := h.mysql.GetPendingSyncSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending symbols: %w", err)
	}
	
	if len(pendingSymbols) == 0 {
		h.logger.Info("No incomplete syncs found")
		return nil
	}
	
	h.logger.WithField("count", len(pendingSymbols)).Info("Found incomplete syncs to resume")
	
	// Process each pending symbol
	symbolsList := make([]string, 0, len(pendingSymbols))
	for _, symbolInfo := range pendingSymbols {
		symbol, ok := symbolInfo["symbol"].(string)
		if ok {
			symbolsList = append(symbolsList, symbol)
		}
	}
	
	// Resume sync for all pending symbols in parallel
	if len(symbolsList) > 0 {
		// Default to 1000 days and 1m interval
		return h.processBatch(ctx, symbolsList, "1m", 1000)
	}
	
	return nil
}

// LoadMarketWatchSymbolsParallel loads market watch symbols in parallel
func (h *OptimizedHistoricalLoader) LoadMarketWatchSymbolsParallel(
	ctx context.Context,
	sessionToken string,
	interval string,
	days int,
) error {
	// Get market watch symbols
	symbols, err := h.mysql.GetMarketWatchSymbols(ctx, sessionToken)
	if err != nil {
		return fmt.Errorf("failed to get market watch symbols: %w", err)
	}
	
	if len(symbols) == 0 {
		h.logger.Warn("No symbols in market watch")
		return nil
	}
	
	h.logger.WithFields(logrus.Fields{
		"session": sessionToken,
		"symbols": len(symbols),
	})
	
	// Process all symbols in parallel (market watch is usually small)
	return h.processBatch(ctx, symbols, interval, days)
}