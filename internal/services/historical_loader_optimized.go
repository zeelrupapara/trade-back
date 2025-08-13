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
	"github.com/trade-back/pkg/config"
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

// isForexSymbol checks if a symbol is a forex pair (contains underscore)
func (h *OptimizedHistoricalLoader) isForexSymbol(symbol string) bool {
	// OANDA forex symbols use underscore format like EUR_USD
	return len(symbol) > 0 && (symbol[3:4] == "_" || (len(symbol) > 4 && symbol[4:5] == "_"))
}

// NewOptimizedHistoricalLoader creates a new optimized loader
func NewOptimizedHistoricalLoader(
	influx *database.InfluxClient,
	mysql *database.MySQLClient,
	nats *messaging.NATSClient,
	config *config.Config,
	logger *logrus.Logger,
) *OptimizedHistoricalLoader {
	return &OptimizedHistoricalLoader{
		HistoricalLoader: NewHistoricalLoader(influx, mysql, config, logger),
		maxWorkers:       10,  // Process 10 symbols concurrently
		batchSize:        50,  // Process 50 symbols per batch
		chunkDays:        30,  // Load 30 days at a time
		nats:            nats,
	}
}

// LoadHistoricalDataIncremental loads data from latest to oldest (with smart sync)
func (h *OptimizedHistoricalLoader) LoadHistoricalDataIncremental(
	ctx context.Context, 
	symbol string, 
	interval string, 
	totalDays int,
) error {
	// FOREX symbols now support historical sync via OANDA REST API
	// No longer skip them
	
	h.logger.WithFields(logrus.Fields{
		"symbol":    symbol,
		"interval":  interval,
		"totalDays": totalDays,
	}).Info("Starting smart historical sync")

	// SMART SYNC: Check existing data first
	existingEarliest, existingLatest, existingCount, err := h.influx.GetDataTimeRange(ctx, symbol)
	if err != nil {
		h.logger.WithError(err).Warn("Failed to check existing data, proceeding with full sync")
	}
	
	endTime := time.Now()
	targetStartTime := endTime.AddDate(0, 0, -totalDays)
	daysLoaded := 0
	existingDataDays := 0
	
	// Check if we already have sufficient data
	if !existingEarliest.IsZero() && !existingLatest.IsZero() {
		existingDataDays = int(existingLatest.Sub(existingEarliest).Hours() / 24)
		
		h.logger.WithFields(logrus.Fields{
			"symbol":           symbol,
			"existingEarliest": existingEarliest.Format("2006-01-02"),
			"existingLatest":   existingLatest.Format("2006-01-02"),
			"existingDays":     existingDataDays,
			"existingCount":    existingCount,
		}).Info("Found existing data in InfluxDB")
		
		// If we already have data from the target period
		if existingEarliest.Before(targetStartTime) || existingEarliest.Equal(targetStartTime) {
			// Check if we need to fill recent gap
			gapDays := int(time.Since(existingLatest).Hours() / 24)
			
			if gapDays <= 1 {
				h.logger.WithFields(logrus.Fields{
					"symbol": symbol,
					"message": "Data is up to date, skipping sync",
				}).Info("Smart sync: No sync needed")
				
				// Send 100% completion
				if h.nats != nil {
					h.nats.PublishSyncComplete(symbol, int(existingCount), existingEarliest, existingLatest)
				}
				if h.mysql != nil {
					h.mysql.UpdateSyncStatus(ctx, symbol, "completed", 100, int(existingCount), "")
				}
				return nil
			}
			
			// Only sync the gap
			h.logger.WithFields(logrus.Fields{
				"symbol": symbol,
				"gapDays": gapDays,
				"message": "Only syncing recent gap",
			}).Info("Smart sync: Filling gap")
			
			endTime = time.Now()
			targetStartTime = existingLatest
			totalDays = gapDays
		} else {
			// We need older data, adjust the sync period
			if !existingEarliest.IsZero() {
				// Calculate how many more days we need before existing data
				additionalDaysNeeded := int(existingEarliest.Sub(targetStartTime).Hours() / 24)
				
				h.logger.WithFields(logrus.Fields{
					"symbol": symbol,
					"additionalDaysNeeded": additionalDaysNeeded,
					"message": "Syncing older data to extend coverage",
				}).Info("Smart sync: Extending historical range")
				
				// Sync the older data
				endTime = existingEarliest
				totalDays = additionalDaysNeeded
			}
		}
	} else {
		h.logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"message": "No existing data found, performing full sync",
		}).Info("Smart sync: Full sync required")
	}
	
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
	
	// Adjust expected klines based on what we're actually syncing
	expectedNewKlines := totalDays * expectedKlinesPerDay
	actualKlinesLoaded := 0
	
	// If we have existing data, include it in progress calculation
	totalExpectedKlines := expectedNewKlines
	baseProgressOffset := 0
	
	if existingCount > 0 {
		// Calculate total expected including existing
		originalTotalDays := int(time.Since(targetStartTime).Hours() / 24)
		totalExpectedKlines = originalTotalDays * expectedKlinesPerDay
		
		// Calculate base progress from existing data
		baseProgressOffset = int((float64(existingCount) / float64(totalExpectedKlines)) * 100)
		
		h.logger.WithFields(logrus.Fields{
			"existingCount": existingCount,
			"expectedNew": expectedNewKlines,
			"totalExpected": totalExpectedKlines,
			"baseProgress": baseProgressOffset,
		}).Info("Smart sync: Calculated progress offsets")
	}
	
	// Load data in chunks from latest to oldest
	for daysLoaded < totalDays {
		chunkDays := h.chunkDays
		
		// For OANDA/FOREX symbols with 1m interval, use smaller chunks due to API limits
		if h.isForexSymbol(symbol) && interval == "1m" {
			// OANDA has a max of 5000 candles per request
			// For 1m interval: 5000 / (24*60) = ~3.47 days
			chunkDays = 3 // Safe limit for 1-minute candles
		} else if h.isForexSymbol(symbol) {
			// For other intervals, can use larger chunks
			chunkDays = 10 // Conservative for other intervals
		}
		
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
		
		// Determine which exchange API to use based on symbol
		var klines []exchange.HistoricalKline
		var err error
		
		// Check if symbol is from OANDA (forex symbols with underscore)
		if h.oandaREST != nil && h.isForexSymbol(symbol) {
			// For OANDA, we need to use their API
			klines, err = h.oandaREST.GetKlinesBatch(
				ctx,
				symbol,
				interval,
				startTime.UnixMilli(),
				endTime.UnixMilli(),
			)
		} else {
			// Default to Binance for crypto symbols
			klines, err = h.binanceREST.GetKlinesBatch(
				ctx,
				symbol,
				interval,
				startTime.UnixMilli(),
				endTime.UnixMilli(),
			)
		}
		
		if err != nil {
			errorMsg := fmt.Sprintf("Failed to fetch klines: %v", err)
			// Send error notification via WebSocket
			if h.nats != nil {
				h.nats.PublishSyncError(symbol, errorMsg)
			}
			// Update database with error status
			if h.mysql != nil {
				currentProgress := int((float64(actualKlinesLoaded) / float64(totalExpectedKlines)) * 100)
				h.mysql.UpdateSyncStatus(ctx, symbol, "failed", currentProgress, totalExpectedKlines, errorMsg)
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
		
		// Calculate REAL progress percentage including existing data
		progress := baseProgressOffset
		if expectedNewKlines > 0 {
			// Add progress from new data to base
			newDataProgress := int((float64(actualKlinesLoaded) / float64(expectedNewKlines)) * float64(100 - baseProgressOffset))
			progress = baseProgressOffset + newDataProgress
			
			// Cap at 99% until actually complete
			if progress > 99 && daysLoaded < totalDays {
				progress = 99
			}
		}
		
		h.logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"progress": fmt.Sprintf("%d%%", progress),
			"days":     fmt.Sprintf("%d/%d", daysLoaded, totalDays),
			"klines":   fmt.Sprintf("%d/%d", actualKlinesLoaded, totalExpectedKlines),
		}).Info("Sync progress")
		
		// Send progress update via NATS with REAL data
		if h.nats != nil {
			// Send actual progress and total expected klines
			if err := h.nats.PublishSyncProgress(symbol, progress, totalExpectedKlines); err != nil {
				h.logger.WithError(err).Warn("Failed to publish sync progress")
			}
		}
		
		// Also update in database for persistence
		if h.mysql != nil {
			if err := h.mysql.UpdateSyncStatus(ctx, symbol, "syncing", progress, totalExpectedKlines, ""); err != nil {
				h.logger.WithError(err).Warn("Failed to update sync status in database")
			}
		}
		
		// Small delay to avoid rate limits
		time.Sleep(100 * time.Millisecond)
	}
	
	// Calculate final totals including existing data
	finalTotalBars := actualKlinesLoaded
	if existingCount > 0 {
		finalTotalBars = int(existingCount) + actualKlinesLoaded
	}
	
	// Send completion notification with ACTUAL data
	if h.nats != nil {
		// Get actual date range
		finalEarliest, finalLatest, _, _ := h.influx.GetDataTimeRange(ctx, symbol)
		if finalEarliest.IsZero() {
			finalEarliest = time.Now().AddDate(0, 0, -totalDays)
		}
		if finalLatest.IsZero() {
			finalLatest = time.Now()
		}
		
		// Send total bars including existing
		if err := h.nats.PublishSyncComplete(symbol, finalTotalBars, finalEarliest, finalLatest); err != nil {
			h.logger.WithError(err).Warn("Failed to publish sync complete")
		}
		
		h.logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"newKlinesLoaded": actualKlinesLoaded,
			"existingKlines": existingCount,
			"totalKlines": finalTotalBars,
			"smartSync": existingCount > 0,
		}).Info("Smart sync completed")
	}
	
	// Update database with final 100% progress
	if h.mysql != nil {
		if err := h.mysql.UpdateSyncStatus(ctx, symbol, "completed", 100, finalTotalBars, ""); err != nil {
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