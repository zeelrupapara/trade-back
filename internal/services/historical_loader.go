package services

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/exchange"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// HistoricalLoader loads historical market data
type HistoricalLoader struct {
	binanceREST *exchange.BinanceRESTClient
	oandaREST   *exchange.OANDARESTClient
	influx      *database.InfluxClient
	mysql       *database.MySQLClient
	logger      *logrus.Entry
	config      *config.Config
	
	// Configuration
	maxConcurrent int
	batchDelay    time.Duration
}

// NewHistoricalLoader creates a new historical data loader
func NewHistoricalLoader(
	influx *database.InfluxClient,
	mysql *database.MySQLClient,
	config *config.Config,
	logger *logrus.Logger,
) *HistoricalLoader {
	var oandaREST *exchange.OANDARESTClient
	if config.Exchange.OANDA.APIKey != "" && config.Exchange.OANDA.AccountID != "" {
		oandaREST = exchange.NewOANDARESTClient(&config.Exchange.OANDA, logger)
	}

	return &HistoricalLoader{
		binanceREST:   exchange.NewBinanceRESTClient(logger),
		oandaREST:     oandaREST,
		influx:        influx,
		mysql:         mysql,
		config:        config,
		logger:        logger.WithField("component", "historical-loader"),
		maxConcurrent: 3, // Process 3 symbols concurrently
		batchDelay:    time.Second, // Delay between batches
	}
}

// LoadHistoricalDataWithCheckpoint loads historical data with checkpoint support
func (h *HistoricalLoader) LoadHistoricalDataWithCheckpoint(ctx context.Context, symbol string, interval string, startTime, endTime time.Time) error {
	// Update sync status to syncing
	if err := h.mysql.UpdateSyncStatus(ctx, symbol, "syncing", 0, 0, ""); err != nil {
		h.logger.WithError(err).Warn("Failed to update sync status")
	}

	// Determine exchange for symbol
	symbolInfo, err := h.mysql.GetSymbolByName(ctx, symbol)
	if err != nil {
		h.mysql.UpdateSyncStatus(ctx, symbol, "failed", 0, 0, "Symbol not found in database")
		return fmt.Errorf("failed to get symbol info: %w", err)
	}

	// Fetch klines from appropriate exchange
	var klines []exchange.HistoricalKline
	
	switch symbolInfo.Exchange {
	case "oanda":
		if h.oandaREST == nil {
			h.mysql.UpdateSyncStatus(ctx, symbol, "failed", 0, 0, "OANDA not configured")
			return fmt.Errorf("OANDA REST client not available")
		}
		klines, err = h.oandaREST.GetKlinesBatch(
			ctx,
			symbol,
			interval,
			startTime.UnixMilli(),
			endTime.UnixMilli(),
		)
		
	default: // binance or unknown (default to binance)
		klines, err = h.binanceREST.GetKlinesBatch(
			ctx,
			symbol,
			interval,
			startTime.UnixMilli(),
			endTime.UnixMilli(),
		)
	}
	
	if err != nil {
		h.mysql.UpdateSyncStatus(ctx, symbol, "failed", 0, 0, err.Error())
		return fmt.Errorf("failed to fetch klines from %s: %w", symbolInfo.Exchange, err)
	}

	totalBars := len(klines)

	// Convert and store klines with checkpoint updates
	for i, kline := range klines {
		bar, err := h.convertKlineToBar(symbol, kline)
		if err != nil {
			h.logger.WithError(err).Warn("Failed to convert kline")
			continue
		}

		// Store in InfluxDB
		if err := h.influx.WriteBar(ctx, bar, interval); err != nil {
			h.logger.WithError(err).Error("Failed to write bar to InfluxDB")
			// Continue with other bars
		}

		// Update checkpoint every 1000 bars
		if i > 0 && i%1000 == 0 {
			progress := int((float64(i) / float64(totalBars)) * 100)
			if err := h.mysql.UpdateSyncStatus(ctx, symbol, "syncing", progress, totalBars, ""); err != nil {
				h.logger.WithError(err).Warn("Failed to update sync checkpoint")
			}
		}
	}

	// Mark as completed
	if err := h.mysql.UpdateSyncStatus(ctx, symbol, "completed", 100, totalBars, ""); err != nil {
		h.logger.WithError(err).Warn("Failed to update sync status to completed")
	}

	h.logger.WithFields(logrus.Fields{
		"symbol": symbol,
		"stored": totalBars,
	}).Info("Historical data loaded successfully")

	return nil
}

// isForexSymbol checks if a symbol is a forex pair (contains underscore)
func (h *HistoricalLoader) isForexSymbol(symbol string) bool {
	// OANDA forex symbols use underscore format like EUR_USD
	return len(symbol) > 0 && (symbol[3:4] == "_" || (len(symbol) > 4 && symbol[4:5] == "_"))
}

// LoadHistoricalData loads historical data for a single symbol
func (h *HistoricalLoader) LoadHistoricalData(ctx context.Context, symbol string, interval string, days int) error {
	// Skip FOREX symbols from historical sync (they use real-time streaming)
	if h.isForexSymbol(symbol) {
		h.logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"reason": "FOREX symbols use real-time streaming only",
		}).Debug("Skipping historical sync for FOREX symbol")
		
		// Mark as completed immediately
		if h.mysql != nil {
			h.mysql.UpdateSyncStatus(ctx, symbol, "completed", 100, 0, "FOREX - streaming only")
		}
		return nil
	}
	// h.logger.WithFields(logrus.Fields{
	// 	"symbol":   symbol,
	// 	"interval": interval,
	// 	"days":     days,
	// }).Info("Loading historical data")

	// Calculate time range
	endTime := time.Now()
	startTime := endTime.AddDate(0, 0, -days)

	// Fetch klines from Binance
	klines, err := h.binanceREST.GetKlinesBatch(
		ctx,
		symbol,
		interval,
		startTime.UnixMilli(),
		endTime.UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("failed to fetch klines: %w", err)
	}

	// h.logger.WithFields(logrus.Fields{
	// 	"symbol": symbol,
	// 	"count":  len(klines),
	// }).Info("Fetched klines")

	// Convert and store klines
	for i, kline := range klines {
		bar, err := h.convertKlineToBar(symbol, kline)
		if err != nil {
			h.logger.WithError(err).Warn("Failed to convert kline")
			continue
		}

		// Store in InfluxDB
		if err := h.influx.WriteBar(ctx, bar, interval); err != nil {
			h.logger.WithError(err).Error("Failed to write bar to InfluxDB")
			// Continue with other bars
		}

		// Progress update every 1000 bars
		if i > 0 && i%1000 == 0 {
			// h.logger.WithFields(logrus.Fields{
			// 	"symbol":   symbol,
			// 	"progress": fmt.Sprintf("%d/%d", i, len(klines)),
			// }).Info("Loading progress")
		}
	}

	// h.logger.WithFields(logrus.Fields{
	// 	"symbol": symbol,
	// 	"stored": len(klines),
	// }).Info("Historical data loaded successfully")

	return h.LoadHistoricalDataWithCheckpoint(ctx, symbol, interval, startTime, endTime)
}

// ResumeIncompleteSync resumes any incomplete historical data syncs
func (h *HistoricalLoader) ResumeIncompleteSync(ctx context.Context) error {
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
	for _, symbolInfo := range pendingSymbols {
		symbol, ok := symbolInfo["symbol"].(string)
		if !ok {
			continue
		}
		
		// Get sync status to determine where to resume from
		syncStatus, err := h.mysql.GetSyncStatus(ctx, symbol)
		if err != nil {
			h.logger.WithError(err).WithField("symbol", symbol).Warn("Failed to get sync status")
			continue
		}
		
		// Default to 1000 days if not specified
		days := 1000
		interval := "1m" // Default interval
		
		h.logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"status":   syncStatus.Status,
			"progress": syncStatus.Progress,
		}).Info("Resuming sync for symbol")
		
		// Resume the sync
		go func(sym string, intv string, d int) {
			if err := h.LoadHistoricalData(context.Background(), sym, intv, d); err != nil {
				h.logger.WithError(err).WithField("symbol", sym).Error("Failed to resume sync")
			}
		}(symbol, interval, days)
		
		// Small delay between resuming different symbols
		time.Sleep(2 * time.Second)
	}
	
	return nil
}

// LoadAllSymbols loads historical data for all active symbols
func (h *HistoricalLoader) LoadAllSymbols(ctx context.Context, interval string, days int) error {
	// Get active symbols from database
	symbols, err := h.mysql.GetSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols: %w", err)
	}

	activeSymbols := make([]string, 0)
	for _, sym := range symbols {
		if sym.IsActive {
			activeSymbols = append(activeSymbols, sym.Symbol)
		}
	}

	h.logger.WithField("symbols", len(activeSymbols)).Info("Loading historical data for all symbols")

	// Process symbols with concurrency limit
	sem := make(chan struct{}, h.maxConcurrent)
	var wg sync.WaitGroup
	errors := make(chan error, len(activeSymbols))

	for _, symbol := range activeSymbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			
			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Load data for symbol
			if err := h.LoadHistoricalData(ctx, sym, interval, days); err != nil {
				errors <- fmt.Errorf("symbol %s: %w", sym, err)
			}
			
			// Small delay between symbols
			time.Sleep(h.batchDelay)
		}(symbol)
	}

	// Wait for all goroutines
	wg.Wait()
	close(errors)

	// Collect errors
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}

	if len(allErrors) > 0 {
		h.logger.WithField("errors", len(allErrors)).Error("Some symbols failed to load")
		return fmt.Errorf("failed to load %d symbols", len(allErrors))
	}

	return nil
}

// LoadATHATL loads all-time high and low for symbols
func (h *HistoricalLoader) LoadATHATL(ctx context.Context) error {
	// h.logger.Info("Loading ATH/ATL for all symbols")

	// First, ensure we have sufficient daily data
	if err := h.LoadAllSymbols(ctx, "1d", 730); err != nil { // 2 years
		h.logger.WithError(err).Warn("Failed to load 2 years of daily data")
	}

	// Get active symbols
	symbols, err := h.mysql.GetSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbols: %w", err)
	}

	// Calculate and store ATH/ATL for each symbol
	for _, sym := range symbols {
		if !sym.IsActive {
			continue
		}

		ath, atl, err := h.influx.GetATHATL(ctx, sym.Symbol)
		if err != nil {
			h.logger.WithError(err).WithField("symbol", sym.Symbol).Warn("Failed to get ATH/ATL")
			continue
		}

		h.logger.WithFields(logrus.Fields{
			"symbol": sym.Symbol,
			"ath":    ath,
			"atl":    atl,
		})
	}

	return nil
}

// FillGaps fills gaps in historical data
func (h *HistoricalLoader) FillGaps(ctx context.Context, symbol string, interval string) error {
	// This would check for gaps in the data and fill them
	// Implementation depends on specific requirements
	h.logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
	})

	// TODO: Implement gap detection and filling
	return nil
}

// convertKlineToBar converts Binance kline data to our Bar model
func (h *HistoricalLoader) convertKlineToBar(symbol string, kline exchange.HistoricalKline) (*models.Bar, error) {
	open, err := strconv.ParseFloat(kline.Open, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse open: %w", err)
	}

	high, err := strconv.ParseFloat(kline.High, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse high: %w", err)
	}

	low, err := strconv.ParseFloat(kline.Low, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse low: %w", err)
	}

	close_, err := strconv.ParseFloat(kline.Close, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse close: %w", err)
	}

	volume, err := strconv.ParseFloat(kline.Volume, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse volume: %w", err)
	}

	return &models.Bar{
		Symbol:     symbol,
		Timestamp:  time.Unix(kline.OpenTime/1000, (kline.OpenTime%1000)*1e6),
		Open:       open,
		High:       high,
		Low:        low,
		Close:      close_,
		Volume:     volume,
		TradeCount: int64(kline.NumberOfTrades),
	}, nil
}