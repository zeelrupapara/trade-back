package exchange

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// Hub is the central orchestrator for all price data
type Hub struct {
	// Core components
	pool         *ConnectionPool
	nats         *messaging.NATSClient
	influx       *database.InfluxClient
	mysql        *database.MySQLClient
	redis        *cache.RedisClient
	influxBatcher *InfluxBatcher
	processor    *PriceProcessor
	dynamicSymbols *DynamicSymbolManager
	binanceREST  *BinanceRESTClient
	
	// Configuration
	cfg          *config.Config
	logger       *logrus.Entry
	
	// State
	running      atomic.Bool
	symbols      []string
	
	// Price handlers
	priceHandlers []PriceHandler
	
	// Statistics
	stats        *HubStats
	
	// Error rate limiting
	lastErrorLog  time.Time
	errorLogMu    sync.Mutex
	
	// Gap filling workers
	gapWorkers   chan struct{}
	
	// Synchronization
	mu           sync.RWMutex
	wg           sync.WaitGroup
	done         chan struct{}
}

// PriceHandler is a function that handles price updates
type PriceHandler func(*models.PriceData) error

// HubStats contains hub statistics
type HubStats struct {
	MessagesProcessed  atomic.Uint64
	MessagesPerSecond  atomic.Uint64
	ErrorCount         atomic.Uint64
	LastUpdateTime     atomic.Int64
	ConnectionStatus   map[string]bool
	BufferStats        BufferStats
}

// NewHub creates a new price hub
func NewHub(cfg *config.Config, logger *logrus.Logger) *Hub {
	return &Hub{
		cfg:        cfg,
		logger:     logger.WithField("component", "hub"),
		stats:      &HubStats{},
		done:       make(chan struct{}),
		gapWorkers: make(chan struct{}, 5), // Max 5 concurrent gap filling operations
	}
}

// Initialize initializes the hub with all dependencies
func (h *Hub) Initialize(
	nats *messaging.NATSClient,
	influx *database.InfluxClient,
	mysql *database.MySQLClient,
	redis *cache.RedisClient,
) error {
	h.nats = nats
	h.influx = influx
	h.mysql = mysql
	h.redis = redis
	
	// Create dynamic symbol manager
	h.dynamicSymbols = NewDynamicSymbolManager(h, redis, h.logger.Logger)
	
	// Get initial market watch symbols
	initialSymbols, err := h.dynamicSymbols.getAllMarketWatchSymbols(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get market watch symbols: %w", err)
	}
	
	// Convert map to slice
	symbols := make([]string, 0, len(initialSymbols))
	for symbol := range initialSymbols {
		symbols = append(symbols, symbol)
	}
	
	// If no market watch symbols, start with empty list
	if len(symbols) == 0 {
		// h.logger.Info("No market watch symbols found, starting with empty subscription")
		symbols = []string{} // Empty list
	}
	
	h.symbols = symbols
	// h.logger.WithField("symbols", len(symbols)).Info("Loaded market watch symbols")
	
	// Create InfluxDB batcher
	h.influxBatcher = NewInfluxBatcher(h.influx, h.logger.Logger)
	
	// Create price processor with 4 workers
	h.processor = NewPriceProcessor(h, 4)
	
	// Create Binance REST client
	h.binanceREST = NewBinanceRESTClient(h.logger.Logger)
	
	// Create connection pool
	h.pool = NewConnectionPool(symbols, &h.cfg.Exchange, h.logger.Logger)
	h.pool.SetPriceHandler(h.processor.ProcessPrice)
	
	// Register default handlers
	h.RegisterHandler(h.publishToNATS)
	h.RegisterHandler(h.storeToInfluxBatched)
	h.RegisterHandler(h.updateCache)
	
	return nil
}

// Start starts the hub
func (h *Hub) Start(ctx context.Context) error {
	if h.running.Load() {
		return fmt.Errorf("hub already running")
	}
	
	// h.logger.Info("Starting price hub...")
	
	// Start InfluxDB batcher
	h.influxBatcher.Start()
	
	// Start price processor
	h.processor.Start()
	
	// Start connection pool
	if err := h.pool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start connection pool: %w", err)
	}
	
	h.running.Store(true)
	
	// Start dynamic symbol manager
	if err := h.dynamicSymbols.Start(ctx); err != nil {
		return fmt.Errorf("failed to start dynamic symbol manager: %w", err)
	}
	
	// Start statistics updater
	h.wg.Add(1)
	go h.updateStatistics(ctx)
	
	// Start gap monitor
	h.wg.Add(1)
	go h.monitorGaps(ctx)
	
	// Start health checker
	h.wg.Add(1)
	go h.healthChecker(ctx)
	
	// h.logger.Info("Price hub started successfully")
	return nil
}

// Stop stops the hub
func (h *Hub) Stop() error {
	if !h.running.Load() {
		return nil
	}
	
	// h.logger.Info("Stopping price hub...")
	
	close(h.done)
	h.running.Store(false)
	
	// Stop dynamic symbol manager
	h.dynamicSymbols.Stop()
	
	// Stop connection pool
	if err := h.pool.Stop(); err != nil {
		h.logger.WithError(err).Error("Failed to stop connection pool")
	}
	
	// Stop price processor
	h.processor.Stop()
	
	// Stop InfluxDB batcher
	h.influxBatcher.Stop()
	
	// Wait for goroutines
	h.wg.Wait()
	
	// h.logger.Info("Price hub stopped")
	return nil
}

// RegisterHandler registers a price handler
func (h *Hub) RegisterHandler(handler PriceHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.priceHandlers = append(h.priceHandlers, handler)
}

// GetStats returns hub statistics
func (h *Hub) GetStats() HubStats {
	stats := HubStats{
		MessagesProcessed: atomic.Uint64{},
		MessagesPerSecond: atomic.Uint64{},
		ErrorCount:        atomic.Uint64{},
		LastUpdateTime:    atomic.Int64{},
	}
	
	stats.MessagesProcessed.Store(h.stats.MessagesProcessed.Load())
	stats.MessagesPerSecond.Store(h.stats.MessagesPerSecond.Load())
	stats.ErrorCount.Store(h.stats.ErrorCount.Load())
	stats.LastUpdateTime.Store(h.stats.LastUpdateTime.Load())
	stats.ConnectionStatus = h.pool.GetConnectionStatus()
	stats.BufferStats = h.pool.GetBuffer().Stats()
	
	return stats
}

// GetBuffer returns the circular buffer
func (h *Hub) GetBuffer() *CircularBuffer {
	return h.pool.GetBuffer()
}

// handlePriceUpdate handles price updates from the connection pool
func (h *Hub) handlePriceUpdate(price *models.PriceData) {
	// Update statistics
	h.stats.MessagesProcessed.Add(1)
	h.stats.LastUpdateTime.Store(time.Now().Unix())
	
	// Process synchronously to avoid goroutine explosion
	h.mu.RLock()
	handlers := make([]PriceHandler, len(h.priceHandlers))
	copy(handlers, h.priceHandlers)
	h.mu.RUnlock()
	
	// Execute handlers sequentially with timeout
	for _, handler := range handlers {
		// Create a context with timeout for each handler
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		
		// Run handler in a goroutine but wait for it
		done := make(chan error, 1)
		go func() {
			done <- handler(price)
		}()
		
		// Wait for handler or timeout
		select {
		case err := <-done:
			if err != nil {
				h.stats.ErrorCount.Add(1)
				h.logErrorRateLimited(err, "Price handler error")
			}
		case <-ctx.Done():
			h.logErrorRateLimited(fmt.Errorf("handler timeout for %s", price.Symbol), "Price handler timeout")
		}
		
		cancel()
	}
}

// handlePriceUpdateDirect handles price updates directly without creating goroutines
func (h *Hub) handlePriceUpdateDirect(price *models.PriceData) {
	// Update statistics
	h.stats.MessagesProcessed.Add(1)
	h.stats.LastUpdateTime.Store(time.Now().Unix())
	
	// Get handlers
	h.mu.RLock()
	handlers := h.priceHandlers
	h.mu.RUnlock()
	
	// Execute handlers directly
	for _, handler := range handlers {
		if err := handler(price); err != nil {
			h.stats.ErrorCount.Add(1)
			// Don't log every error to avoid log spam
		}
	}
}

// publishToNATS publishes price updates to NATS
func (h *Hub) publishToNATS(price *models.PriceData) error {
	subject := fmt.Sprintf("prices.%s", price.Symbol)
	return h.nats.PublishPrice(subject, price)
}

// storeToInflux stores price data to InfluxDB (deprecated - use storeToInfluxBatched)
func (h *Hub) storeToInflux(price *models.PriceData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return h.influx.WritePriceData(ctx, price)
}

// storeToInfluxBatched stores price data to InfluxDB using batching
func (h *Hub) storeToInfluxBatched(price *models.PriceData) error {
	return h.influxBatcher.Write(price)
}

// updateCache updates Redis cache
func (h *Hub) updateCache(price *models.PriceData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return h.redis.SetPrice(ctx, price.Symbol, price)
}

// loadActiveSymbols loads active symbols from database
func (h *Hub) loadActiveSymbols(ctx context.Context) ([]string, error) {
	symbolInfos, err := h.mysql.GetSymbols(ctx)
	if err != nil {
		return nil, err
	}
	
	symbols := make([]string, 0, len(symbolInfos))
	for _, info := range symbolInfos {
		if info.IsActive {
			symbols = append(symbols, info.Symbol)
		}
	}
	
	return symbols, nil
}

// updateStatistics updates hub statistics
func (h *Hub) updateStatistics(ctx context.Context) {
	defer h.wg.Done()
	
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	var lastCount uint64
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case <-ticker.C:
			currentCount := h.stats.MessagesProcessed.Load()
			messagesPerSecond := currentCount - lastCount
			h.stats.MessagesPerSecond.Store(messagesPerSecond)
			lastCount = currentCount
			
		}
	}
}

// monitorGaps monitors for sequence gaps
func (h *Hub) monitorGaps(ctx context.Context) {
	defer h.wg.Done()
	
	if !h.cfg.Features.EnigmaEnabled {
		return
	}
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case <-ticker.C:
			buffer := h.pool.GetBuffer()
			
			// Check gaps for each symbol
			for _, symbol := range buffer.GetSymbols() {
				gaps := buffer.FindGaps(symbol)
				
				for _, gap := range gaps {
					// Commented out - too verbose, generates thousands of logs
					// h.logger.WithFields(logrus.Fields{
					// 	"symbol":    gap.Symbol,
					// 	"gap_size":  gap.GapSize,
					// 	"from_seq":  gap.FromSeq,
					// 	"to_seq":    gap.ToSeq,
					// 	"duration":  gap.ToTime.Sub(gap.FromTime),
					// }).Warn("Sequence gap detected")
					
					// Implement gap filling logic with bounded concurrency
					select {
					case h.gapWorkers <- struct{}{}:
						go func(g SequenceGap) {
							defer func() { <-h.gapWorkers }()
							h.fillGap(g)
						}(gap)
					default:
						// Commented out - too verbose
						// h.logger.WithFields(logrus.Fields{
						// 	"symbol": gap.Symbol,
						// 	"gap_size": gap.GapSize,
						// }).Warn("Gap worker pool exhausted, skipping gap")
					}
				}
			}
		}
	}
}

// healthChecker monitors component health
func (h *Hub) healthChecker(ctx context.Context) {
	defer h.wg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case <-ticker.C:
			// Check MySQL
			if err := h.mysql.Health(ctx); err != nil {
				h.logger.WithError(err).Error("MySQL health check failed")
			}
			
			// Check InfluxDB
			if err := h.influx.Health(ctx); err != nil {
				h.logger.WithError(err).Error("InfluxDB health check failed")
			}
			
			// Check Redis
			if err := h.redis.Health(ctx); err != nil {
				h.logger.WithError(err).Error("Redis health check failed")
			}
			
			// Check NATS
			if !h.nats.IsConnected() {
				h.logger.Error("NATS connection lost")
			}
			
			// Check connection pool
			connStatus := h.pool.GetConnectionStatus()
			activeConnections := 0
			for _, connected := range connStatus {
				if connected {
					activeConnections++
				}
			}
			
			if activeConnections == 0 {
				h.logger.Error("No active WebSocket connections")
			}
			
		}
	}
}

// AddSymbol adds a new symbol to monitor
func (h *Hub) AddSymbol(symbol string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Check if already monitoring
	for _, s := range h.symbols {
		if s == symbol {
			return nil
		}
	}
	
	// Add to symbols list
	h.symbols = append(h.symbols, symbol)
	
	// TODO: Rebalance connections if needed
	
	return nil
}

// RemoveSymbol removes a symbol from monitoring
func (h *Hub) RemoveSymbol(symbol string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	// Find and remove symbol
	for i, s := range h.symbols {
		if s == symbol {
			h.symbols = append(h.symbols[:i], h.symbols[i+1:]...)
			break
		}
	}
	
	// TODO: Rebalance connections if needed
	
	return nil
}

// fillGap fills a sequence gap by fetching missing data
func (h *Hub) fillGap(gap SequenceGap) {
	// h.logger.WithFields(logrus.Fields{
	// 	"symbol":   gap.Symbol,
	// 	"gap_size": gap.GapSize,
	// 	"from_time": gap.FromTime,
	// 	"to_time":   gap.ToTime,
	// }).Info("Filling sequence gap")
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Calculate the time range for the gap
	startTime := gap.FromTime.UnixMilli()
	endTime := gap.ToTime.UnixMilli()
	
	// Fetch historical klines from Binance to fill the gap (1m interval)
	klines, err := h.binanceREST.GetKlinesBatch(ctx, gap.Symbol, "1m", startTime, endTime)
	if err != nil {
		h.logger.WithError(err).WithField("symbol", gap.Symbol).Error("Failed to fetch historical data for gap")
		return
	}
	
	// Convert klines to price data and store
	priceCount := 0
	for _, kline := range klines {
		// Parse close price
		closePrice, err := strconv.ParseFloat(kline.Close, 64)
		if err != nil {
			h.logger.WithError(err).Error("Failed to parse close price")
			continue
		}
		
		// Parse volume
		volume, err := strconv.ParseFloat(kline.Volume, 64)
		if err != nil {
			h.logger.WithError(err).Error("Failed to parse volume")
			continue
		}
		
		price := &models.PriceData{
			Symbol:    gap.Symbol,
			Price:     closePrice,
			Timestamp: time.Unix(kline.OpenTime/1000, 0),
			Volume:    volume,
			Bid:       closePrice, // Approximate with close price
			Ask:       closePrice, // Approximate with close price
		}
		
		// Store to InfluxDB
		if err := h.storeToInfluxBatched(price); err != nil {
			h.logger.WithError(err).WithField("symbol", gap.Symbol).Error("Failed to store gap fill price")
			continue
		}
		
		// Update cache
		if err := h.updateCache(price); err != nil {
			h.logger.WithError(err).WithField("symbol", gap.Symbol).Error("Failed to update cache for gap fill")
		}
		
		priceCount++
	}
	
	// h.logger.WithFields(logrus.Fields{
	// 	"symbol":      gap.Symbol,
	// 	"gap_size":    gap.GapSize,
	// 	"prices_filled": priceCount,
	// }).Info("Gap filled successfully")
}

// logErrorRateLimited logs errors with rate limiting to prevent log spam
func (h *Hub) logErrorRateLimited(err error, message string) {
	h.errorLogMu.Lock()
	defer h.errorLogMu.Unlock()
	
	// Only log once per second
	if time.Since(h.lastErrorLog) < time.Second {
		return
	}
	
	h.lastErrorLog = time.Now()
	h.logger.WithError(err).Error(message)
}

// UpdateSymbols dynamically updates the symbols the hub is subscribed to
func (h *Hub) UpdateSymbols(ctx context.Context, newSymbols []string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	if !h.running.Load() {
		return fmt.Errorf("hub is not running")
	}
	
	// h.logger.WithField("count", len(newSymbols)).Info("Updating hub symbols")
	
	// Create new connection pool first (before stopping old one)
	newPool := NewConnectionPool(newSymbols, &h.cfg.Exchange, h.logger.Logger)
	newPool.SetPriceHandler(h.processor.ProcessPrice)
	
	// Start new connection pool
	if err := newPool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start new connection pool: %w", err)
	}
	
	// Now stop the old pool (after new one is running)
	oldPool := h.pool
	if oldPool != nil {
		if err := oldPool.Stop(); err != nil {
			h.logger.WithError(err).Error("Failed to stop old connection pool")
		}
	}
	
	// Atomic update of symbols and pool
	h.symbols = newSymbols
	h.pool = newPool
	
	// h.logger.WithField("symbols", len(newSymbols)).Info("Hub symbols updated successfully")
	return nil
}
