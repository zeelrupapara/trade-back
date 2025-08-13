package enigma

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/internal/services"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// Calculator calculates Enigma levels for symbols
type Calculator struct {
	influx         *database.InfluxClient
	redis          *cache.RedisClient
	nats           *messaging.NATSClient
	extremeTracker *services.UnifiedExtremeTracker
	logger         *logrus.Entry
	cfg            *config.FeaturesConfig
	
	// Symbol tracking
	symbols  map[string]*SymbolData
	mu       sync.RWMutex
	
	// Control
	running  bool
	done     chan struct{}
	wg       sync.WaitGroup
}

// SymbolData holds symbol-specific data for calculations
type SymbolData struct {
	Symbol        string
	AssetClass    models.AssetClass
	ATH           float64
	ATL           float64
	LastPrice     float64
	LastUpdate    time.Time
	LastCalc      time.Time
	DataSource    string
}

// NewCalculator creates a new Enigma calculator
func NewCalculator(
	influx *database.InfluxClient,
	redis *cache.RedisClient,
	nats *messaging.NATSClient,
	extremeTracker *services.UnifiedExtremeTracker,
	cfg *config.FeaturesConfig,
	logger *logrus.Logger,
) *Calculator {
	return &Calculator{
		influx:         influx,
		redis:          redis,
		nats:           nats,
		extremeTracker: extremeTracker,
		logger:         logger.WithField("component", "enigma"),
		cfg:            cfg,
		symbols:        make(map[string]*SymbolData),
		done:           make(chan struct{}),
	}
}

// Start starts the Enigma calculator
func (ec *Calculator) Start(ctx context.Context) error {
	if ec.running {
		return fmt.Errorf("calculator already running")
	}
	
	if !ec.cfg.EnigmaEnabled {
		ec.logger.Info("Enigma calculator disabled")
		return nil
	}
	
	ec.running = true
	// ec.logger.Info("Starting Enigma calculator")
	
	// Load historical ATH/ATL on startup (non-blocking)
	go func() {
		// ec.logger.Info("Loading historical ATH/ATL data...")
		if err := ec.loadHistoricalExtremes(ctx); err != nil {
			ec.logger.WithError(err).Warn("Failed to load historical ATH/ATL, will calculate from incoming data")
		}
	}()
	
	// Subscribe to price updates
	if err := ec.subscribeToPrices(); err != nil {
		return fmt.Errorf("failed to subscribe to prices: %w", err)
	}
	
	// Start calculation loop
	ec.wg.Add(1)
	go ec.calculationLoop(ctx)
	
	// Start ATH/ATL updater
	ec.wg.Add(1)
	go ec.athAtlUpdater(ctx)
	
	return nil
}

// Stop stops the Enigma calculator
func (ec *Calculator) Stop() error {
	if !ec.running {
		return nil
	}
	
	ec.logger.Info("Stopping Enigma calculator")
	close(ec.done)
	ec.wg.Wait()
	ec.running = false
	
	return nil
}

// subscribeToPrices subscribes to price updates
func (ec *Calculator) subscribeToPrices() error {
	return ec.nats.SubscribePrices(func(price *models.PriceData) {
		ec.updatePrice(price)
	})
}

// updatePrice updates the price for a symbol and triggers real-time Enigma calculation
func (ec *Calculator) updatePrice(price *models.PriceData) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	
	symbolData, exists := ec.symbols[price.Symbol]
	if !exists {
		symbolData = &SymbolData{
			Symbol: price.Symbol,
		}
		ec.symbols[price.Symbol] = symbolData
	}
	
	// Update the price
	symbolData.LastPrice = price.Price
	symbolData.LastUpdate = price.Timestamp
	
	// If we have ATH/ATL, calculate and send Enigma update immediately
	if symbolData.ATH > 0 && symbolData.ATL > 0 {
		// Calculate Enigma level in real-time
		enigma := ec.calculate(symbolData)
		
		// Send update through NATS for real-time updates
		go func(symbol string, enigmaData *models.EnigmaData) {
			if err := ec.nats.PublishEnigma(symbol, enigmaData); err != nil {
				ec.logger.WithError(err).WithField("symbol", symbol).Debug("Failed to publish real-time enigma")
			}
		}(price.Symbol, enigma)
		
		// Update last calculation time
		symbolData.LastCalc = time.Now()
	}
}

// calculationLoop runs the main calculation loop
func (ec *Calculator) calculationLoop(ctx context.Context) {
	defer ec.wg.Done()
	
	ticker := time.NewTicker(ec.cfg.EnigmaCalculationInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ec.done:
			return
		case <-ticker.C:
			ec.calculateAll()
		}
	}
}

// calculateAll calculates Enigma levels for all symbols
func (ec *Calculator) calculateAll() {
	ec.mu.RLock()
	symbols := make([]string, 0, len(ec.symbols))
	for symbol := range ec.symbols {
		symbols = append(symbols, symbol)
	}
	ec.mu.RUnlock()
	
	var wg sync.WaitGroup
	for _, symbol := range symbols {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			if err := ec.calculateForSymbol(s); err != nil {
				ec.logger.WithError(err).WithField("symbol", s).Error("Failed to calculate Enigma")
			}
		}(symbol)
	}
	
	wg.Wait()
}

// calculateForSymbol calculates Enigma level for a specific symbol
func (ec *Calculator) calculateForSymbol(symbol string) error {
	ec.mu.RLock()
	symbolData, exists := ec.symbols[symbol]
	ec.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("symbol data not found")
	}
	
	// Skip if recently calculated
	if time.Since(symbolData.LastCalc) < time.Minute {
		return nil
	}
	
	// Ensure we have ATH/ATL from all-time data
	if symbolData.ATH == 0 || symbolData.ATL == 0 {
		// Try to fetch from extreme tracker
		if ec.extremeTracker != nil {
			extreme, err := ec.extremeTracker.GetExtremes(context.Background(), symbol)
			if err == nil && extreme != nil {
				symbolData.ATH = extreme.ATH
				symbolData.ATL = extreme.ATL
				symbolData.AssetClass = extreme.AssetClass
				symbolData.DataSource = extreme.DataSource
			} else {
				return nil // Can't calculate without extremes
			}
		} else {
			return nil
		}
	}
	
	// Calculate Enigma level
	enigma := ec.calculate(symbolData)
	
	// Store in Redis
	if err := ec.redis.SetEnigmaLevel(context.Background(), symbol, enigma); err != nil {
		return fmt.Errorf("failed to cache enigma: %w", err)
	}
	
	// Store in InfluxDB
	if err := ec.influx.WriteEnigmaData(context.Background(), enigma); err != nil {
		return fmt.Errorf("failed to store enigma: %w", err)
	}
	
	// Publish to NATS
	if err := ec.nats.PublishEnigma(symbol, enigma); err != nil {
		return fmt.Errorf("failed to publish enigma: %w", err)
	}
	
	// Update last calculation time
	ec.mu.Lock()
	symbolData.LastCalc = time.Now()
	ec.mu.Unlock()
	
	
	return nil
}

// calculate performs the actual Enigma calculation
func (ec *Calculator) calculate(data *SymbolData) *models.EnigmaData {
	// Calculate price range
	range_ := data.ATH - data.ATL
	if range_ == 0 {
		range_ = 1 // Prevent division by zero
	}
	
	// Calculate current level (0-100%)
	level := ((data.LastPrice - data.ATL) / range_) * 100
	
	// Ensure level is within bounds
	if level < 0 {
		level = 0
	} else if level > 100 {
		level = 100
	}
	
	// Calculate Fibonacci levels
	fibLevels := models.FibonacciLevels{
		L0:   data.ATL,
		L236: data.ATL + (range_ * 0.236),
		L382: data.ATL + (range_ * 0.382),
		L50:  data.ATL + (range_ * 0.5),
		L618: data.ATL + (range_ * 0.618),
		L786: data.ATL + (range_ * 0.786),
		L100: data.ATH,
	}
	
	return &models.EnigmaData{
		Symbol:    data.Symbol,
		Level:     level,
		ATH:       data.ATH,
		ATL:       data.ATL,
		FibLevels: fibLevels,
		Timestamp: time.Now(),
	}
}

// athAtlUpdater updates ATH/ATL values periodically
func (ec *Calculator) athAtlUpdater(ctx context.Context) {
	defer ec.wg.Done()
	
	// Initial update
	ec.updateAllATHATL(ctx)
	
	// Update every hour
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ec.done:
			return
		case <-ticker.C:
			ec.updateAllATHATL(ctx)
		}
	}
}

// updateAllATHATL updates ATH/ATL for all symbols using unified extreme tracker
func (ec *Calculator) updateAllATHATL(ctx context.Context) {
	ec.mu.RLock()
	symbols := make([]string, 0, len(ec.symbols))
	for symbol := range ec.symbols {
		symbols = append(symbols, symbol)
	}
	ec.mu.RUnlock()
	
	for _, symbol := range symbols {
		var extreme *models.AssetExtreme
		var err error
		
		// Use unified extreme tracker for all-time data
		if ec.extremeTracker != nil {
			extreme, err = ec.extremeTracker.GetExtremes(ctx, symbol)
		} else {
			// Fallback to InfluxDB if no tracker
			ath, atl, err := ec.influx.GetATHATL(ctx, symbol)
			if err == nil {
				extreme = &models.AssetExtreme{
					Symbol: symbol,
					ATH: ath,
					ATL: atl,
				}
			}
		}
		
		if err != nil {
			ec.logger.WithError(err).WithField("symbol", symbol).Warn("Failed to get extremes")
			continue
		}
		
		if extreme == nil {
			continue
		}
		
		ec.mu.Lock()
		if data, exists := ec.symbols[symbol]; exists {
			data.ATH = extreme.ATH
			data.ATL = extreme.ATL
			data.AssetClass = extreme.AssetClass
			data.DataSource = extreme.DataSource
			
			// Check if current price is new extreme and update tracker
			if ec.extremeTracker != nil {
				if ec.extremeTracker.UpdateExtremeFromPrice(symbol, data.LastPrice) {
					// Extreme was updated, refresh our local data
					if newExtreme, err := ec.extremeTracker.GetExtremes(ctx, symbol); err == nil {
						data.ATH = newExtreme.ATH
						data.ATL = newExtreme.ATL
					}
				}
			}
		}
		ec.mu.Unlock()
	}
}

// GetEnigmaLevel gets the current Enigma level for a symbol
func (ec *Calculator) GetEnigmaLevel(symbol string) (*models.EnigmaData, error) {
	// For crypto assets, always fetch fresh extremes from CoinGecko
	if ec.extremeTracker != nil {
		ctx := context.Background()
		extreme, err := ec.extremeTracker.GetExtremes(ctx, symbol)
		if err == nil && extreme != nil && extreme.ATH > 0 && extreme.ATL > 0 {
			// Update the cached data
			ec.mu.Lock()
			if _, exists := ec.symbols[symbol]; !exists {
				ec.symbols[symbol] = &SymbolData{
					Symbol: symbol,
				}
			}
			ec.symbols[symbol].ATH = extreme.ATH
			ec.symbols[symbol].ATL = extreme.ATL
			ec.symbols[symbol].AssetClass = extreme.AssetClass
			ec.symbols[symbol].DataSource = extreme.DataSource
			ec.mu.Unlock()
		}
	}
	
	// Get current price from Redis cache
	if ec.redis != nil {
		ctx := context.Background()
		if priceData, err := ec.redis.GetPrice(ctx, symbol); err == nil && priceData != nil {
			ec.mu.Lock()
			if _, exists := ec.symbols[symbol]; exists {
				ec.symbols[symbol].LastPrice = priceData.Price
			}
			ec.mu.Unlock()
		}
	}
	
	// Now use the updated data
	ec.mu.RLock()
	symbolData, exists := ec.symbols[symbol]
	ec.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("symbol not found")
	}
	
	if symbolData.ATH == 0 || symbolData.ATL == 0 {
		return nil, fmt.Errorf("ATH/ATL not available")
	}
	
	// If no current price, use ATL as fallback for calculation
	if symbolData.LastPrice == 0 {
		ec.logger.WithField("symbol", symbol).Warn("No current price available, using ATL as fallback")
		symbolData.LastPrice = symbolData.ATL
	}
	
	return ec.calculate(symbolData), nil
}

// AddSymbol adds a symbol to track
func (ec *Calculator) AddSymbol(symbol string) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	
	if _, exists := ec.symbols[symbol]; !exists {
		ec.symbols[symbol] = &SymbolData{
			Symbol: symbol,
		}
	}
}

// RemoveSymbol removes a symbol from tracking
func (ec *Calculator) RemoveSymbol(symbol string) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	
	delete(ec.symbols, symbol)
}

// loadHistoricalExtremes loads historical ATH/ATL data on startup
func (ec *Calculator) loadHistoricalExtremes(ctx context.Context) error {
	// Common crypto, forex, and stock symbols (reduced for testing)
	commonSymbols := []string{
		// Crypto
		"BTCUSDT", "ETHUSDT",
		// Forex
		"EURUSD",
		// Stocks
		"AAPL",
	}
	
	// Preload extremes using unified tracker
	if ec.extremeTracker != nil {
		ec.extremeTracker.PreloadExtremes(ctx, commonSymbols)
	}
	
	// Load into local cache
	loadedCount := 0
	for _, symbol := range commonSymbols {
		var extreme *models.AssetExtreme
		
		if ec.extremeTracker != nil {
			extreme, _ = ec.extremeTracker.GetExtremes(ctx, symbol)
		} else {
			// Fallback to InfluxDB
			ath, atl, err := ec.influx.GetATHATL(ctx, symbol)
			if err == nil && ath > 0 && atl > 0 {
				extreme = &models.AssetExtreme{
					Symbol: symbol,
					ATH: ath,
					ATL: atl,
				}
			}
		}
		
		if extreme == nil || extreme.ATH == 0 || extreme.ATL == 0 {
			continue
		}
		
		// Store in memory
		ec.mu.Lock()
		if _, exists := ec.symbols[symbol]; !exists {
			ec.symbols[symbol] = &SymbolData{
				Symbol: symbol,
			}
		}
		ec.symbols[symbol].ATH = extreme.ATH
		ec.symbols[symbol].ATL = extreme.ATL
		ec.symbols[symbol].AssetClass = extreme.AssetClass
		ec.symbols[symbol].DataSource = extreme.DataSource
		ec.mu.Unlock()
		
		loadedCount++
	}
	
	if loadedCount > 0 {
		ec.logger.WithField("loaded", loadedCount).Info("Successfully loaded all-time extremes")
	} else {
		ec.logger.Warn("No extreme data found, will fetch on demand")
	}
	
	return nil
}