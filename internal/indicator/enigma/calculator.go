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
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// Calculator calculates Enigma levels for symbols
type Calculator struct {
	influx   *database.InfluxClient
	redis    *cache.RedisClient
	nats     *messaging.NATSClient
	logger   *logrus.Entry
	cfg      *config.FeaturesConfig
	
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
	ATH           float64
	ATL           float64
	LastPrice     float64
	LastUpdate    time.Time
	LastCalc      time.Time
}

// NewCalculator creates a new Enigma calculator
func NewCalculator(
	influx *database.InfluxClient,
	redis *cache.RedisClient,
	nats *messaging.NATSClient,
	cfg *config.FeaturesConfig,
	logger *logrus.Logger,
) *Calculator {
	return &Calculator{
		influx:  influx,
		redis:   redis,
		nats:    nats,
		logger:  logger.WithField("component", "enigma"),
		cfg:     cfg,
		symbols: make(map[string]*SymbolData),
		done:    make(chan struct{}),
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
	
	// Load historical ATH/ATL on startup
	// ec.logger.Info("Loading historical ATH/ATL data...")
	if err := ec.loadHistoricalExtremes(ctx); err != nil {
		ec.logger.WithError(err).Warn("Failed to load historical ATH/ATL, will calculate from incoming data")
	}
	
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

// updatePrice updates the price for a symbol
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
	
	symbolData.LastPrice = price.Price
	symbolData.LastUpdate = price.Timestamp
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
	
	// Ensure we have ATH/ATL
	if symbolData.ATH == 0 || symbolData.ATL == 0 {
		return nil // Will be updated by ATH/ATL updater
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

// updateAllATHATL updates ATH/ATL for all symbols
func (ec *Calculator) updateAllATHATL(ctx context.Context) {
	ec.mu.RLock()
	symbols := make([]string, 0, len(ec.symbols))
	for symbol := range ec.symbols {
		symbols = append(symbols, symbol)
	}
	ec.mu.RUnlock()
	
	for _, symbol := range symbols {
		ath, atl, err := ec.influx.GetATHATL(ctx, symbol)
		if err != nil {
			ec.logger.WithError(err).WithField("symbol", symbol).Error("Failed to get ATH/ATL")
			continue
		}
		
		ec.mu.Lock()
		if data, exists := ec.symbols[symbol]; exists {
			data.ATH = ath
			data.ATL = atl
			
			// Also check if current price is new ATH/ATL
			if data.LastPrice > data.ATH && data.LastPrice > 0 {
				data.ATH = data.LastPrice
			}
			if data.LastPrice < data.ATL && data.LastPrice > 0 && data.ATL > 0 {
				data.ATL = data.LastPrice
			}
		}
		ec.mu.Unlock()
		
	}
}

// GetEnigmaLevel gets the current Enigma level for a symbol
func (ec *Calculator) GetEnigmaLevel(symbol string) (*models.EnigmaData, error) {
	// Try cache first
	enigma, err := ec.redis.GetEnigmaLevel(context.Background(), symbol)
	if err == nil && enigma != nil {
		return enigma, nil
	}
	
	// Calculate if not in cache
	ec.mu.RLock()
	symbolData, exists := ec.symbols[symbol]
	ec.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("symbol not found")
	}
	
	if symbolData.ATH == 0 || symbolData.ATL == 0 {
		return nil, fmt.Errorf("ATH/ATL not available")
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
	// For now, load ATH/ATL for common symbols
	// In a production system, this would come from MySQL or configuration
	commonSymbols := []string{
		"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
		"ADAUSDT", "DOGEUSDT", "MATICUSDT", "DOTUSDT", "AVAXUSDT",
	}
	
	// ec.logger.WithField("symbols", len(commonSymbols)).Info("Loading ATH/ATL for common symbols")
	
	// Load ATH/ATL for each symbol
	loadedCount := 0
	for _, symbol := range commonSymbols {
		ath, atl, err := ec.influx.GetATHATL(ctx, symbol)
		if err != nil {
			continue
		}
		
		if ath == 0 || atl == 0 {
			continue
		}
		
		// Store in memory
		ec.mu.Lock()
		if _, exists := ec.symbols[symbol]; !exists {
			ec.symbols[symbol] = &SymbolData{
				Symbol: symbol,
			}
		}
		ec.symbols[symbol].ATH = ath
		ec.symbols[symbol].ATL = atl
		ec.mu.Unlock()
		
		loadedCount++
	}
	
	if loadedCount > 0 {
		// ec.logger.WithField("loaded", loadedCount).Info("Successfully loaded historical ATH/ATL data")
	} else {
		ec.logger.Warn("No historical ATH/ATL data found, will calculate from incoming prices")
	}
	
	return nil
}