package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/external"
	"github.com/trade-back/pkg/models"
)

// UnifiedExtremeTracker tracks extremes for all asset types
type UnifiedExtremeTracker struct {
	// Data providers
	coingecko    *external.CoinGeckoClient
	alphaVantage *external.AlphaVantageClient
	influxDB     *database.InfluxClient
	mysqlDB      *database.MySQLClient
	redisCache   *cache.RedisClient
	
	// Asset classifier
	classifier   *AssetClassifier
	
	// In-memory cache for fast access
	extremes     map[string]*models.AssetExtreme
	extremeMutex sync.RWMutex
	
	// Update tracking
	lastUpdate   map[string]time.Time
	updateMutex  sync.RWMutex
	
	logger       *logrus.Entry
}

// NewUnifiedExtremeTracker creates a new unified extreme tracker
func NewUnifiedExtremeTracker(
	coingecko *external.CoinGeckoClient,
	alphaVantage *external.AlphaVantageClient,
	influxDB *database.InfluxClient,
	mysqlDB *database.MySQLClient,
	redisCache *cache.RedisClient,
	logger *logrus.Logger,
) *UnifiedExtremeTracker {
	return &UnifiedExtremeTracker{
		coingecko:    coingecko,
		alphaVantage: alphaVantage,
		influxDB:     influxDB,
		mysqlDB:      mysqlDB,
		redisCache:   redisCache,
		classifier:   NewAssetClassifier(),
		extremes:     make(map[string]*models.AssetExtreme),
		lastUpdate:   make(map[string]time.Time),
		logger:       logger.WithField("component", "extreme-tracker"),
	}
}

// GetExtremes fetches extremes for any asset type
func (ut *UnifiedExtremeTracker) GetExtremes(ctx context.Context, symbol string) (*models.AssetExtreme, error) {
	// Check memory cache first
	ut.extremeMutex.RLock()
	if extreme, exists := ut.extremes[symbol]; exists {
		ut.extremeMutex.RUnlock()
		
		// Check if cache is still fresh (1 hour for most assets)
		ut.updateMutex.RLock()
		lastUpdate := ut.lastUpdate[symbol]
		ut.updateMutex.RUnlock()
		
		if time.Since(lastUpdate) < time.Hour {
			return extreme, nil
		}
	} else {
		ut.extremeMutex.RUnlock()
	}
	
	// Check Redis cache
	if cached, err := ut.getFromRedis(ctx, symbol); err == nil && cached != nil {
		ut.updateMemoryCache(symbol, cached)
		return cached, nil
	}
	
	// Classify the asset
	assetClass := ut.classifier.ClassifySymbol(symbol)
	
	// Fetch from appropriate source based on asset class
	var extreme *models.AssetExtreme
	var err error
	
	switch assetClass {
	case models.AssetClassCrypto:
		extreme, err = ut.getCryptoExtremes(ctx, symbol)
		
	case models.AssetClassForex:
		extreme, err = ut.getForexExtremes(ctx, symbol)
		
	case models.AssetClassStock:
		extreme, err = ut.getStockExtremes(ctx, symbol)
		
	case models.AssetClassCommodity:
		// For commodities, try Alpha Vantage first
		extreme, err = ut.getCommodityExtremes(ctx, symbol)
		
	default:
		return nil, fmt.Errorf("unsupported asset class: %s", assetClass)
	}
	
	if err != nil {
		return nil, err
	}
	
	// Store in all cache layers
	ut.updateAllCaches(ctx, symbol, extreme)
	
	return extreme, nil
}

// getCryptoExtremes fetches crypto extremes from CoinGecko or InfluxDB
func (ut *UnifiedExtremeTracker) getCryptoExtremes(ctx context.Context, symbol string) (*models.AssetExtreme, error) {
	// Try CoinGecko first
	if ut.coingecko != nil {
		ath, atl, err := ut.coingecko.GetATHATL(ctx, symbol)
		if err == nil {
			return &models.AssetExtreme{
				Symbol:      symbol,
				AssetClass:  models.AssetClassCrypto,
				ATH:         ath,
				ATL:         atl,
				DataSource:  "coingecko",
				LastUpdated: time.Now().Unix(),
			}, nil
		}
		ut.logger.WithError(err).WithField("symbol", symbol).Warn("Failed to get from CoinGecko, trying InfluxDB")
	}
	
	// Fallback to InfluxDB historical data
	if ut.influxDB != nil {
		ath, atl, err := ut.influxDB.GetATHATL(ctx, symbol)
		if err == nil && ath > 0 && atl > 0 {
			return &models.AssetExtreme{
				Symbol:      symbol,
				AssetClass:  models.AssetClassCrypto,
				ATH:         ath,
				ATL:         atl,
				DataSource:  "influxdb",
				LastUpdated: time.Now().Unix(),
			}, nil
		}
	}
	
	return nil, fmt.Errorf("failed to get crypto extremes for %s", symbol)
}

// getForexExtremes fetches forex extremes from Alpha Vantage
func (ut *UnifiedExtremeTracker) getForexExtremes(ctx context.Context, pair string) (*models.AssetExtreme, error) {
	if ut.alphaVantage == nil {
		return nil, fmt.Errorf("Alpha Vantage client not configured")
	}
	
	extreme, err := ut.alphaVantage.GetForexExtremes(ctx, pair)
	if err != nil {
		return nil, fmt.Errorf("failed to get forex extremes: %w", err)
	}
	
	return extreme, nil
}

// getStockExtremes fetches stock extremes from Alpha Vantage
func (ut *UnifiedExtremeTracker) getStockExtremes(ctx context.Context, symbol string) (*models.AssetExtreme, error) {
	if ut.alphaVantage == nil {
		return nil, fmt.Errorf("Alpha Vantage client not configured")
	}
	
	extreme, err := ut.alphaVantage.GetStockExtremes(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get stock extremes: %w", err)
	}
	
	return extreme, nil
}

// getCommodityExtremes fetches commodity extremes
func (ut *UnifiedExtremeTracker) getCommodityExtremes(ctx context.Context, symbol string) (*models.AssetExtreme, error) {
	// Commodities might be available as ETFs on Alpha Vantage
	// For example, GLD for Gold, SLV for Silver
	commodityETFs := map[string]string{
		"GOLD":   "GLD",
		"SILVER": "SLV",
		"OIL":    "USO",
		"GAS":    "UNG",
	}
	
	etfSymbol, exists := commodityETFs[symbol]
	if !exists {
		etfSymbol = symbol // Try the symbol as-is
	}
	
	if ut.alphaVantage != nil {
		extreme, err := ut.alphaVantage.GetStockExtremes(ctx, etfSymbol)
		if err == nil {
			extreme.Symbol = symbol // Keep original symbol
			extreme.AssetClass = models.AssetClassCommodity
			return extreme, nil
		}
	}
	
	return nil, fmt.Errorf("failed to get commodity extremes for %s", symbol)
}

// UpdateExtremeFromPrice updates extremes if a new price is a new extreme
func (ut *UnifiedExtremeTracker) UpdateExtremeFromPrice(symbol string, price float64) bool {
	ut.extremeMutex.Lock()
	defer ut.extremeMutex.Unlock()
	
	extreme, exists := ut.extremes[symbol]
	if !exists {
		// No existing data, can't update
		return false
	}
	
	updated := false
	
	// Check for new ATH
	if price > extreme.ATH {
		ut.logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"old_ath":  extreme.ATH,
			"new_ath":  price,
		}).Info("New all-time high detected!")
		
		extreme.ATH = price
		extreme.ATHDate = time.Now().Format("2006-01-02")
		extreme.LastUpdated = time.Now().Unix()
		updated = true
	}
	
	// Check for new ATL
	if price < extreme.ATL && price > 0 {
		ut.logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"old_atl":  extreme.ATL,
			"new_atl":  price,
		}).Info("New all-time low detected!")
		
		extreme.ATL = price
		extreme.ATLDate = time.Now().Format("2006-01-02")
		extreme.LastUpdated = time.Now().Unix()
		updated = true
	}
	
	if updated {
		// Update all caches
		go ut.updateAllCaches(context.Background(), symbol, extreme)
	}
	
	return updated
}

// PreloadExtremes preloads extremes for multiple symbols
func (ut *UnifiedExtremeTracker) PreloadExtremes(ctx context.Context, symbols []string) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 5) // Limit concurrent requests
	
	for _, symbol := range symbols {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			if _, err := ut.GetExtremes(ctx, s); err != nil {
				ut.logger.WithError(err).WithField("symbol", s).Warn("Failed to preload extremes")
			}
		}(symbol)
	}
	
	wg.Wait()
	ut.logger.WithField("count", len(symbols)).Info("Preloaded extremes")
}

// updateMemoryCache updates the in-memory cache
func (ut *UnifiedExtremeTracker) updateMemoryCache(symbol string, extreme *models.AssetExtreme) {
	ut.extremeMutex.Lock()
	ut.extremes[symbol] = extreme
	ut.extremeMutex.Unlock()
	
	ut.updateMutex.Lock()
	ut.lastUpdate[symbol] = time.Now()
	ut.updateMutex.Unlock()
}

// updateAllCaches updates all cache layers
func (ut *UnifiedExtremeTracker) updateAllCaches(ctx context.Context, symbol string, extreme *models.AssetExtreme) {
	// Update memory cache
	ut.updateMemoryCache(symbol, extreme)
	
	// Update Redis cache
	if ut.redisCache != nil {
		if err := ut.storeInRedis(ctx, symbol, extreme); err != nil {
			ut.logger.WithError(err).Warn("Failed to update Redis cache")
		}
	}
	
	// Update MySQL (for persistence)
	if ut.mysqlDB != nil {
		if err := ut.storeInMySQL(ctx, extreme); err != nil {
			ut.logger.WithError(err).Warn("Failed to update MySQL")
		}
	}
}

// getFromRedis retrieves extremes from Redis cache
func (ut *UnifiedExtremeTracker) getFromRedis(ctx context.Context, symbol string) (*models.AssetExtreme, error) {
	// Implementation would depend on Redis client methods
	// For now, return not found
	return nil, fmt.Errorf("not implemented")
}

// storeInRedis stores extremes in Redis cache
func (ut *UnifiedExtremeTracker) storeInRedis(ctx context.Context, symbol string, extreme *models.AssetExtreme) error {
	// Implementation would depend on Redis client methods
	// Store with TTL based on asset class
	_ = time.Hour // ttl := time.Hour
	if extreme.AssetClass == models.AssetClassCrypto {
		_ = 30 * time.Minute // ttl = 30 * time.Minute // Crypto is more volatile
	}
	
	// Pseudo-code:
	// return ut.redisCache.SetWithTTL(fmt.Sprintf("extreme:%s", symbol), extreme, ttl)
	return nil
}

// storeInMySQL stores extremes in MySQL for persistence
func (ut *UnifiedExtremeTracker) storeInMySQL(ctx context.Context, extreme *models.AssetExtreme) error {
	// Implementation would depend on MySQL client methods
	// This would insert/update the asset_extremes table
	return nil
}

// GetMultipleExtremes fetches extremes for multiple symbols efficiently
func (ut *UnifiedExtremeTracker) GetMultipleExtremes(ctx context.Context, symbols []string) map[string]*models.AssetExtreme {
	results := make(map[string]*models.AssetExtreme)
	var mu sync.Mutex
	var wg sync.WaitGroup
	
	// Limit concurrent requests
	semaphore := make(chan struct{}, 10)
	
	for _, symbol := range symbols {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			if extreme, err := ut.GetExtremes(ctx, s); err == nil {
				mu.Lock()
				results[s] = extreme
				mu.Unlock()
			}
		}(symbol)
	}
	
	wg.Wait()
	return results
}

// RefreshAllExtremes refreshes extremes for all cached symbols
func (ut *UnifiedExtremeTracker) RefreshAllExtremes(ctx context.Context) {
	ut.extremeMutex.RLock()
	symbols := make([]string, 0, len(ut.extremes))
	for symbol := range ut.extremes {
		symbols = append(symbols, symbol)
	}
	ut.extremeMutex.RUnlock()
	
	ut.logger.WithField("count", len(symbols)).Info("Refreshing all extremes")
	
	for _, symbol := range symbols {
		// Force refresh by clearing from cache
		ut.updateMutex.Lock()
		delete(ut.lastUpdate, symbol)
		ut.updateMutex.Unlock()
		
		// Re-fetch
		if _, err := ut.GetExtremes(ctx, symbol); err != nil {
			ut.logger.WithError(err).WithField("symbol", symbol).Warn("Failed to refresh extremes")
		}
		
		// Small delay to respect rate limits
		time.Sleep(100 * time.Millisecond)
	}
}

// GetAssetInfo returns asset information including classification
func (ut *UnifiedExtremeTracker) GetAssetInfo(symbol string) models.AssetInfo {
	return ut.classifier.GetAssetInfo(symbol)
}