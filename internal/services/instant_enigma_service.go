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
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/models"
)

// InstantEnigmaService provides instant Enigma calculations using external data
type InstantEnigmaService struct {
	coingecko  *external.CoinGeckoClient
	influx     *database.InfluxClient
	redis      *cache.RedisClient
	nats       *messaging.NATSClient
	logger     *logrus.Entry
	
	// Cache for ATH/ATL data
	athAtlCache map[string]*ATHATLData
	cacheMutex  sync.RWMutex
	cacheTTL    time.Duration
}

// ATHATLData represents cached ATH/ATL data
type ATHATLData struct {
	Symbol    string
	ATH       float64
	ATL       float64
	FetchedAt time.Time
}

// NewInstantEnigmaService creates a new instant Enigma service
func NewInstantEnigmaService(
	coingecko *external.CoinGeckoClient,
	influx *database.InfluxClient,
	redis *cache.RedisClient,
	nats *messaging.NATSClient,
	logger *logrus.Logger,
) *InstantEnigmaService {
	return &InstantEnigmaService{
		coingecko:   coingecko,
		influx:      influx,
		redis:       redis,
		nats:        nats,
		logger:      logger.WithField("component", "instant-enigma"),
		athAtlCache: make(map[string]*ATHATLData),
		cacheTTL:    1 * time.Hour, // Cache ATH/ATL for 1 hour
	}
}

// GetInstantEnigma calculates Enigma levels instantly using CoinGecko data
func (s *InstantEnigmaService) GetInstantEnigma(ctx context.Context, symbol string, currentPrice float64) (*models.EnigmaData, error) {
	// Get ATH/ATL from cache or CoinGecko
	athAtl, err := s.getATHATL(ctx, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get ATH/ATL: %w", err)
	}
	
	// Calculate Enigma level
	enigmaLevel := s.calculateEnigmaLevel(currentPrice, athAtl.ATH, athAtl.ATL)
	
	// Calculate Fibonacci levels
	fibLevels := s.calculateFibonacciLevels(athAtl.ATH, athAtl.ATL)
	
	enigma := &models.EnigmaData{
		Symbol:    symbol,
		Level:     enigmaLevel,
		ATH:       athAtl.ATH,
		ATL:       athAtl.ATL,
		FibLevels: fibLevels,
		Timestamp: time.Now(),
	}
	
	// Store in cache
	if err := s.redis.SetEnigmaLevel(ctx, symbol, enigma); err != nil {
		s.logger.WithError(err).Warn("Failed to cache Enigma data")
	}
	
	// Store in InfluxDB
	if err := s.influx.WriteEnigmaData(ctx, enigma); err != nil {
		s.logger.WithError(err).Warn("Failed to store Enigma in InfluxDB")
	}
	
	// Publish to NATS
	if err := s.nats.PublishEnigma(symbol, enigma); err != nil {
		s.logger.WithError(err).Warn("Failed to publish Enigma update")
	}
	
	s.logger.WithFields(logrus.Fields{
		"symbol": symbol,
		"level":  enigmaLevel,
		"ath":    athAtl.ATH,
		"atl":    athAtl.ATL,
	})
	
	return enigma, nil
}

// getATHATL gets ATH/ATL from cache or fetches from CoinGecko
func (s *InstantEnigmaService) getATHATL(ctx context.Context, symbol string) (*ATHATLData, error) {
	// Check cache first
	s.cacheMutex.RLock()
	cached, exists := s.athAtlCache[symbol]
	s.cacheMutex.RUnlock()
	
	if exists && time.Since(cached.FetchedAt) < s.cacheTTL {
		return cached, nil
	}
	
	// Fetch from CoinGecko
	ath, atl, err := s.coingecko.GetATHATL(ctx, symbol)
	if err != nil {
		// Try to get from historical data as fallback
		return s.getATHATLFromHistory(ctx, symbol)
	}
	
	// Update cache
	athAtlData := &ATHATLData{
		Symbol:    symbol,
		ATH:       ath,
		ATL:       atl,
		FetchedAt: time.Now(),
	}
	
	s.cacheMutex.Lock()
	s.athAtlCache[symbol] = athAtlData
	s.cacheMutex.Unlock()
	
	return athAtlData, nil
}

// getATHATLFromHistory fallback method to get ATH/ATL from historical data
func (s *InstantEnigmaService) getATHATLFromHistory(ctx context.Context, symbol string) (*ATHATLData, error) {
	// For now, return a simple error - this would need proper InfluxDB query implementation
	// In the future, we could query InfluxDB for historical extremes using a query like:
	// from(bucket: "trades")
	// |> range(start: -365d)
	// |> filter(fn: (r) => r._measurement == "trades" and r.symbol == "SYMBOL")
	// |> group()
	// |> reduce(...)
	return nil, fmt.Errorf("historical ATH/ATL not implemented for symbol %s", symbol)
}

// calculateEnigmaLevel calculates the Enigma level (0-1 range)
func (s *InstantEnigmaService) calculateEnigmaLevel(currentPrice, ath, atl float64) float64 {
	if ath <= atl {
		return 0.5 // Invalid data
	}
	
	// Calculate position between ATL and ATH
	level := (currentPrice - atl) / (ath - atl)
	
	// Clamp between 0 and 1
	if level < 0 {
		level = 0
	} else if level > 1 {
		level = 1
	}
	
	return level
}

// calculateFibonacciLevels calculates standard Fibonacci retracement levels
func (s *InstantEnigmaService) calculateFibonacciLevels(ath, atl float64) models.FibonacciLevels {
	diff := ath - atl
	
	return models.FibonacciLevels{
		L0:   atl,                      // 0%
		L236: atl + (diff * 0.236),     // 23.6%
		L382: atl + (diff * 0.382),     // 38.2%
		L50:  atl + (diff * 0.5),       // 50%
		L618: atl + (diff * 0.618),     // 61.8%
		L786: atl + (diff * 0.786),     // 78.6%
		L100: ath,                      // 100%
	}
}

// PreloadATHATL preloads ATH/ATL data for multiple symbols
func (s *InstantEnigmaService) PreloadATHATL(ctx context.Context, symbols []string) {
	var wg sync.WaitGroup
	
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			
			if _, err := s.getATHATL(ctx, sym); err != nil {
				s.logger.WithError(err).WithField("symbol", sym).Warn("Failed to preload ATH/ATL")
			}
		}(symbol)
		
		// Small delay to respect rate limits
		time.Sleep(100 * time.Millisecond)
	}
	
	wg.Wait()
	s.logger.WithField("symbols", len(symbols)).Info("Preloaded ATH/ATL data")
}