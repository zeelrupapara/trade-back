package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/cache"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/models"
)

// PeriodExtremesService calculates and tracks period-based high/low levels
type PeriodExtremesService struct {
	influxDB   *database.InfluxClient
	redisCache *cache.RedisClient
	natsClient *messaging.NATSClient
	classifier *AssetClassifier
	logger     *logrus.Entry
	
	// In-memory cache for fast access
	cache      map[string]map[models.PeriodType]*models.PeriodLevel
	cacheMutex sync.RWMutex
	
	// Update tracking
	lastUpdate map[string]time.Time
	updateMutex sync.RWMutex
	
	// Background update control
	stopChan   chan struct{}
	running    bool
	wg         sync.WaitGroup
}

// NewPeriodExtremesService creates a new period extremes service
func NewPeriodExtremesService(
	influxDB *database.InfluxClient,
	redisCache *cache.RedisClient,
	natsClient *messaging.NATSClient,
	logger *logrus.Logger,
) *PeriodExtremesService {
	return &PeriodExtremesService{
		influxDB:   influxDB,
		redisCache: redisCache,
		natsClient: natsClient,
		classifier: NewAssetClassifier(),
		logger:     logger.WithField("component", "period-extremes"),
		cache:      make(map[string]map[models.PeriodType]*models.PeriodLevel),
		lastUpdate: make(map[string]time.Time),
		stopChan:   make(chan struct{}),
	}
}

// Start begins background updates and period boundary checks
func (s *PeriodExtremesService) Start(ctx context.Context) error {
	if s.running {
		return fmt.Errorf("service already running")
	}
	
	s.running = true
	s.logger.Info("Starting period extremes service")
	
	// Start background update goroutine
	s.wg.Add(1)
	go s.backgroundUpdater(ctx)
	
	// Start period boundary checker
	s.wg.Add(1)
	go s.periodBoundaryChecker(ctx)
	
	return nil
}

// Stop gracefully stops the service
func (s *PeriodExtremesService) Stop() error {
	if !s.running {
		return nil
	}
	
	s.logger.Info("Stopping period extremes service")
	close(s.stopChan)
	s.wg.Wait()
	s.running = false
	
	return nil
}

// GetPeriodLevels retrieves period levels for a symbol
func (s *PeriodExtremesService) GetPeriodLevels(ctx context.Context, symbol string, period models.PeriodType) (*models.PeriodLevel, error) {
	// Check memory cache first
	if cached := s.getFromCache(symbol, period); cached != nil {
		// Check if cache is fresh (5 minutes for daily, 15 for weekly, 30 for monthly)
		cacheTTL := s.getCacheTTL(period)
		if time.Since(cached.LastUpdated) < cacheTTL {
			return cached, nil
		}
	}
	
	// Calculate period boundaries
	now := time.Now()
	start, end := models.GetPeriodBoundaries(period, now)
	
	// Fetch from InfluxDB - only need high and low for ATH/ATL
	high, low, _, _, err := s.influxDB.GetPeriodHighLow(ctx, symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to get period high/low: %w", err)
	}
	
	// Create period level with only ATH/ATL
	level := &models.PeriodLevel{
		Symbol:      symbol,
		Period:      period,
		High:        high,  // ATH for this period
		Low:         low,   // ATL for this period
		StartTime:   start,
		EndTime:     end,
		LastUpdated: time.Now(),
		IsActive:    models.IsPeriodActive(period, start),
	}
	
	// Update cache
	s.updateCache(symbol, period, level)
	
	// Store in Redis with TTL (use background context to avoid cancellation)
	go s.storeInRedis(context.Background(), symbol, period, level)
	
	// Publish update via NATS
	go s.publishUpdate(symbol, period, level)
	
	return level, nil
}

// GetAllPeriodLevels retrieves levels for all periods
func (s *PeriodExtremesService) GetAllPeriodLevels(ctx context.Context, symbol string) (*models.PeriodLevelsResponse, error) {
	response := &models.PeriodLevelsResponse{
		Symbol:     symbol,
		AssetClass: s.classifier.ClassifySymbol(symbol),
		Timestamp:  time.Now(),
	}
	
	// Define periods to fetch
	periods := []models.PeriodType{
		models.PeriodDaily,
		models.PeriodWeekly,
		models.PeriodMonthly,
		models.PeriodYearly,
	}
	
	// Create wait group for parallel fetching
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, 0)
	
	for _, period := range periods {
		wg.Add(1)
		go func(p models.PeriodType) {
			defer wg.Done()
			
			level, err := s.GetPeriodLevels(ctx, symbol, p)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("%s: %w", p, err))
				mu.Unlock()
				return
			}
			
			mu.Lock()
			switch p {
			case models.PeriodDaily:
				response.Daily = level
			case models.PeriodWeekly:
				response.Weekly = level
			case models.PeriodMonthly:
				response.Monthly = level
			case models.PeriodYearly:
				response.Yearly = level
			}
			mu.Unlock()
		}(period)
	}
	
	wg.Wait()
	
	if len(errors) > 0 {
		s.logger.WithField("symbol", symbol).WithField("errors", errors).Warn("Some period levels failed to load")
	}
	
	return response, nil
}

// GetMultipleSymbolLevels retrieves levels for multiple symbols efficiently
func (s *PeriodExtremesService) GetMultipleSymbolLevels(ctx context.Context, symbols []string, period models.PeriodType) (map[string]*models.PeriodLevel, error) {
	results := make(map[string]*models.PeriodLevel)
	var mu sync.Mutex
	var wg sync.WaitGroup
	
	// Limit concurrent requests
	semaphore := make(chan struct{}, 10)
	
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			
			level, err := s.GetPeriodLevels(ctx, sym, period)
			if err != nil {
				s.logger.WithError(err).WithField("symbol", sym).Warn("Failed to get period levels")
				return
			}
			
			mu.Lock()
			results[sym] = level
			mu.Unlock()
		}(symbol)
	}
	
	wg.Wait()
	return results, nil
}

// UpdateFromPrice updates period levels if a new extreme is hit
func (s *PeriodExtremesService) UpdateFromPrice(symbol string, price float64) {
	periods := []models.PeriodType{
		models.PeriodDaily,
		models.PeriodWeekly,
		models.PeriodMonthly,
		models.PeriodYearly,
	}
	
	for _, period := range periods {
		if cached := s.getFromCache(symbol, period); cached != nil {
			updated := false
			oldHigh := cached.High
			oldLow := cached.Low
			updateType := ""
			
			// Check for new high
			if price > cached.High {
				cached.High = price
				updated = true
				updateType = "new_high"
				s.logger.WithFields(logrus.Fields{
					"symbol": symbol,
					"period": period,
					"old_high": oldHigh,
					"new_high": price,
				}).Info("New period high detected")
			}
			
			// Check for new low
			if price < cached.Low && price > 0 {
				cached.Low = price
				updated = true
				updateType = "new_low"
				s.logger.WithFields(logrus.Fields{
					"symbol": symbol,
					"period": period,
					"old_low": oldLow,
					"new_low": price,
				}).Info("New period low detected")
			}
			
			if updated {
				cached.LastUpdated = time.Now()
				s.updateCache(symbol, period, cached)
				
				// Publish update via NATS for WebSocket broadcast
				go s.publishLevelUpdate(symbol, period, cached, updateType, oldHigh, oldLow, price)
			}
			
			// Check if price is approaching any Fibonacci level
			go s.checkLevelApproach(symbol, period, cached, price)
		}
	}
}

// backgroundUpdater periodically updates cached levels
func (s *PeriodExtremesService) backgroundUpdater(ctx context.Context) {
	defer s.wg.Done()
	
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.updateAllCachedLevels(ctx)
		}
	}
}

// periodBoundaryChecker checks for period boundaries and resets levels
func (s *PeriodExtremesService) periodBoundaryChecker(ctx context.Context) {
	defer s.wg.Done()
	
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	lastCheck := make(map[models.PeriodType]time.Time)
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			now := time.Now()
			
			// Check daily boundary (midnight UTC)
			if now.Hour() == 0 && now.Minute() < 1 {
				if lastCheck[models.PeriodDaily].Day() != now.Day() {
					s.handlePeriodBoundary(ctx, models.PeriodDaily)
					lastCheck[models.PeriodDaily] = now
				}
			}
			
			// Check weekly boundary (Monday midnight UTC)
			if now.Weekday() == time.Monday && now.Hour() == 0 && now.Minute() < 1 {
				if lastCheck[models.PeriodWeekly].Day() != now.Day() {
					s.handlePeriodBoundary(ctx, models.PeriodWeekly)
					lastCheck[models.PeriodWeekly] = now
				}
			}
			
			// Check monthly boundary (1st of month midnight UTC)
			if now.Day() == 1 && now.Hour() == 0 && now.Minute() < 1 {
				if lastCheck[models.PeriodMonthly].Month() != now.Month() {
					s.handlePeriodBoundary(ctx, models.PeriodMonthly)
					lastCheck[models.PeriodMonthly] = now
				}
			}
			
			// Check yearly boundary (Jan 1st midnight UTC)
			if now.Month() == time.January && now.Day() == 1 && now.Hour() == 0 && now.Minute() < 1 {
				if lastCheck[models.PeriodYearly].Year() != now.Year() {
					s.handlePeriodBoundary(ctx, models.PeriodYearly)
					lastCheck[models.PeriodYearly] = now
				}
			}
		}
	}
}

// handlePeriodBoundary handles the transition to a new period
func (s *PeriodExtremesService) handlePeriodBoundary(ctx context.Context, period models.PeriodType) {
	s.logger.WithField("period", period).Info("Period boundary crossed, resetting levels")
	
	// Collect affected symbols before clearing
	affectedSymbols := make([]string, 0)
	s.cacheMutex.RLock()
	for symbol := range s.cache {
		affectedSymbols = append(affectedSymbols, symbol)
	}
	s.cacheMutex.RUnlock()
	
	// Clear cache for this period
	s.cacheMutex.Lock()
	for symbol := range s.cache {
		if s.cache[symbol] != nil {
			delete(s.cache[symbol], period)
		}
	}
	s.cacheMutex.Unlock()
	
	// Publish period boundary event for WebSocket broadcast
	s.publishPeriodBoundary(period, affectedSymbols)
}

// updateAllCachedLevels updates all cached levels
func (s *PeriodExtremesService) updateAllCachedLevels(ctx context.Context) {
	s.cacheMutex.RLock()
	symbols := make([]string, 0, len(s.cache))
	for symbol := range s.cache {
		symbols = append(symbols, symbol)
	}
	s.cacheMutex.RUnlock()
	
	for _, symbol := range symbols {
		periods := []models.PeriodType{
			models.PeriodDaily,
			models.PeriodWeekly,
			models.PeriodMonthly,
		}
		
		for _, period := range periods {
			if _, err := s.GetPeriodLevels(ctx, symbol, period); err != nil {
				s.logger.WithError(err).WithFields(logrus.Fields{
					"symbol": symbol,
					"period": period,
				}).Warn("Failed to update period levels")
			}
		}
	}
}

// Cache helper methods

func (s *PeriodExtremesService) getFromCache(symbol string, period models.PeriodType) *models.PeriodLevel {
	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()
	
	if symbolCache, exists := s.cache[symbol]; exists {
		if level, exists := symbolCache[period]; exists {
			return level
		}
	}
	return nil
}

func (s *PeriodExtremesService) updateCache(symbol string, period models.PeriodType, level *models.PeriodLevel) {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()
	
	if s.cache[symbol] == nil {
		s.cache[symbol] = make(map[models.PeriodType]*models.PeriodLevel)
	}
	s.cache[symbol][period] = level
	
	s.updateMutex.Lock()
	s.lastUpdate[symbol] = time.Now()
	s.updateMutex.Unlock()
}

func (s *PeriodExtremesService) getCacheTTL(period models.PeriodType) time.Duration {
	switch period {
	case models.PeriodDaily:
		return 5 * time.Minute
	case models.PeriodWeekly:
		return 15 * time.Minute
	case models.PeriodMonthly:
		return 30 * time.Minute
	case models.PeriodYearly:
		return 1 * time.Hour
	default:
		return 5 * time.Minute
	}
}

// Redis storage methods

func (s *PeriodExtremesService) storeInRedis(ctx context.Context, symbol string, period models.PeriodType, level *models.PeriodLevel) {
	if s.redisCache == nil {
		return
	}
	
	key := fmt.Sprintf("period_level:%s:%s", symbol, period)
	ttl := s.getCacheTTL(period)
	
	// Store with TTL
	if err := s.redisCache.SetJSON(ctx, key, level, ttl); err != nil {
		s.logger.WithError(err).Warn("Failed to store period level in Redis")
	}
}

func (s *PeriodExtremesService) getFromRedis(ctx context.Context, symbol string, period models.PeriodType) (*models.PeriodLevel, error) {
	if s.redisCache == nil {
		return nil, fmt.Errorf("redis not configured")
	}
	
	key := fmt.Sprintf("period_level:%s:%s", symbol, period)
	var level models.PeriodLevel
	
	found, err := s.redisCache.GetJSON(ctx, key, &level)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("not found in cache")
	}
	
	return &level, nil
}

// NATS publishing

func (s *PeriodExtremesService) publishUpdate(symbol string, period models.PeriodType, level *models.PeriodLevel) {
	if s.natsClient == nil {
		return
	}
	
	subject := fmt.Sprintf("levels.update.%s.%s", symbol, period)
	if err := s.natsClient.PublishJSON(subject, level); err != nil {
		s.logger.WithError(err).Warn("Failed to publish period level update")
	}
}

// publishLevelUpdate publishes detailed level update for WebSocket broadcast
func (s *PeriodExtremesService) publishLevelUpdate(symbol string, period models.PeriodType, level *models.PeriodLevel, updateType string, oldHigh, oldLow, price float64) {
	if s.natsClient == nil {
		return
	}
	
	update := models.PeriodLevelUpdate{
		Type:         updateType,
		Symbol:       symbol,
		Period:       period,
		Level:        level,
		CurrentPrice: price,
		Timestamp:    time.Now().Unix(),
	}
	
	if updateType == "new_high" {
		update.OldValue = oldHigh
		update.NewValue = level.High
	} else if updateType == "new_low" {
		update.OldValue = oldLow
		update.NewValue = level.Low
	}
	
	// Publish to WebSocket broadcast channel
	subject := "websocket.broadcast.period_levels"
	if err := s.natsClient.PublishJSON(subject, update); err != nil {
		s.logger.WithError(err).Warn("Failed to publish WebSocket level update")
	}
}

// checkLevelApproach checks if price is near any Fibonacci level
func (s *PeriodExtremesService) checkLevelApproach(symbol string, period models.PeriodType, level *models.PeriodLevel, price float64) {
	if s.natsClient == nil || level == nil {
		return
	}
	
	// Define tolerance (0.2% of price)
	tolerance := price * 0.002
	
	// Only check high and low levels (no Fibonacci)
	levels := map[string]float64{
		"high": level.High,
		"low":  level.Low,
	}
	
	for levelName, levelPrice := range levels {
		distance := price - levelPrice
		absDistance := distance
		if absDistance < 0 {
			absDistance = -absDistance
		}
		
		if absDistance <= tolerance {
			direction := "below"
			if distance > 0 {
				direction = "above"
			}
			
			alert := models.LevelApproachAlert{
				Symbol:          symbol,
				Period:          period,
				Level:           levelName,
				LevelPrice:      levelPrice,
				CurrentPrice:    price,
				Distance:        distance,
				DistancePercent: (absDistance / price) * 100,
				Direction:       direction,
				Timestamp:       time.Now().Unix(),
			}
			
			// Publish alert
			subject := "websocket.broadcast.level_approach"
			s.natsClient.PublishJSON(subject, alert)
			
			// Only alert for the closest level
			break
		}
	}
}

// publishPeriodBoundary publishes when a new period starts
func (s *PeriodExtremesService) publishPeriodBoundary(period models.PeriodType, affectedSymbols []string) {
	if s.natsClient == nil {
		return
	}
	
	event := models.PeriodBoundaryEvent{
		Period:          period,
		AffectedSymbols: affectedSymbols,
		Timestamp:       time.Now().Unix(),
	}
	
	// Broadcast to all WebSocket clients
	subject := "websocket.broadcast.period_boundary"
	if err := s.natsClient.PublishJSON(subject, event); err != nil {
		s.logger.WithError(err).Warn("Failed to publish period boundary event")
	}
}