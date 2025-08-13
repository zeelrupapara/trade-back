package exchange

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
)

// OANDADailyStats holds 24hr statistics for an instrument
type OANDADailyStats struct {
	Instrument    string    `json:"instrument"`
	Open24h       float64   `json:"open_24h"`
	High24h       float64   `json:"high_24h"`
	Low24h        float64   `json:"low_24h"`
	Close24h      float64   `json:"close_24h"`
	Change24h     float64   `json:"change_24h"`
	ChangePercent float64   `json:"change_percent"`
	Volume24h     int64     `json:"volume_24h"` // Tick count for forex
	LastUpdate    time.Time `json:"last_update"`
}

// OANDADailyStatsManager manages 24hr statistics for OANDA instruments
type OANDADailyStatsManager struct {
	restClient     *OANDARESTClient
	config         *config.OANDAConfig
	logger         *logrus.Entry
	instruments    []string
	stats          map[string]*OANDADailyStats
	mu             sync.RWMutex
	updateInterval time.Duration
	done           chan struct{}
	wg             sync.WaitGroup
}

// NewOANDADailyStatsManager creates a new daily stats manager
func NewOANDADailyStatsManager(config *config.OANDAConfig, instruments []string, logger *logrus.Logger) *OANDADailyStatsManager {
	return &OANDADailyStatsManager{
		restClient:     NewOANDARESTClient(config, logger),
		config:         config,
		logger:         logger.WithField("component", "oanda-daily-stats"),
		instruments:    instruments,
		stats:          make(map[string]*OANDADailyStats),
		updateInterval: 5 * time.Minute, // Update every 5 minutes
		done:           make(chan struct{}),
	}
}

// Start begins the periodic update of 24hr statistics
func (dm *OANDADailyStatsManager) Start(ctx context.Context) error {
	dm.logger.Info("Starting OANDA daily stats manager")
	
	// Initial fetch
	if err := dm.updateAllStats(ctx); err != nil {
		dm.logger.WithError(err).Error("Failed initial stats update")
	}
	
	// Start periodic updates
	dm.wg.Add(1)
	go dm.periodicUpdate(ctx)
	
	return nil
}

// Stop stops the daily stats manager
func (dm *OANDADailyStatsManager) Stop() {
	dm.logger.Info("Stopping OANDA daily stats manager")
	close(dm.done)
	dm.wg.Wait()
}

// periodicUpdate runs periodic updates of 24hr statistics
func (dm *OANDADailyStatsManager) periodicUpdate(ctx context.Context) {
	defer dm.wg.Done()
	
	ticker := time.NewTicker(dm.updateInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-dm.done:
			return
		case <-ticker.C:
			if err := dm.updateAllStats(ctx); err != nil {
				dm.logger.WithError(err).Error("Failed to update daily stats")
			}
		}
	}
}

// updateAllStats updates statistics for all instruments
func (dm *OANDADailyStatsManager) updateAllStats(ctx context.Context) error {
	dm.logger.Debug("Updating 24hr statistics for all instruments")
	
	var wg sync.WaitGroup
	errors := make(chan error, len(dm.instruments))
	
	for _, instrument := range dm.instruments {
		wg.Add(1)
		go func(inst string) {
			defer wg.Done()
			if err := dm.updateInstrumentStats(ctx, inst); err != nil {
				errors <- fmt.Errorf("%s: %w", inst, err)
			}
		}(instrument)
	}
	
	wg.Wait()
	close(errors)
	
	// Collect any errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
		dm.logger.WithError(err).Warn("Failed to update instrument stats")
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("failed to update %d instruments", len(errs))
	}
	
	dm.logger.WithField("instruments", len(dm.instruments)).Debug("Successfully updated all 24hr statistics")
	return nil
}

// updateInstrumentStats updates statistics for a single instrument
func (dm *OANDADailyStatsManager) updateInstrumentStats(ctx context.Context, instrument string) error {
	// Fetch D (daily) candles for the last 2 days
	// We need 2 days to calculate 24hr change from previous close
	to := time.Now()
	from := to.Add(-48 * time.Hour)
	
	// Use 0 for count when from and to are specified
	candles, err := dm.restClient.GetCandles(ctx, instrument, "D", from, to, 0)
	if err != nil {
		return fmt.Errorf("failed to fetch daily candles: %w", err)
	}
	
	if len(candles) < 2 {
		// Try with H1 candles for last 24 hours as fallback
		return dm.updateWithHourlyCandles(ctx, instrument)
	}
	
	// Calculate 24hr statistics from daily candles
	yesterday := candles[len(candles)-2]
	today := candles[len(candles)-1]
	
	// Parse values
	var open24h, high24h, low24h, close24h float64
	
	// Use yesterday's close as 24hr open
	if yesterday.Mid != nil {
		open24h, _ = dm.parseFloat(yesterday.Mid.Close)
	}
	
	// Use today's values for current stats
	if today.Mid != nil {
		high24h, _ = dm.parseFloat(today.Mid.High)
		low24h, _ = dm.parseFloat(today.Mid.Low)
		close24h, _ = dm.parseFloat(today.Mid.Close)
	}
	
	// Calculate change
	change24h := close24h - open24h
	changePercent := 0.0
	if open24h > 0 {
		changePercent = (change24h / open24h) * 100
	}
	
	// Update stats
	stats := &OANDADailyStats{
		Instrument:    instrument,
		Open24h:       open24h,
		High24h:       high24h,
		Low24h:        low24h,
		Close24h:      close24h,
		Change24h:     change24h,
		ChangePercent: changePercent,
		Volume24h:     int64(today.Volume),
		LastUpdate:    time.Now(),
	}
	
	dm.mu.Lock()
	dm.stats[instrument] = stats
	dm.mu.Unlock()
	
	dm.logger.WithFields(logrus.Fields{
		"instrument": instrument,
		"high":       high24h,
		"low":        low24h,
		"change":     changePercent,
	}).Debug("Updated 24hr statistics")
	
	return nil
}

// updateWithHourlyCandles uses hourly candles to calculate 24hr stats
func (dm *OANDADailyStatsManager) updateWithHourlyCandles(ctx context.Context, instrument string) error {
	to := time.Now()
	from := to.Add(-24 * time.Hour)
	
	// Use 0 for count when from and to are specified
	candles, err := dm.restClient.GetCandles(ctx, instrument, "H1", from, to, 0)
	if err != nil {
		return fmt.Errorf("failed to fetch hourly candles: %w", err)
	}
	
	if len(candles) == 0 {
		return fmt.Errorf("no candles available")
	}
	
	// Calculate 24hr stats from hourly candles
	var open24h, high24h, low24h, close24h float64
	var volume24h int64
	
	for i, candle := range candles {
		if candle.Mid == nil {
			continue
		}
		
		// Parse values
		o, _ := dm.parseFloat(candle.Mid.Open)
		h, _ := dm.parseFloat(candle.Mid.High)
		l, _ := dm.parseFloat(candle.Mid.Low)
		c, _ := dm.parseFloat(candle.Mid.Close)
		
		if i == 0 {
			open24h = o
			high24h = h
			low24h = l
		} else {
			if h > high24h {
				high24h = h
			}
			if l < low24h && l > 0 {
				low24h = l
			}
		}
		
		if i == len(candles)-1 {
			close24h = c
		}
		
		volume24h += int64(candle.Volume)
	}
	
	// Calculate change
	change24h := close24h - open24h
	changePercent := 0.0
	if open24h > 0 {
		changePercent = (change24h / open24h) * 100
	}
	
	// Update stats
	stats := &OANDADailyStats{
		Instrument:    instrument,
		Open24h:       open24h,
		High24h:       high24h,
		Low24h:        low24h,
		Close24h:      close24h,
		Change24h:     change24h,
		ChangePercent: changePercent,
		Volume24h:     volume24h,
		LastUpdate:    time.Now(),
	}
	
	dm.mu.Lock()
	dm.stats[instrument] = stats
	dm.mu.Unlock()
	
	return nil
}

// GetStats returns the current 24hr statistics for an instrument
func (dm *OANDADailyStatsManager) GetStats(instrument string) *OANDADailyStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	
	if stats, exists := dm.stats[instrument]; exists {
		// Return a copy
		statsCopy := *stats
		return &statsCopy
	}
	
	return nil
}

// GetAllStats returns all current statistics
func (dm *OANDADailyStatsManager) GetAllStats() map[string]*OANDADailyStats {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	
	result := make(map[string]*OANDADailyStats)
	for k, v := range dm.stats {
		statsCopy := *v
		result[k] = &statsCopy
	}
	
	return result
}

// parseFloat safely parses a string to float64
func (dm *OANDADailyStatsManager) parseFloat(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

// SetUpdateInterval sets the update interval for statistics
func (dm *OANDADailyStatsManager) SetUpdateInterval(interval time.Duration) {
	dm.updateInterval = interval
}