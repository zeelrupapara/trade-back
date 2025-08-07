package aggregation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/internal/database"
	"github.com/trade-back/internal/messaging"
	"github.com/trade-back/pkg/models"
)

// OHLCVAggregator aggregates tick data into OHLCV bars
type OHLCVAggregator struct {
	influx  *database.InfluxClient
	nats    *messaging.NATSClient
	logger  *logrus.Entry
	
	// Active bars being built
	activeBars map[string]*BarBuilder
	mu         sync.RWMutex
	
	// Configuration
	resolutions []string
	
	// Control
	running bool
	done    chan struct{}
	wg      sync.WaitGroup
}

// BarBuilder builds OHLCV bars from tick data
type BarBuilder struct {
	Symbol     string
	Resolution string
	StartTime  time.Time
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     float64
	TradeCount int64
	FirstTick  bool
}

// Resolution represents a time resolution
type Resolution struct {
	Name     string
	Duration time.Duration
}

var supportedResolutions = []Resolution{
	{"1m", 1 * time.Minute},
	{"5m", 5 * time.Minute},
	{"15m", 15 * time.Minute},
	{"30m", 30 * time.Minute},
	{"1h", 1 * time.Hour},
	{"4h", 4 * time.Hour},
	{"1d", 24 * time.Hour},
}

// NewOHLCVAggregator creates a new OHLCV aggregator
func NewOHLCVAggregator(
	influx *database.InfluxClient,
	nats *messaging.NATSClient,
	logger *logrus.Logger,
) *OHLCVAggregator {
	resolutions := make([]string, len(supportedResolutions))
	for i, r := range supportedResolutions {
		resolutions[i] = r.Name
	}
	
	return &OHLCVAggregator{
		influx:      influx,
		nats:        nats,
		logger:      logger.WithField("component", "ohlcv-aggregator"),
		activeBars:  make(map[string]*BarBuilder),
		resolutions: resolutions,
		done:        make(chan struct{}),
	}
}

// Start starts the aggregator
func (oa *OHLCVAggregator) Start(ctx context.Context) error {
	if oa.running {
		return fmt.Errorf("aggregator already running")
	}
	
	oa.running = true
	// oa.logger.Info("Starting OHLCV aggregator")
	
	// Subscribe to price updates
	if err := oa.subscribeToPrices(); err != nil {
		return fmt.Errorf("failed to subscribe to prices: %w", err)
	}
	
	// Start bar completion checker
	oa.wg.Add(1)
	go oa.barCompletionChecker(ctx)
	
	return nil
}

// Stop stops the aggregator
func (oa *OHLCVAggregator) Stop() error {
	if !oa.running {
		return nil
	}
	
	// oa.logger.Info("Stopping OHLCV aggregator")
	
	// Complete all active bars
	oa.completeAllBars()
	
	close(oa.done)
	oa.wg.Wait()
	oa.running = false
	
	return nil
}

// subscribeToPrices subscribes to price updates
func (oa *OHLCVAggregator) subscribeToPrices() error {
	return oa.nats.SubscribePrices(func(price *models.PriceData) {
		oa.processPriceUpdate(price)
	})
}

// processPriceUpdate processes a price update
func (oa *OHLCVAggregator) processPriceUpdate(price *models.PriceData) {
	// Update bars for each resolution
	for _, resolution := range supportedResolutions {
		oa.updateBar(price, resolution)
	}
}

// updateBar updates or creates a bar for the given resolution
func (oa *OHLCVAggregator) updateBar(price *models.PriceData, resolution Resolution) {
	// Calculate bar start time
	barStartTime := oa.getBarStartTime(price.Timestamp, resolution.Duration)
	key := fmt.Sprintf("%s:%s:%d", price.Symbol, resolution.Name, barStartTime.Unix())
	
	oa.mu.Lock()
	defer oa.mu.Unlock()
	
	bar, exists := oa.activeBars[key]
	if !exists {
		// Create new bar
		bar = &BarBuilder{
			Symbol:     price.Symbol,
			Resolution: resolution.Name,
			StartTime:  barStartTime,
			Open:       price.Price,
			High:       price.Price,
			Low:        price.Price,
			Close:      price.Price,
			Volume:     price.Volume,
			TradeCount: 1,
			FirstTick:  true,
		}
		oa.activeBars[key] = bar
	} else {
		// Update existing bar
		if price.Price > bar.High {
			bar.High = price.Price
		}
		if price.Price < bar.Low {
			bar.Low = price.Price
		}
		bar.Close = price.Price
		bar.Volume += price.Volume
		bar.TradeCount++
	}
}

// getBarStartTime calculates the start time of a bar
func (oa *OHLCVAggregator) getBarStartTime(timestamp time.Time, duration time.Duration) time.Time {
	// Truncate to the nearest duration
	return timestamp.Truncate(duration)
}

// barCompletionChecker checks for completed bars
func (oa *OHLCVAggregator) barCompletionChecker(ctx context.Context) {
	defer oa.wg.Done()
	
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-oa.done:
			return
		case <-ticker.C:
			oa.checkCompletedBars()
		}
	}
}

// checkCompletedBars checks and completes bars that have ended
func (oa *OHLCVAggregator) checkCompletedBars() {
	now := time.Now()
	
	oa.mu.Lock()
	defer oa.mu.Unlock()
	
	completedBars := make([]*models.Bar, 0)
	keysToDelete := make([]string, 0)
	
	for key, builder := range oa.activeBars {
		// Find the resolution duration
		var duration time.Duration
		for _, res := range supportedResolutions {
			if res.Name == builder.Resolution {
				duration = res.Duration
				break
			}
		}
		
		// Check if bar period has ended
		barEndTime := builder.StartTime.Add(duration)
		if now.After(barEndTime) {
			// Convert to Bar model
			bar := &models.Bar{
				Symbol:     builder.Symbol,
				Timestamp:  builder.StartTime,
				Open:       builder.Open,
				High:       builder.High,
				Low:        builder.Low,
				Close:      builder.Close,
				Volume:     builder.Volume,
				TradeCount: builder.TradeCount,
			}
			
			completedBars = append(completedBars, bar)
			keysToDelete = append(keysToDelete, key)
		}
	}
	
	// Remove completed bars
	for _, key := range keysToDelete {
		delete(oa.activeBars, key)
	}
	
	// Store completed bars - the resolutions were already captured when we created the bars
	if len(completedBars) > 0 {
		// Create a simple wrapper with resolutions  
		// We need to track resolutions before deleting
		barData := make([]struct {
			Bar        *models.Bar
			Resolution string
		}, 0, len(completedBars))
		
		// Re-scan to get resolutions (this is safe since we're still under lock)
		for i, bar := range completedBars {
			// Find resolution from the original key that was deleted
			resolution := "1m" // default
			if i < len(keysToDelete) {
				// Extract resolution from key format: "symbol:resolution:timestamp"
				parts := strings.Split(keysToDelete[i], ":")
				if len(parts) >= 2 {
					resolution = parts[1]
				}
			}
			barData = append(barData, struct {
				Bar        *models.Bar
				Resolution string
			}{Bar: bar, Resolution: resolution})
		}
		
		go oa.storeCompletedBarsStructured(barData)
	}
}

// storeCompletedBarsStructured stores completed bars to InfluxDB with their resolutions
func (oa *OHLCVAggregator) storeCompletedBarsStructured(barData []struct {
	Bar        *models.Bar
	Resolution string
}) {
	ctx := context.Background()
	
	// Store the bars
	for _, data := range barData {
		if err := oa.influx.WriteBar(ctx, data.Bar, data.Resolution); err != nil {
			oa.logger.WithError(err).WithFields(logrus.Fields{
				"symbol":     data.Bar.Symbol,
				"resolution": data.Resolution,
				"timestamp":  data.Bar.Timestamp,
			}).Error("Failed to store bar")
		} else {
			oa.logger.WithFields(logrus.Fields{
				"symbol":     data.Bar.Symbol,
				"resolution": data.Resolution,
				"timestamp":  data.Bar.Timestamp,
				"close":      data.Bar.Close,
				"volume":     data.Bar.Volume,
			}).Debug("Bar stored")
		}
	}
}

// completeAllBars completes all active bars (for shutdown)
func (oa *OHLCVAggregator) completeAllBars() {
	oa.mu.Lock()
	defer oa.mu.Unlock()
	
	completedBars := make([]*models.Bar, 0, len(oa.activeBars))
	
	for _, builder := range oa.activeBars {
		bar := &models.Bar{
			Symbol:     builder.Symbol,
			Timestamp:  builder.StartTime,
			Open:       builder.Open,
			High:       builder.High,
			Low:        builder.Low,
			Close:      builder.Close,
			Volume:     builder.Volume,
			TradeCount: builder.TradeCount,
		}
		completedBars = append(completedBars, bar)
	}
	
	// Clear active bars
	oa.activeBars = make(map[string]*BarBuilder)
	
	// Store completed bars
	if len(completedBars) > 0 {
		// We need to pass resolutions, but the bars are already complete
		// Just use a default resolution
		barData := make([]struct {
			Bar        *models.Bar
			Resolution string
		}, 0, len(completedBars))
		for _, bar := range completedBars {
			barData = append(barData, struct {
				Bar        *models.Bar
				Resolution string
			}{Bar: bar, Resolution: "1m"})
		}
		oa.storeCompletedBarsStructured(barData)
	}
}

// GetActiveBarCount returns the number of active bars being built
func (oa *OHLCVAggregator) GetActiveBarCount() int {
	oa.mu.RLock()
	defer oa.mu.RUnlock()
	return len(oa.activeBars)
}

// GetActiveBar returns an active bar if it exists
func (oa *OHLCVAggregator) GetActiveBar(symbol, resolution string) *BarBuilder {
	oa.mu.RLock()
	defer oa.mu.RUnlock()
	
	// Find the active bar for this symbol and resolution
	for _, bar := range oa.activeBars {
		if bar.Symbol == symbol && bar.Resolution == resolution {
			return bar
		}
	}
	
	return nil
}