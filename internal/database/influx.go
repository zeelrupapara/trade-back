package database

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// InfluxClient handles InfluxDB time-series operations
type InfluxClient struct {
	client    influxdb2.Client
	writeAPI  api.WriteAPIBlocking
	queryAPI  api.QueryAPI
	logger    *logrus.Entry
	cfg       *config.InfluxConfig
	org       string
	bucket    string
}

// NewInfluxClient creates a new InfluxDB client
func NewInfluxClient(cfg *config.InfluxConfig, logger *logrus.Logger) *InfluxClient {
	client := influxdb2.NewClientWithOptions(
		cfg.URL,
		cfg.Token,
		influxdb2.DefaultOptions().
			SetHTTPRequestTimeout(uint(cfg.Timeout.Seconds())).
			SetLogLevel(0), // Silent - no logs
	)
	
	return &InfluxClient{
		client:   client,
		writeAPI: client.WriteAPIBlocking(cfg.Org, cfg.Bucket),
		queryAPI: client.QueryAPI(cfg.Org),
		logger:   logger.WithField("component", "influxdb"),
		cfg:      cfg,
		org:      cfg.Org,
		bucket:   cfg.Bucket,
	}
}

// Close closes the InfluxDB client
func (ic *InfluxClient) Close() {
	ic.client.Close()
}

// Health checks InfluxDB health
func (ic *InfluxClient) Health(ctx context.Context) error {
	health, err := ic.client.Health(ctx)
	if err != nil {
		return fmt.Errorf("failed to check health: %w", err)
	}
	
	if health.Status != "pass" {
		msg := ""
		if health.Message != nil {
			msg = *health.Message
		}
		return fmt.Errorf("influxdb health check failed: %s", msg)
	}
	
	return nil
}

// Price data operations

// WritePriceData writes price data to InfluxDB
func (ic *InfluxClient) WritePriceData(ctx context.Context, price *models.PriceData) error {
	// Use exchange from price data, default to "binance" if not set for backward compatibility
	exchange := price.Exchange
	if exchange == "" {
		exchange = "binance"
	}
	
	point := influxdb2.NewPoint(
		"trades",
		map[string]string{
			"exchange": exchange,
			"symbol":   price.Symbol,
		},
		map[string]interface{}{
			"price":    price.Price,
			"bid":      price.Bid,
			"ask":      price.Ask,
			"volume":   price.Volume,
			"sequence": price.Sequence,
		},
		price.Timestamp,
	)
	
	if err := ic.writeAPI.WritePoint(ctx, point); err != nil {
		return fmt.Errorf("failed to write price data: %w", err)
	}
	
	return nil
}

// WritePriceBatch writes multiple price data points
func (ic *InfluxClient) WritePriceBatch(ctx context.Context, prices []*models.PriceData) error {
	points := make([]*write.Point, 0, len(prices))
	
	for _, price := range prices {
		// Use exchange from price data, default to "binance" if not set for backward compatibility
		exchange := price.Exchange
		if exchange == "" {
			exchange = "binance"
		}
		
		point := influxdb2.NewPoint(
			"trades",
			map[string]string{
				"exchange": exchange,
				"symbol":   price.Symbol,
			},
			map[string]interface{}{
				"price":    price.Price,
				"bid":      price.Bid,
				"ask":      price.Ask,
				"volume":   price.Volume,
				"sequence": price.Sequence,
			},
			price.Timestamp,
		)
		points = append(points, point)
	}
	
	if err := ic.writeAPI.WritePoint(ctx, points...); err != nil {
		return fmt.Errorf("failed to write price batch: %w", err)
	}
	
	return nil
}

// Bar data operations

// WriteBar writes OHLCV bar data
func (ic *InfluxClient) WriteBar(ctx context.Context, bar *models.Bar, resolution string) error {
	// Use "ohlcv" for 1m data, "ohlcv_<resolution>" for others
	measurement := "ohlcv"
	if resolution != "1m" && resolution != "" {
		measurement = fmt.Sprintf("ohlcv_%s", resolution)
	}
	
	point := influxdb2.NewPoint(
		measurement,
		map[string]string{
			"exchange": "binance",
			"symbol":   bar.Symbol,
		},
		map[string]interface{}{
			"open":        bar.Open,
			"high":        bar.High,
			"low":         bar.Low,
			"close":       bar.Close,
			"volume":      bar.Volume,
			"trade_count": bar.TradeCount,
		},
		bar.Timestamp,
	)
	
	if err := ic.writeAPI.WritePoint(ctx, point); err != nil {
		return fmt.Errorf("failed to write bar data: %w", err)
	}
	
	return nil
}

// WriteBars writes multiple OHLCV bars in an optimized batch
func (ic *InfluxClient) WriteBars(ctx context.Context, bars []*models.Bar, resolution string) error {
	if len(bars) == 0 {
		return nil
	}
	
	// Use "ohlcv" for 1m data, "ohlcv_<resolution>" for others
	measurement := "ohlcv"
	if resolution != "1m" && resolution != "" {
		measurement = fmt.Sprintf("ohlcv_%s", resolution)
	}
	
	// Use the blocking write API for better performance with large batches
	writeAPI := ic.client.WriteAPIBlocking(ic.org, ic.bucket)
	
	points := make([]*write.Point, 0, len(bars))
	for _, bar := range bars {
		point := influxdb2.NewPoint(
			measurement,
			map[string]string{
				"exchange": "binance",
				"symbol":   bar.Symbol,
			},
			map[string]interface{}{
				"open":        bar.Open,
				"high":        bar.High,
				"low":         bar.Low,
				"close":       bar.Close,
				"volume":      bar.Volume,
				"trade_count": bar.TradeCount,
			},
			bar.Timestamp,
		)
		points = append(points, point)
	}
	
	// Write all points in a single batch operation
	// This is much faster than writing points individually
	if err := writeAPI.WritePoint(ctx, points...); err != nil {
		return fmt.Errorf("failed to write bars batch (%d points): %w", len(points), err)
	}
	
	return nil
}

// GetBars retrieves OHLCV bars for a symbol (with smart aggregation from 1m data)
func (ic *InfluxClient) GetBars(ctx context.Context, symbol string, from, to time.Time, resolution string) ([]*models.Bar, error) {
	// If resolution is not 1m, try to aggregate from 1m data first
	if resolution != "1m" && resolution != "" {
		// Check if we have 1m data for this period
		hasData, err := ic.has1MinuteData(ctx, symbol, from, to)
		if err == nil && hasData {
			ic.logger.WithFields(logrus.Fields{
				"symbol": symbol,
				"resolution": resolution,
				"method": "aggregation",
			}).Debug("Using aggregation from 1m data")
			// Aggregate from 1m data
			return ic.aggregateFromOneMinute(ctx, symbol, from, to, resolution)
		}
	}
	
	// Fallback to direct query (for 1m or if aggregation not possible)
	measurement := "ohlcv"
	if resolution == "1m" {
		measurement = "ohlcv"
	} else {
		measurement = fmt.Sprintf("ohlcv_%s", resolution)
	}
	
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: %s, stop: %s)
			|> filter(fn: (r) => r._measurement == "%s")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "open" or r._field == "high" or r._field == "low" or r._field == "close" or r._field == "volume")
			|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
			|> sort(columns: ["_time"])
	`, ic.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), measurement, symbol)
	
	// Log the query for debugging
	ic.logger.WithFields(logrus.Fields{
		"measurement": measurement,
		"symbol":      symbol,
		"from":        from.Format(time.RFC3339),
		"to":          to.Format(time.RFC3339),
		"bucket":      ic.bucket,
		"query":       query,
	}).Debug("Executing InfluxDB query for bars")
	
	result, err := ic.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query bars: %w", err)
	}
	defer result.Close()
	
	bars := make([]*models.Bar, 0)
	for result.Next() {
		record := result.Record()
		
		bar := &models.Bar{
			Symbol:    symbol,
			Timestamp: record.Time(),
		}
		
		// Extract values
		if v, ok := record.Values()["open"].(float64); ok {
			bar.Open = v
		}
		if v, ok := record.Values()["high"].(float64); ok {
			bar.High = v
		}
		if v, ok := record.Values()["low"].(float64); ok {
			bar.Low = v
		}
		if v, ok := record.Values()["close"].(float64); ok {
			bar.Close = v
		}
		if v, ok := record.Values()["volume"].(float64); ok {
			bar.Volume = v
		}
		if v, ok := record.Values()["trade_count"].(int64); ok {
			bar.TradeCount = v
		}
		
		bars = append(bars, bar)
	}
	
	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}
	
	return bars, nil
}

// GetLatestPrice retrieves the latest price for a symbol
func (ic *InfluxClient) GetLatestPrice(ctx context.Context, symbol string) (*models.PriceData, error) {
	query := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -1m)
			|> filter(fn: (r) => r._measurement == "trades")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "price" or r._field == "bid" or r._field == "ask" or r._field == "volume")
			|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
			|> last()
	`, ic.bucket, symbol)
	
	result, err := ic.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest price: %w", err)
	}
	defer result.Close()
	
	if !result.Next() {
		return nil, nil // No data
	}
	
	record := result.Record()
	price := &models.PriceData{
		Symbol:    symbol,
		Timestamp: record.Time(),
	}
	
	// Extract values
	if v, ok := record.Values()["price"].(float64); ok {
		price.Price = v
	}
	if v, ok := record.Values()["bid"].(float64); ok {
		price.Bid = v
	}
	if v, ok := record.Values()["ask"].(float64); ok {
		price.Ask = v
	}
	if v, ok := record.Values()["volume"].(float64); ok {
		price.Volume = v
	}
	
	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}
	
	return price, nil
}

// Enigma data operations

// WriteEnigmaData writes Enigma indicator data
func (ic *InfluxClient) WriteEnigmaData(ctx context.Context, enigma *models.EnigmaData) error {
	point := influxdb2.NewPoint(
		"enigma",
		map[string]string{
			"symbol": enigma.Symbol,
		},
		map[string]interface{}{
			"level":     enigma.Level,
			"ath":       enigma.ATH,
			"atl":       enigma.ATL,
			"fib_0":     enigma.FibLevels.L0,
			"fib_23_6":  enigma.FibLevels.L236,
			"fib_38_2":  enigma.FibLevels.L382,
			"fib_50":    enigma.FibLevels.L50,
			"fib_61_8":  enigma.FibLevels.L618,
			"fib_78_6":  enigma.FibLevels.L786,
			"fib_100":   enigma.FibLevels.L100,
		},
		enigma.Timestamp,
	)
	
	if err := ic.writeAPI.WritePoint(ctx, point); err != nil {
		return fmt.Errorf("failed to write enigma data: %w", err)
	}
	
	return nil
}

// GetLatestBar retrieves the most recent bar for a symbol
func (ic *InfluxClient) GetLatestBar(ctx context.Context, symbol string, interval string) (*models.Bar, error) {
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -30d)
		|> filter(fn: (r) => r._measurement == "ohlcv")
		|> filter(fn: (r) => r.symbol == "%s")
		|> filter(fn: (r) => r._field == "close")
		|> last()
	`, ic.bucket, symbol)
	
	result, err := ic.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest bar: %w", err)
	}
	defer result.Close()
	
	if !result.Next() {
		return nil, nil // No data exists
	}
	
	record := result.Record()
	timestamp := record.Time()
	
	// Get the actual OHLCV values for this timestamp
	barQuery := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "ohlcv")
		|> filter(fn: (r) => r.symbol == "%s")
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
	`, ic.bucket, 
		timestamp.Add(-time.Minute).Format(time.RFC3339),
		timestamp.Add(time.Minute).Format(time.RFC3339),
		symbol)
	
	barResult, err := ic.queryAPI.Query(ctx, barQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query bar details: %w", err)
	}
	defer barResult.Close()
	
	if !barResult.Next() {
		return nil, nil
	}
	
	barRecord := barResult.Record()
	bar := &models.Bar{
		Symbol:    symbol,
		Timestamp: timestamp,
	}
	
	if v, ok := barRecord.Values()["open"].(float64); ok {
		bar.Open = v
	}
	if v, ok := barRecord.Values()["high"].(float64); ok {
		bar.High = v
	}
	if v, ok := barRecord.Values()["low"].(float64); ok {
		bar.Low = v
	}
	if v, ok := barRecord.Values()["close"].(float64); ok {
		bar.Close = v
	}
	if v, ok := barRecord.Values()["volume"].(float64); ok {
		bar.Volume = v
	}
	
	return bar, nil
}

// GetDataTimeRange retrieves the earliest and latest timestamps for a symbol
func (ic *InfluxClient) GetDataTimeRange(ctx context.Context, symbol string) (earliest, latest time.Time, count int64, err error) {
	// Query for earliest timestamp
	earliestQuery := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -1000d)
		|> filter(fn: (r) => r._measurement == "ohlcv")
		|> filter(fn: (r) => r.symbol == "%s")
		|> filter(fn: (r) => r._field == "close")
		|> first()
	`, ic.bucket, symbol)
	
	earliestResult, err := ic.queryAPI.Query(ctx, earliestQuery)
	if err != nil {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("failed to query earliest: %w", err)
	}
	defer earliestResult.Close()
	
	if earliestResult.Next() {
		earliest = earliestResult.Record().Time()
	}
	
	// Query for latest timestamp
	latestQuery := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -1000d)
		|> filter(fn: (r) => r._measurement == "ohlcv")
		|> filter(fn: (r) => r.symbol == "%s")
		|> filter(fn: (r) => r._field == "close")
		|> last()
	`, ic.bucket, symbol)
	
	latestResult, err := ic.queryAPI.Query(ctx, latestQuery)
	if err != nil {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("failed to query latest: %w", err)
	}
	defer latestResult.Close()
	
	if latestResult.Next() {
		latest = latestResult.Record().Time()
	}
	
	// Count total data points
	countQuery := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: -1000d)
		|> filter(fn: (r) => r._measurement == "ohlcv")
		|> filter(fn: (r) => r.symbol == "%s")
		|> filter(fn: (r) => r._field == "close")
		|> count()
	`, ic.bucket, symbol)
	
	countResult, err := ic.queryAPI.Query(ctx, countQuery)
	if err != nil {
		return earliest, latest, 0, fmt.Errorf("failed to query count: %w", err)
	}
	defer countResult.Close()
	
	if countResult.Next() {
		if v, ok := countResult.Record().Value().(int64); ok {
			count = v
		}
	}
	
	return earliest, latest, count, nil
}

// GetATHATL retrieves all-time high and low for a symbol
func (ic *InfluxClient) GetATHATL(ctx context.Context, symbol string) (ath, atl float64, err error) {
	// Query for ATH - check both ohlcv and ohlcv_1d measurements
	athQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -10y)
			|> filter(fn: (r) => r._measurement == "ohlcv" or r._measurement == "ohlcv_1d")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "high")
			|> max()
	`, ic.bucket, symbol)
	
	result, err := ic.queryAPI.Query(ctx, athQuery)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query ATH: %w", err)
	}
	
	if result.Next() {
		if v, ok := result.Record().Value().(float64); ok {
			ath = v
		}
	}
	result.Close()
	
	// Query for ATL - check both ohlcv and ohlcv_1d measurements
	atlQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -10y)
			|> filter(fn: (r) => r._measurement == "ohlcv" or r._measurement == "ohlcv_1d")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "low")
			|> min()
	`, ic.bucket, symbol)
	
	result, err = ic.queryAPI.Query(ctx, atlQuery)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query ATL: %w", err)
	}
	
	if result.Next() {
		if v, ok := result.Record().Value().(float64); ok {
			atl = v
		}
	}
	result.Close()
	
	return ath, atl, nil
}

// GetPeriodHighLow retrieves high and low for a specific time period
func (ic *InfluxClient) GetPeriodHighLow(ctx context.Context, symbol string, start, end time.Time) (high, low, open, close float64, err error) {
	// Query for high
	highQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: %s, stop: %s)
			|> filter(fn: (r) => r._measurement == "ohlcv" or r._measurement == "ohlcv_1d")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "high")
			|> max()
	`, ic.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), symbol)
	
	result, err := ic.queryAPI.Query(ctx, highQuery)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to query period high: %w", err)
	}
	
	if result.Next() {
		if v, ok := result.Record().Value().(float64); ok {
			high = v
		}
	}
	result.Close()
	
	// Query for low
	lowQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: %s, stop: %s)
			|> filter(fn: (r) => r._measurement == "ohlcv" or r._measurement == "ohlcv_1d")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "low")
			|> min()
	`, ic.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), symbol)
	
	result, err = ic.queryAPI.Query(ctx, lowQuery)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("failed to query period low: %w", err)
	}
	
	if result.Next() {
		if v, ok := result.Record().Value().(float64); ok {
			low = v
		}
	}
	result.Close()
	
	// Query for open (first close price in the period)
	openQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: %s, stop: %s)
			|> filter(fn: (r) => r._measurement == "ohlcv" or r._measurement == "ohlcv_1d")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "open")
			|> first()
	`, ic.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), symbol)
	
	result, err = ic.queryAPI.Query(ctx, openQuery)
	if err != nil {
		return high, low, 0, 0, nil // Open/close are optional
	}
	
	if result.Next() {
		if v, ok := result.Record().Value().(float64); ok {
			open = v
		}
	}
	result.Close()
	
	// Query for close (last close price in the period)
	closeQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: %s, stop: %s)
			|> filter(fn: (r) => r._measurement == "ohlcv" or r._measurement == "ohlcv_1d")
			|> filter(fn: (r) => r.symbol == "%s")
			|> filter(fn: (r) => r._field == "close")
			|> last()
	`, ic.bucket, start.Format(time.RFC3339), end.Format(time.RFC3339), symbol)
	
	result, err = ic.queryAPI.Query(ctx, closeQuery)
	if err != nil {
		return high, low, open, 0, nil // Close is optional
	}
	
	if result.Next() {
		if v, ok := result.Record().Value().(float64); ok {
			close = v
		}
	}
	result.Close()
	
	return high, low, open, close, nil
}

// GetMultiplePeriodHighLows retrieves high/low for multiple periods efficiently
func (ic *InfluxClient) GetMultiplePeriodHighLows(ctx context.Context, symbol string, periods map[string][]time.Time) (map[string]struct{ High, Low, Open, Close float64 }, error) {
	results := make(map[string]struct{ High, Low, Open, Close float64 })
	
	for period, times := range periods {
		if len(times) != 2 {
			continue
		}
		
		high, low, open, close, err := ic.GetPeriodHighLow(ctx, symbol, times[0], times[1])
		if err != nil {
			ic.logger.WithError(err).WithField("period", period).Warn("Failed to get period high/low")
			continue
		}
		
		results[period] = struct{ High, Low, Open, Close float64 }{
			High:  high,
			Low:   low,
			Open:  open,
			Close: close,
		}
	}
	
	return results, nil
}

// has1MinuteData checks if we have 1-minute data for the given period
func (ic *InfluxClient) has1MinuteData(ctx context.Context, symbol string, from, to time.Time) (bool, error) {
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "ohlcv")
		|> filter(fn: (r) => r.symbol == "%s")
		|> filter(fn: (r) => r._field == "close")
		|> count()
	`, ic.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), symbol)
	
	result, err := ic.queryAPI.Query(ctx, query)
	if err != nil {
		return false, err
	}
	defer result.Close()
	
	if result.Next() {
		if count, ok := result.Record().Value().(int64); ok && count > 0 {
			return true, nil
		}
	}
	
	return false, nil
}

// aggregateFromOneMinute aggregates 1-minute data to higher timeframes
func (ic *InfluxClient) aggregateFromOneMinute(ctx context.Context, symbol string, from, to time.Time, resolution string) ([]*models.Bar, error) {
	// Get aggregation window in minutes
	windowMinutes := ic.getAggregationWindow(resolution)
	if windowMinutes == 0 {
		return nil, fmt.Errorf("unsupported resolution: %s", resolution)
	}
	
	// Fetch 1-minute data
	query := fmt.Sprintf(`
		from(bucket: "%s")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "ohlcv")
		|> filter(fn: (r) => r.symbol == "%s")
		|> filter(fn: (r) => r._field == "open" or r._field == "high" or r._field == "low" or r._field == "close" or r._field == "volume")
		|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
		|> sort(columns: ["_time"])
	`, ic.bucket, from.Format(time.RFC3339), to.Format(time.RFC3339), symbol)
	
	result, err := ic.queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query 1m data for aggregation: %w", err)
	}
	defer result.Close()
	
	// Collect all 1-minute bars
	var oneMinuteBars []*models.Bar
	for result.Next() {
		record := result.Record()
		bar := &models.Bar{
			Symbol:    symbol,
			Timestamp: record.Time(),
		}
		
		if v, ok := record.Values()["open"].(float64); ok {
			bar.Open = v
		}
		if v, ok := record.Values()["high"].(float64); ok {
			bar.High = v
		}
		if v, ok := record.Values()["low"].(float64); ok {
			bar.Low = v
		}
		if v, ok := record.Values()["close"].(float64); ok {
			bar.Close = v
		}
		if v, ok := record.Values()["volume"].(float64); ok {
			bar.Volume = v
		}
		
		oneMinuteBars = append(oneMinuteBars, bar)
	}
	
	if result.Err() != nil {
		return nil, fmt.Errorf("query error: %w", result.Err())
	}
	
	// Aggregate to target resolution
	return ic.aggregateBars(oneMinuteBars, windowMinutes), nil
}

// getAggregationWindow returns the window size in minutes for aggregation
func (ic *InfluxClient) getAggregationWindow(resolution string) int {
	switch resolution {
	case "3m":
		return 3
	case "5m":
		return 5
	case "15m":
		return 15
	case "30m":
		return 30
	case "1h":
		return 60
	case "2h":
		return 120
	case "4h":
		return 240
	case "6h":
		return 360
	case "8h":
		return 480
	case "12h":
		return 720
	case "1d", "1D":
		return 1440
	case "3d", "3D":
		return 4320
	case "1w", "1W":
		return 10080
	default:
		return 0
	}
}

// aggregateBars aggregates 1-minute bars into higher timeframe bars
func (ic *InfluxClient) aggregateBars(oneMinuteBars []*models.Bar, windowMinutes int) []*models.Bar {
	if len(oneMinuteBars) == 0 {
		return []*models.Bar{}
	}
	
	var aggregatedBars []*models.Bar
	var currentBars []*models.Bar
	var windowStart time.Time
	
	for _, bar := range oneMinuteBars {
		// Align to window boundary
		barMinute := bar.Timestamp.Truncate(time.Duration(windowMinutes) * time.Minute)
		
		// Start new window if needed
		if windowStart.IsZero() || barMinute.After(windowStart) {
			// Process previous window
			if len(currentBars) > 0 {
				aggregatedBar := ic.combineBarWindow(currentBars, windowStart)
				aggregatedBars = append(aggregatedBars, aggregatedBar)
			}
			
			// Start new window
			windowStart = barMinute
			currentBars = []*models.Bar{bar}
		} else {
			// Add to current window
			currentBars = append(currentBars, bar)
		}
	}
	
	// Process last window
	if len(currentBars) > 0 {
		aggregatedBar := ic.combineBarWindow(currentBars, windowStart)
		aggregatedBars = append(aggregatedBars, aggregatedBar)
	}
	
	return aggregatedBars
}

// combineBarWindow combines multiple bars into a single bar
func (ic *InfluxClient) combineBarWindow(bars []*models.Bar, timestamp time.Time) *models.Bar {
	if len(bars) == 0 {
		return nil
	}
	
	aggregated := &models.Bar{
		Symbol:    bars[0].Symbol,
		Timestamp: timestamp,
		Open:      bars[0].Open,                    // First bar's open
		Close:     bars[len(bars)-1].Close,         // Last bar's close
		High:      bars[0].High,                    // Initialize with first
		Low:       bars[0].Low,                     // Initialize with first
		Volume:    0,                                // Sum all volumes
	}
	
	// Find high, low, and sum volume
	for _, bar := range bars {
		if bar.High > aggregated.High {
			aggregated.High = bar.High
		}
		if bar.Low < aggregated.Low {
			aggregated.Low = bar.Low
		}
		aggregated.Volume += bar.Volume
	}
	
	return aggregated
}