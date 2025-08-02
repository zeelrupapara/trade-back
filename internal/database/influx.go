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
		return fmt.Errorf("influxdb health check failed: %s", health.Message)
	}
	
	return nil
}

// Price data operations

// WritePriceData writes price data to InfluxDB
func (ic *InfluxClient) WritePriceData(ctx context.Context, price *models.PriceData) error {
	point := influxdb2.NewPoint(
		"trades",
		map[string]string{
			"exchange": "binance",
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
		point := influxdb2.NewPoint(
			"trades",
			map[string]string{
				"exchange": "binance",
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
	measurement := fmt.Sprintf("ohlcv_%s", resolution)
	
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

// GetBars retrieves OHLCV bars for a symbol
func (ic *InfluxClient) GetBars(ctx context.Context, symbol string, from, to time.Time, resolution string) ([]*models.Bar, error) {
	measurement := fmt.Sprintf("ohlcv_%s", resolution)
	
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

// GetATHATL retrieves all-time high and low for a symbol
func (ic *InfluxClient) GetATHATL(ctx context.Context, symbol string) (ath, atl float64, err error) {
	// Query for ATH
	athQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -10y)
			|> filter(fn: (r) => r._measurement == "ohlcv_1d")
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
	
	// Query for ATL
	atlQuery := fmt.Sprintf(`
		from(bucket: "%s")
			|> range(start: -10y)
			|> filter(fn: (r) => r._measurement == "ohlcv_1d")
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

// CreateContinuousQueries creates continuous queries for data aggregation
func (ic *InfluxClient) CreateContinuousQueries(ctx context.Context) error {
	// Note: InfluxDB 2.x uses Tasks instead of continuous queries
	// This is a placeholder for setting up aggregation tasks
	
	// Example task creation would be done via the InfluxDB UI or API
	// Tasks would aggregate tick data into 1m, 5m, 1h, 1d bars
	
	ic.logger.Info("Continuous queries should be set up via InfluxDB UI or API")
	return nil
}