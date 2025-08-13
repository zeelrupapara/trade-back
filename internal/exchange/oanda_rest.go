package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/config"
	"github.com/trade-back/pkg/models"
)

// OANDARESTClient handles OANDA REST API operations
type OANDARESTClient struct {
	config     *config.OANDAConfig
	httpClient *http.Client
	logger     *logrus.Entry
}

// OANDAInstrument represents an OANDA instrument
type OANDAInstrument struct {
	Name                        string  `json:"name"`
	Type                        string  `json:"type"`
	DisplayName                 string  `json:"displayName"`
	PipLocation                 int     `json:"pipLocation"`
	DisplayPrecision            int     `json:"displayPrecision"`
	TradeUnitsPrecision         int     `json:"tradeUnitsPrecision"`
	MinimumTradeSize            string  `json:"minimumTradeSize"`
	MaximumTrailingStopDistance string  `json:"maximumTrailingStopDistance"`
	MinimumTrailingStopDistance string  `json:"minimumTrailingStopDistance"`
	MaximumPositionSize         string  `json:"maximumPositionSize"`
	MaximumOrderUnits           string  `json:"maximumOrderUnits"`
	MarginRate                  string  `json:"marginRate"`
}

// OANDACandle represents an OANDA candlestick
type OANDACandle struct {
	Complete bool      `json:"complete"`
	Volume   int       `json:"volume"`
	Time     time.Time `json:"time"`
	Mid      *OANDAPriceData `json:"mid,omitempty"`
	Bid      *OANDAPriceData `json:"bid,omitempty"`
	Ask      *OANDAPriceData `json:"ask,omitempty"`
}

// OANDAPriceData represents OANDA OHLC price data
type OANDAPriceData struct {
	Open  string `json:"o"`
	High  string `json:"h"`
	Low   string `json:"l"`
	Close string `json:"c"`
}

// OANDAPrice represents current pricing information
type OANDAPrice struct {
	Instrument string    `json:"instrument"`
	Time       time.Time `json:"time"`
	Bids       []OANDAQuote `json:"bids"`
	Asks       []OANDAQuote `json:"asks"`
	CloseoutBid string   `json:"closeoutBid"`
	CloseoutAsk string   `json:"closeoutAsk"`
	Status      string   `json:"status"`
	Tradeable   bool     `json:"tradeable"`
}

// OANDAQuote represents bid/ask quote
type OANDAQuote struct {
	Price     string `json:"price"`
	Liquidity int    `json:"liquidity"`
}

// Response structures
type InstrumentsResponse struct {
	Instruments []OANDAInstrument `json:"instruments"`
}

type CandlesResponse struct {
	Instrument  string        `json:"instrument"`
	Granularity string        `json:"granularity"`
	Candles     []OANDACandle `json:"candles"`
}

type PricingResponse struct {
	Prices []OANDAPrice `json:"prices"`
}

// NewOANDARESTClient creates a new OANDA REST client
func NewOANDARESTClient(config *config.OANDAConfig, logger *logrus.Logger) *OANDARESTClient {
	return &OANDARESTClient{
		config: config,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger.WithField("component", "oanda-rest"),
	}
}

// GetInstruments fetches all available instruments from OANDA
func (c *OANDARESTClient) GetInstruments(ctx context.Context) ([]OANDAInstrument, error) {
	url := fmt.Sprintf("%s/v3/accounts/%s/instruments", c.config.APIURL, c.config.AccountID)
	
	// Debug logging
	c.logger.WithFields(logrus.Fields{
		"api_url": c.config.APIURL,
		"account_id": c.config.AccountID,
		"has_api_key": c.config.APIKey != "",
		"environment": c.config.Environment,
	}).Debug("Making OANDA API request")
	
	req, err := c.createRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	var response InstrumentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	c.logger.WithField("count", len(response.Instruments)).Debug("Fetched OANDA instruments")
	return response.Instruments, nil
}

// GetCandles fetches historical candle data for an instrument
func (c *OANDARESTClient) GetCandles(ctx context.Context, instrument, granularity string, from, to time.Time, count int) ([]OANDACandle, error) {
	url := fmt.Sprintf("%s/v3/instruments/%s/candles", c.config.APIURL, instrument)
	
	// Build query parameters
	params := []string{}
	if granularity != "" {
		params = append(params, fmt.Sprintf("granularity=%s", granularity))
	}
	if !from.IsZero() {
		params = append(params, fmt.Sprintf("from=%s", from.UTC().Format(time.RFC3339)))
	}
	if !to.IsZero() {
		params = append(params, fmt.Sprintf("to=%s", to.UTC().Format(time.RFC3339)))
	}
	if count > 0 {
		params = append(params, fmt.Sprintf("count=%d", count))
	}
	
	// Always request mid prices for consistency
	params = append(params, "price=M")
	
	if len(params) > 0 {
		url += "?" + strings.Join(params, "&")
	}

	req, err := c.createRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	var response CandlesResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"instrument":   instrument,
		"granularity":  granularity,
		"count":        len(response.Candles),
	}).Debug("Fetched OANDA candles")

	return response.Candles, nil
}

// GetKlinesBatch fetches historical data in batches (similar to Binance client)
func (c *OANDARESTClient) GetKlinesBatch(ctx context.Context, instrument, interval string, startTime, endTime int64) ([]HistoricalKline, error) {
	// Convert interval to OANDA granularity
	granularity := c.intervalToGranularity(interval)
	if granularity == "" {
		return nil, fmt.Errorf("unsupported interval: %s", interval)
	}

	from := time.Unix(startTime/1000, (startTime%1000)*1e6)
	to := time.Unix(endTime/1000, (endTime%1000)*1e6)

	candles, err := c.GetCandles(ctx, instrument, granularity, from, to, 0)
	if err != nil {
		return nil, err
	}

	// Convert OANDA candles to HistoricalKline format
	var klines []HistoricalKline
	for _, candle := range candles {
		if !candle.Complete || candle.Mid == nil {
			continue
		}

		kline := HistoricalKline{
			OpenTime:  candle.Time.UnixMilli(),
			CloseTime: candle.Time.Add(c.granularityToDuration(granularity)).UnixMilli() - 1,
			Open:      candle.Mid.Open,
			High:      candle.Mid.High,
			Low:       candle.Mid.Low,
			Close:     candle.Mid.Close,
			Volume:    strconv.Itoa(candle.Volume),
			NumberOfTrades: candle.Volume, // Use volume as trade count approximation
		}
		klines = append(klines, kline)
	}

	return klines, nil
}

// GetCurrentPrices fetches current prices for instruments
func (c *OANDARESTClient) GetCurrentPrices(ctx context.Context, instruments []string) ([]OANDAPrice, error) {
	if len(instruments) == 0 {
		return nil, fmt.Errorf("no instruments provided")
	}

	url := fmt.Sprintf("%s/v3/accounts/%s/pricing", c.config.APIURL, c.config.AccountID)
	url += "?instruments=" + strings.Join(instruments, ",")

	req, err := c.createRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	var response PricingResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.Prices, nil
}

// ConvertToTradeBackSymbols converts OANDA instruments to trade-back symbol format
func (c *OANDARESTClient) ConvertToTradeBackSymbols(instruments []OANDAInstrument) []models.SymbolInfo {
	var symbols []models.SymbolInfo
	
	for _, instrument := range instruments {
		// Skip non-currency instruments for now
		if instrument.Type != "CURRENCY" {
			continue
		}

		// Parse currency pair (e.g., "EUR_USD" -> "EUR", "USD")
		parts := strings.Split(instrument.Name, "_")
		if len(parts) != 2 {
			continue
		}

		symbol := models.SymbolInfo{
			Exchange:        "oanda",
			Symbol:          instrument.Name,
			FullName:        instrument.DisplayName,
			InstrumentType:  "FOREX",
			BaseCurrency:    parts[0],
			QuoteCurrency:   parts[1],
			IsActive:        true,
			MinPriceIncrement:   c.calculateMinPriceIncrement(instrument.PipLocation, instrument.DisplayPrecision),
			MinQuantityIncrement: c.parseMinTradeSize(instrument.MinimumTradeSize),
		}
		
		symbols = append(symbols, symbol)
	}

	return symbols
}

// Helper functions

func (c *OANDARESTClient) createRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	return req, nil
}

func (c *OANDARESTClient) intervalToGranularity(interval string) string {
	switch interval {
	case "5s":
		return "S5"
	case "10s":
		return "S10"
	case "15s":
		return "S15"
	case "30s":
		return "S30"
	case "1m":
		return "M1"
	case "2m":
		return "M2"
	case "4m":
		return "M4"
	case "5m":
		return "M5"
	case "10m":
		return "M10"
	case "15m":
		return "M15"
	case "30m":
		return "M30"
	case "1h":
		return "H1"
	case "2h":
		return "H2"
	case "3h":
		return "H3"
	case "4h":
		return "H4"
	case "6h":
		return "H6"
	case "8h":
		return "H8"
	case "12h":
		return "H12"
	case "1d":
		return "D"
	case "1w":
		return "W"
	case "1M":
		return "M"
	default:
		return ""
	}
}

func (c *OANDARESTClient) granularityToDuration(granularity string) time.Duration {
	switch granularity {
	case "S5":
		return 5 * time.Second
	case "S10":
		return 10 * time.Second
	case "S15":
		return 15 * time.Second
	case "S30":
		return 30 * time.Second
	case "M1":
		return time.Minute
	case "M2":
		return 2 * time.Minute
	case "M4":
		return 4 * time.Minute
	case "M5":
		return 5 * time.Minute
	case "M10":
		return 10 * time.Minute
	case "M15":
		return 15 * time.Minute
	case "M30":
		return 30 * time.Minute
	case "H1":
		return time.Hour
	case "H2":
		return 2 * time.Hour
	case "H3":
		return 3 * time.Hour
	case "H4":
		return 4 * time.Hour
	case "H6":
		return 6 * time.Hour
	case "H8":
		return 8 * time.Hour
	case "H12":
		return 12 * time.Hour
	case "D":
		return 24 * time.Hour
	case "W":
		return 7 * 24 * time.Hour
	case "M":
		return 30 * 24 * time.Hour // Approximate
	default:
		return time.Minute
	}
}

func (c *OANDARESTClient) calculateMinPriceIncrement(pipLocation, displayPrecision int) float64 {
	if pipLocation == 0 {
		return 0.0001 // Default pip size
	}
	
	// Calculate pip value based on pip location
	pipValue := 1.0
	for i := 0; i < pipLocation; i++ {
		pipValue /= 10.0
	}
	return pipValue
}

func (c *OANDARESTClient) parseMinTradeSize(minTradeSize string) float64 {
	if minTradeSize == "" {
		return 1.0 // Default minimum trade size
	}
	
	size, err := strconv.ParseFloat(minTradeSize, 64)
	if err != nil {
		return 1.0
	}
	return size
}