package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/models"
)

// HistoricalKline represents a candlestick/kline data point from REST API
type HistoricalKline struct {
	OpenTime         int64
	Open             string
	High             string
	Low              string
	Close            string
	Volume           string
	CloseTime        int64
	QuoteAssetVolume string
	NumberOfTrades   int
	TakerBuyVolume   string
	TakerBuyQuoteVol string
}

// BinanceRESTClient handles REST API calls to Binance
type BinanceRESTClient struct {
	client    *http.Client
	baseURL   string
	logger    *logrus.Entry
	rateLimit time.Duration
	lastCall  time.Time
}

// NewBinanceRESTClient creates a new Binance REST API client
func NewBinanceRESTClient(logger *logrus.Logger) *BinanceRESTClient {
	return &BinanceRESTClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL:   "https://api.binance.com",
		logger:    logger.WithField("component", "binance-rest"),
		rateLimit: 100 * time.Millisecond, // 10 requests per second max
	}
}

// GetKlines fetches kline/candlestick data
func (b *BinanceRESTClient) GetKlines(ctx context.Context, symbol, interval string, startTime, endTime int64, limit int) ([]HistoricalKline, error) {
	// Rate limiting
	b.enforceRateLimit()

	// Build URL
	endpoint := fmt.Sprintf("%s/api/v3/klines", b.baseURL)
	params := url.Values{}
	params.Add("symbol", symbol)
	params.Add("interval", interval)
	
	if startTime > 0 {
		params.Add("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		params.Add("endTime", strconv.FormatInt(endTime, 10))
	}
	if limit > 0 && limit <= 1000 {
		params.Add("limit", strconv.Itoa(limit))
	} else if limit > 1000 {
		params.Add("limit", "1000")
	}

	fullURL := fmt.Sprintf("%s?%s", endpoint, params.Encode())
	
	b.logger.WithFields(logrus.Fields{
		"symbol":    symbol,
		"interval":  interval,
		"startTime": time.Unix(startTime/1000, 0).Format(time.RFC3339),
		"endTime":   time.Unix(endTime/1000, 0).Format(time.RFC3339),
		"limit":     limit,
	}).Debug("Fetching klines")

	// Create request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Execute request
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: status=%d, body=%s", resp.StatusCode, string(body))
	}

	// Parse response
	var rawKlines [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rawKlines); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Convert to HistoricalKline
	klines := make([]HistoricalKline, 0, len(rawKlines))
	for _, raw := range rawKlines {
		if len(raw) < 11 {
			continue
		}

		kline := HistoricalKline{
			OpenTime:         int64(raw[0].(float64)),
			Open:             raw[1].(string),
			High:             raw[2].(string),
			Low:              raw[3].(string),
			Close:            raw[4].(string),
			Volume:           raw[5].(string),
			CloseTime:        int64(raw[6].(float64)),
			QuoteAssetVolume: raw[7].(string),
			NumberOfTrades:   int(raw[8].(float64)),
			TakerBuyVolume:   raw[9].(string),
			TakerBuyQuoteVol: raw[10].(string),
		}
		klines = append(klines, kline)
	}

	b.logger.WithFields(logrus.Fields{
		"symbol": symbol,
		"count":  len(klines),
	}).Debug("Fetched klines successfully")

	return klines, nil
}

// GetKlinesBatch fetches klines in batches for large date ranges
func (b *BinanceRESTClient) GetKlinesBatch(ctx context.Context, symbol, interval string, startTime, endTime int64) ([]HistoricalKline, error) {
	var allKlines []HistoricalKline
	
	// Calculate interval duration in milliseconds
	intervalMs := b.getIntervalMilliseconds(interval)
	batchSize := int64(1000) // Max klines per request
	batchDuration := intervalMs * batchSize

	currentStart := startTime
	for currentStart < endTime {
		currentEnd := currentStart + batchDuration
		if currentEnd > endTime {
			currentEnd = endTime
		}

		// Fetch batch
		klines, err := b.GetKlines(ctx, symbol, interval, currentStart, currentEnd, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch batch: %w", err)
		}

		allKlines = append(allKlines, klines...)
		
		// Move to next batch
		if len(klines) > 0 {
			currentStart = klines[len(klines)-1].CloseTime + 1
		} else {
			currentStart = currentEnd
		}
	}

	return allKlines, nil
}

// GetSymbolPrice fetches current price for a symbol
func (b *BinanceRESTClient) GetSymbolPrice(ctx context.Context, symbol string) (float64, error) {
	b.enforceRateLimit()

	endpoint := fmt.Sprintf("%s/api/v3/ticker/price?symbol=%s", b.baseURL, symbol)
	
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("API error: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var result struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	price, err := strconv.ParseFloat(result.Price, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse price: %w", err)
	}

	return price, nil
}

// GetExchangeInfo fetches exchange information including all available symbols
func (b *BinanceRESTClient) GetExchangeInfo(ctx context.Context) (*BinanceExchangeInfo, error) {
	b.enforceRateLimit()

	endpoint := fmt.Sprintf("%s/api/v3/exchangeInfo", b.baseURL)
	
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var exchangeInfo BinanceExchangeInfo
	if err := json.NewDecoder(resp.Body).Decode(&exchangeInfo); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	b.logger.WithField("symbols", len(exchangeInfo.Symbols)).Debug("Fetched exchange info successfully")
	
	return &exchangeInfo, nil
}

// ConvertToTradeBackSymbols converts Binance symbol info to trade-back SymbolInfo format
func (b *BinanceRESTClient) ConvertToTradeBackSymbols(exchangeInfo *BinanceExchangeInfo) []models.SymbolInfo {
	symbols := make([]models.SymbolInfo, 0, len(exchangeInfo.Symbols))
	
	for _, binanceSymbol := range exchangeInfo.Symbols {
		// Only include active trading symbols that support spot trading
		if binanceSymbol.Status != "TRADING" || !binanceSymbol.IsSpotTradingAllowed {
			continue
		}
		
		symbol := models.SymbolInfo{
			Exchange:       "binance",
			Symbol:         binanceSymbol.Symbol,
			FullName:       fmt.Sprintf("%s/%s", binanceSymbol.BaseAsset, binanceSymbol.QuoteAsset),
			InstrumentType: "SPOT",
			BaseCurrency:   binanceSymbol.BaseAsset,
			QuoteCurrency:  binanceSymbol.QuoteAsset,
			IsActive:       true,
		}
		
		symbols = append(symbols, symbol)
	}
	
	b.logger.WithFields(logrus.Fields{
		"total_symbols":    len(exchangeInfo.Symbols),
		"filtered_symbols": len(symbols),
	}).Debug("Converted Binance symbols to trade-back format")
	
	return symbols
}

// enforceRateLimit ensures we don't exceed API rate limits
func (b *BinanceRESTClient) enforceRateLimit() {
	elapsed := time.Since(b.lastCall)
	if elapsed < b.rateLimit {
		time.Sleep(b.rateLimit - elapsed)
	}
	b.lastCall = time.Now()
}

// getIntervalMilliseconds returns the duration of an interval in milliseconds
func (b *BinanceRESTClient) getIntervalMilliseconds(interval string) int64 {
	switch interval {
	case "1m":
		return 60 * 1000
	case "3m":
		return 3 * 60 * 1000
	case "5m":
		return 5 * 60 * 1000
	case "15m":
		return 15 * 60 * 1000
	case "30m":
		return 30 * 60 * 1000
	case "1h":
		return 60 * 60 * 1000
	case "2h":
		return 2 * 60 * 60 * 1000
	case "4h":
		return 4 * 60 * 60 * 1000
	case "6h":
		return 6 * 60 * 60 * 1000
	case "8h":
		return 8 * 60 * 60 * 1000
	case "12h":
		return 12 * 60 * 60 * 1000
	case "1d":
		return 24 * 60 * 60 * 1000
	case "3d":
		return 3 * 24 * 60 * 60 * 1000
	case "1w":
		return 7 * 24 * 60 * 60 * 1000
	case "1M":
		return 30 * 24 * 60 * 60 * 1000
	default:
		return 60 * 1000 // Default to 1 minute
	}
}