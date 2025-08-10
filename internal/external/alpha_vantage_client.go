package external

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/trade-back/pkg/models"
)

// AlphaVantageClient handles Alpha Vantage API interactions for forex and stocks
type AlphaVantageClient struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string
	logger     *logrus.Entry
	
	// Rate limiting (5 calls per minute for free tier)
	rateLimiter chan struct{}
	
	// Cache for extremes
	extremeCache map[string]*CachedExtreme
	cacheMutex   sync.RWMutex
	cacheTTL     time.Duration
}

// CachedExtreme represents cached extreme values
type CachedExtreme struct {
	Extreme   *models.AssetExtreme
	CachedAt  time.Time
}

// AlphaVantageQuote represents a quote response
type AlphaVantageQuote struct {
	GlobalQuote struct {
		Symbol           string `json:"01. symbol"`
		Price            string `json:"05. price"`
		Volume           string `json:"06. volume"`
		LatestTradingDay string `json:"07. latest trading day"`
		PreviousClose    string `json:"08. previous close"`
		Change           string `json:"09. change"`
		ChangePercent    string `json:"10. change percent"`
	} `json:"Global Quote"`
}

// AlphaVantageOverview represents company overview with 52-week data
type AlphaVantageOverview struct {
	Symbol          string `json:"Symbol"`
	Name            string `json:"Name"`
	Exchange        string `json:"Exchange"`
	Currency        string `json:"Currency"`
	Week52High      string `json:"52WeekHigh"`
	Week52Low       string `json:"52WeekLow"`
	MarketCap       string `json:"MarketCapitalization"`
	PERatio         string `json:"PERatio"`
	DividendYield   string `json:"DividendYield"`
}

// AlphaVantageTimeSeries represents historical data
type AlphaVantageTimeSeries struct {
	MetaData struct {
		Symbol       string `json:"2. Symbol"`
		LastRefreshed string `json:"3. Last Refreshed"`
	} `json:"Meta Data"`
	TimeSeries map[string]struct {
		Open   string `json:"1. open"`
		High   string `json:"2. high"`
		Low    string `json:"3. low"`
		Close  string `json:"4. close"`
		Volume string `json:"5. volume"`
	} `json:"Time Series (Daily)"`
}

// ForexTimeSeries for forex pairs
type ForexTimeSeries struct {
	MetaData struct {
		FromSymbol string `json:"2. From Symbol"`
		ToSymbol   string `json:"3. To Symbol"`
	} `json:"Meta Data"`
	TimeSeries map[string]struct {
		Open  string `json:"1. open"`
		High  string `json:"2. high"`
		Low   string `json:"3. low"`
		Close string `json:"4. close"`
	} `json:"Time Series FX (Daily)"`
}

// NewAlphaVantageClient creates a new Alpha Vantage client
func NewAlphaVantageClient(apiKey string, logger *logrus.Logger) *AlphaVantageClient {
	client := &AlphaVantageClient{
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		baseURL:      "https://www.alphavantage.co/query",
		apiKey:       apiKey,
		logger:       logger.WithField("component", "alpha-vantage"),
		rateLimiter:  make(chan struct{}, 1),
		extremeCache: make(map[string]*CachedExtreme),
		cacheTTL:     1 * time.Hour, // Cache for 1 hour
	}
	
	// Start rate limiter (5 calls per minute = 1 call per 12 seconds)
	go client.rateLimitWorker()
	
	return client
}

// rateLimitWorker ensures we don't exceed rate limits
func (c *AlphaVantageClient) rateLimitWorker() {
	ticker := time.NewTicker(12 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		select {
		case c.rateLimiter <- struct{}{}:
		default:
		}
	}
}

// GetStockExtremes fetches 52-week high/low for a stock
func (c *AlphaVantageClient) GetStockExtremes(ctx context.Context, symbol string) (*models.AssetExtreme, error) {
	// Check cache first
	c.cacheMutex.RLock()
	if cached, exists := c.extremeCache[symbol]; exists {
		if time.Since(cached.CachedAt) < c.cacheTTL {
			c.cacheMutex.RUnlock()
			return cached.Extreme, nil
		}
	}
	c.cacheMutex.RUnlock()
	
	// Rate limit
	select {
	case <-c.rateLimiter:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	
	// Fetch company overview (contains 52-week data)
	url := fmt.Sprintf("%s?function=OVERVIEW&symbol=%s&apikey=%s",
		c.baseURL, symbol, c.apiKey)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}
	
	var overview AlphaVantageOverview
	if err := json.NewDecoder(resp.Body).Decode(&overview); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	
	// Parse 52-week high/low
	week52High, _ := strconv.ParseFloat(overview.Week52High, 64)
	week52Low, _ := strconv.ParseFloat(overview.Week52Low, 64)
	
	// For stocks, we'll use historical data to find all-time extremes
	// This requires another API call for historical data
	ath, atl := c.getHistoricalExtremes(ctx, symbol)
	
	extreme := &models.AssetExtreme{
		Symbol:      symbol,
		AssetClass:  models.AssetClassStock,
		ATH:         ath,
		ATL:         atl,
		Week52High:  week52High,
		Week52Low:   week52Low,
		DataSource:  "alpha_vantage",
		LastUpdated: time.Now().Unix(),
	}
	
	// Update cache
	c.cacheMutex.Lock()
	c.extremeCache[symbol] = &CachedExtreme{
		Extreme:  extreme,
		CachedAt: time.Now(),
	}
	c.cacheMutex.Unlock()
	
	c.logger.WithFields(logrus.Fields{
		"symbol":      symbol,
		"ath":         ath,
		"atl":         atl,
		"52_week_high": week52High,
		"52_week_low":  week52Low,
	}).Debug("Fetched stock extremes from Alpha Vantage")
	
	return extreme, nil
}

// GetForexExtremes fetches historical extremes for a forex pair
func (c *AlphaVantageClient) GetForexExtremes(ctx context.Context, pair string) (*models.AssetExtreme, error) {
	// Check cache
	c.cacheMutex.RLock()
	if cached, exists := c.extremeCache[pair]; exists {
		if time.Since(cached.CachedAt) < c.cacheTTL {
			c.cacheMutex.RUnlock()
			return cached.Extreme, nil
		}
	}
	c.cacheMutex.RUnlock()
	
	// Parse pair (e.g., EURUSD -> EUR, USD)
	if len(pair) != 6 {
		return nil, fmt.Errorf("invalid forex pair: %s", pair)
	}
	fromSymbol := pair[:3]
	toSymbol := pair[3:]
	
	// Rate limit
	select {
	case <-c.rateLimiter:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	
	// Fetch daily forex data
	url := fmt.Sprintf("%s?function=FX_DAILY&from_symbol=%s&to_symbol=%s&outputsize=full&apikey=%s",
		c.baseURL, fromSymbol, toSymbol, c.apiKey)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}
	
	var timeSeries ForexTimeSeries
	if err := json.NewDecoder(resp.Body).Decode(&timeSeries); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	
	// Calculate extremes from time series
	var ath, atl float64
	var athDate, atlDate string
	first := true
	
	for date, data := range timeSeries.TimeSeries {
		high, _ := strconv.ParseFloat(data.High, 64)
		low, _ := strconv.ParseFloat(data.Low, 64)
		
		if first {
			ath = high
			atl = low
			athDate = date
			atlDate = date
			first = false
		} else {
			if high > ath {
				ath = high
				athDate = date
			}
			if low < atl {
				atl = low
				atlDate = date
			}
		}
	}
	
	extreme := &models.AssetExtreme{
		Symbol:      pair,
		AssetClass:  models.AssetClassForex,
		ATH:         ath,
		ATL:         atl,
		ATHDate:     athDate,
		ATLDate:     atlDate,
		DataSource:  "alpha_vantage",
		LastUpdated: time.Now().Unix(),
	}
	
	// Update cache
	c.cacheMutex.Lock()
	c.extremeCache[pair] = &CachedExtreme{
		Extreme:  extreme,
		CachedAt: time.Now(),
	}
	c.cacheMutex.Unlock()
	
	c.logger.WithFields(logrus.Fields{
		"pair":     pair,
		"ath":      ath,
		"atl":      atl,
		"ath_date": athDate,
		"atl_date": atlDate,
	}).Debug("Fetched forex extremes from Alpha Vantage")
	
	return extreme, nil
}

// getHistoricalExtremes fetches all-time extremes from historical data
func (c *AlphaVantageClient) getHistoricalExtremes(ctx context.Context, symbol string) (ath, atl float64) {
	// For now, we'll use weekly data (more history with fewer points)
	url := fmt.Sprintf("%s?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=%s&apikey=%s",
		c.baseURL, symbol, c.apiKey)
	
	// Create a new context with timeout for this sub-request
	subCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	
	req, err := http.NewRequestWithContext(subCtx, "GET", url, nil)
	if err != nil {
		c.logger.WithError(err).Warn("Failed to fetch historical extremes")
		return 0, 0
	}
	
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.WithError(err).Warn("Failed to fetch historical extremes")
		return 0, 0
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return 0, 0
	}
	
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, 0
	}
	
	// Parse weekly time series
	if weeklyData, ok := data["Weekly Adjusted Time Series"].(map[string]interface{}); ok {
		first := true
		for _, dayData := range weeklyData {
			if day, ok := dayData.(map[string]interface{}); ok {
				if highStr, ok := day["2. high"].(string); ok {
					if high, err := strconv.ParseFloat(highStr, 64); err == nil {
						if first || high > ath {
							ath = high
							first = false
						}
					}
				}
				if lowStr, ok := day["3. low"].(string); ok {
					if low, err := strconv.ParseFloat(lowStr, 64); err == nil {
						if first || low < atl {
							atl = low
						}
					}
				}
			}
		}
	}
	
	return ath, atl
}

// GetQuote fetches current quote for a symbol
func (c *AlphaVantageClient) GetQuote(ctx context.Context, symbol string) (float64, error) {
	// Rate limit
	select {
	case <-c.rateLimiter:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	
	url := fmt.Sprintf("%s?function=GLOBAL_QUOTE&symbol=%s&apikey=%s",
		c.baseURL, symbol, c.apiKey)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("API returned status %d", resp.StatusCode)
	}
	
	var quote AlphaVantageQuote
	if err := json.NewDecoder(resp.Body).Decode(&quote); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}
	
	price, err := strconv.ParseFloat(quote.GlobalQuote.Price, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse price: %w", err)
	}
	
	return price, nil
}

// IsSupported checks if a symbol is supported by Alpha Vantage
func (c *AlphaVantageClient) IsSupported(symbol string, assetClass models.AssetClass) bool {
	switch assetClass {
	case models.AssetClassStock:
		// Most US stocks are supported
		// Could implement a symbol search API call here
		return true
	case models.AssetClassForex:
		// Check if it's a valid forex pair
		if len(symbol) == 6 {
			from := symbol[:3]
			to := symbol[3:]
			return c.isValidCurrency(from) && c.isValidCurrency(to)
		}
		return false
	default:
		return false
	}
}

// isValidCurrency checks if a currency code is valid
func (c *AlphaVantageClient) isValidCurrency(code string) bool {
	validCurrencies := []string{
		"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
		"CNY", "INR", "KRW", "SGD", "HKD", "NOK", "SEK", "DKK",
		"PLN", "THB", "IDR", "HUF", "CZK", "ILS", "CLP", "PHP",
		"AED", "COP", "SAR", "MYR", "RON", "RUB", "ZAR", "MXN",
		"BRL", "TWD", "TRY",
	}
	
	for _, currency := range validCurrencies {
		if currency == code {
			return true
		}
	}
	return false
}

// ClearCache clears the extreme cache
func (c *AlphaVantageClient) ClearCache() {
	c.cacheMutex.Lock()
	c.extremeCache = make(map[string]*CachedExtreme)
	c.cacheMutex.Unlock()
}