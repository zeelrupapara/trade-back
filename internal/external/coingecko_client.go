package external

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// CoinGeckoClient handles CoinGecko API interactions
type CoinGeckoClient struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string
	logger     *logrus.Entry
	
	// Rate limiting
	rateLimiter chan struct{}
	
	// Symbol mapping cache
	symbolMap map[string]string // Binance symbol -> CoinGecko ID
	mapMutex  sync.RWMutex
}

// CoinGeckoMarketData represents market data from CoinGecko
type CoinGeckoMarketData struct {
	ID         string `json:"id"`
	Symbol     string `json:"symbol"`
	Name       string `json:"name"`
	MarketData struct {
		CurrentPrice map[string]float64 `json:"current_price"`
		ATH          map[string]float64 `json:"ath"`
		ATL          map[string]float64 `json:"atl"`
		ATHDate      map[string]string  `json:"ath_date"`
		ATLDate      map[string]string  `json:"atl_date"`
	} `json:"market_data"`
}

// NewCoinGeckoClient creates a new CoinGecko client
func NewCoinGeckoClient(apiKey string, logger *logrus.Logger) *CoinGeckoClient {
	client := &CoinGeckoClient{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL:     "https://api.coingecko.com/api/v3",
		apiKey:      apiKey,
		logger:      logger.WithField("component", "coingecko"),
		rateLimiter: make(chan struct{}, 1), // 1 request at a time
		symbolMap:   initializeSymbolMap(),
	}
	
	// Start rate limiter
	go client.rateLimitWorker()
	
	return client
}

// rateLimitWorker ensures we don't exceed rate limits (30 calls/min for free tier)
func (c *CoinGeckoClient) rateLimitWorker() {
	ticker := time.NewTicker(3 * time.Second) // Conservative: 20 calls/min to avoid 429 errors
	defer ticker.Stop()
	
	for range ticker.C {
		select {
		case c.rateLimiter <- struct{}{}:
		default:
		}
	}
}

// GetATHATL fetches all-time high and low for a symbol
func (c *CoinGeckoClient) GetATHATL(ctx context.Context, binanceSymbol string) (ath, atl float64, err error) {
	c.logger.WithField("symbol", binanceSymbol).Info("CoinGecko GetATHATL called")
	
	// Convert Binance symbol to CoinGecko ID
	coinID := c.getCoinGeckoID(binanceSymbol)
	if coinID == "" {
		c.logger.WithField("symbol", binanceSymbol).Warn("CoinGecko: unsupported symbol")
		return 0, 0, fmt.Errorf("unsupported symbol: %s", binanceSymbol)
	}
	
	c.logger.WithFields(logrus.Fields{
		"symbol": binanceSymbol,
		"coin_id": coinID,
	}).Info("CoinGecko: Converted symbol to ID")
	
	// Rate limit
	select {
	case <-c.rateLimiter:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	
	// Build request
	url := fmt.Sprintf("%s/coins/%s?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false", 
		c.baseURL, coinID)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create request: %w", err)
	}
	
	// Add API key if provided
	if c.apiKey != "" {
		req.Header.Set("x-cg-demo-api-key", c.apiKey)
	}
	
	// Execute request with retry logic for 429 errors
	resp, err := c.executeWithRetry(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	
	// Parse response
	var data CoinGeckoMarketData
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, 0, fmt.Errorf("failed to decode response: %w", err)
	}
	
	// Extract USD values
	ath = data.MarketData.ATH["usd"]
	atl = data.MarketData.ATL["usd"]
	
	c.logger.WithFields(logrus.Fields{
		"symbol": binanceSymbol,
		"coin_id": coinID,
		"ath": ath,
		"atl": atl,
	}).Debug("Fetched ATH/ATL from CoinGecko")
	
	return ath, atl, nil
}

// getCoinGeckoID converts Binance symbol to CoinGecko ID
func (c *CoinGeckoClient) getCoinGeckoID(binanceSymbol string) string {
	c.mapMutex.RLock()
	defer c.mapMutex.RUnlock()
	
	// Handle different trading pairs
	base := binanceSymbol
	
	// Remove common suffixes
	suffixes := []string{"USDT", "BUSD", "USDC", "BTC", "ETH", "BNB"}
	for _, suffix := range suffixes {
		if strings.HasSuffix(base, suffix) {
			base = strings.TrimSuffix(base, suffix)
			break
		}
	}
	
	base = strings.ToLower(base)
	
	if id, exists := c.symbolMap[base]; exists {
		return id
	}
	
	// Default mapping for common cases
	return base
}

// initializeSymbolMap creates mapping between Binance symbols and CoinGecko IDs
func initializeSymbolMap() map[string]string {
	return map[string]string{
		"btc":   "bitcoin",
		"eth":   "ethereum",
		"bnb":   "binancecoin",
		"xrp":   "ripple",
		"ada":   "cardano",
		"doge":  "dogecoin",
		"sol":   "solana",
		"dot":   "polkadot",
		"matic": "matic-network",
		"shib":  "shiba-inu",
		"avax":  "avalanche-2",
		"link":  "chainlink",
		"atom":  "cosmos",
		"ltc":   "litecoin",
		"uni":   "uniswap",
		"xlm":   "stellar",
		"vet":   "vechain",
		"icp":   "internet-computer",
		"fil":   "filecoin",
		"etc":   "ethereum-classic",
		"bnsol": "binance-peg-solana", // BNSOLUSDT
		// Add more mappings as needed
	}
}

// GetMarketData fetches complete market data including current price
func (c *CoinGeckoClient) GetMarketData(ctx context.Context, binanceSymbol string) (*CoinGeckoMarketData, error) {
	coinID := c.getCoinGeckoID(binanceSymbol)
	if coinID == "" {
		return nil, fmt.Errorf("unsupported symbol: %s", binanceSymbol)
	}
	
	// Rate limit
	select {
	case <-c.rateLimiter:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	
	url := fmt.Sprintf("%s/coins/%s?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false", 
		c.baseURL, coinID)
	
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	if c.apiKey != "" {
		req.Header.Set("x-cg-demo-api-key", c.apiKey)
	}
	
	resp, err := c.executeWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	
	var data CoinGeckoMarketData
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	
	return &data, nil
}

// UpdateSymbolMapping allows updating the symbol mapping at runtime
func (c *CoinGeckoClient) UpdateSymbolMapping(binanceSymbol, coinGeckoID string) {
	c.mapMutex.Lock()
	defer c.mapMutex.Unlock()
	
	base := strings.TrimSuffix(strings.ToLower(binanceSymbol), "usdt")
	c.symbolMap[base] = coinGeckoID
}

// executeWithRetry executes HTTP request with exponential backoff for 429 errors
func (c *CoinGeckoClient) executeWithRetry(req *http.Request) (*http.Response, error) {
	maxRetries := 3
	baseDelay := 5 * time.Second
	
	for attempt := 0; attempt <= maxRetries; attempt++ {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %w", err)
		}
		
		// Success case
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		
		// Handle rate limiting
		if resp.StatusCode == 429 {
			resp.Body.Close()
			
			if attempt == maxRetries {
				c.logger.Error("Max retries exceeded for CoinGecko API rate limit")
				return nil, fmt.Errorf("rate limit exceeded, max retries reached")
			}
			
			// Exponential backoff: 5s, 10s, 20s
			delay := baseDelay * time.Duration(1<<attempt)
			c.logger.WithFields(logrus.Fields{
				"attempt": attempt + 1,
				"delay":   delay,
			}).Warn("CoinGecko rate limit hit, retrying after delay")
			
			time.Sleep(delay)
			continue
		}
		
		// Other error status codes
		resp.Body.Close()
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}
	
	return nil, fmt.Errorf("unexpected error in retry logic")
}